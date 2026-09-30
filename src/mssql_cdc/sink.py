"""Idempotent Delta sink for the CDC stream, recording per-batch facts.

* Writes are appends with ``txnAppId``/``txnVersion`` (Delta's idempotent-write
  options), keyed by the micro-batch id, so a replayed batch is skipped.
* Facts about each batch go into the Delta commit (``userMetadata``) and,
  optionally, into a facts table. The table matters: ``commitInfo`` is not kept in
  checkpoints and disappears with log cleanup (``delta.logRetentionDuration``).
* The facts table also times each batch: ``started_at`` and ``duration_ms`` cover the
  read from SQL Server, the facts aggregation and the target write. Offset planning and
  the checkpoint commit run outside ``foreachBatch`` and are not included.
* A micro-batch that read no change rows (the stream moved past idle entries or other
  tables' commits) writes nothing to the target, but still writes its facts row (rows = 0),
  so a current stream on a quiet table keeps writing facts.
* Optional network metrics: with ``metrics_path`` (the directory of the source option
  ``metricsPath``) the sink folds each partition's round trip, read time, MB and
  ``ASYNC_NETWORK_IO`` into the batch facts, with the batch's end offset, the retention
  watermark and headroom (ADR 0017) and the capture and ingestion lag (ADR 0020), both
  measured from that end offset. The sink never connects to SQL Server itself.
  The directory belongs to one stream. The sink empties it before the batch is read (what is
  left is a dead attempt's), then folds every file in it and removes them: micro-batches run
  one at a time and the partitions are read inside ``foreachBatch``, so every file there is
  the current batch's, and a retried task rewrites its file under the same name
  (``<from>-<to>.json``). Selecting files by the rows' LSNs would miss the partitions that
  read none, such as the trailing one that ends at the end offset. Metrics never fail a
  batch. ``mssql_cdc.stream()`` wires both ends from one set of options.
* Snapshots are facts too: ``write_event()`` records the bootstrap and every re-snapshot
  after data loss as one row with ``event`` set and no ``batch_id`` (ADR 0018), idempotent
  the same way.
* Both tables are created on the first batch with the ``DeltaTable`` builder, with a
  comment on every metadata/facts column; existing ones get pending schema migrations
  (``mssql_cdc.migrations``).
"""

from __future__ import annotations

import glob
import json
import os
import statistics
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import migrations
from .migrations.facts import (
    END_COLUMNS,
    EVENT_COLUMNS,
    LAG_COLUMNS,
    NETWORK_COLUMNS,
    RETENTION_COLUMNS,
)
from .tables import is_path

BRONZE_COMMENT = (
    "Append-only change rows from SQL Server CDC, written by mssql-cdc-pyspark's delta_sink. "
    "One row per change: an update is two rows (operation 3, the row before; 4, the row after). "
    "Order changes by (_start_lsn, _command_id, _seqval, _operation). Rows with operation 0 "
    "are a snapshot of the source table, all at one _start_lsn that precedes the changes read "
    "after it."
)
BRONZE_COLUMN_COMMENTS = {
    "_capture_instance": "CDC capture instance the change came from, e.g. dbo_orders.",
    "_start_lsn": (
        "Commit LSN of the source transaction (__$start_lsn) as 0x + 20 uppercase hex. "
        "All changes of one transaction share it; string order is commit order. On snapshot "
        "rows, the LSN recorded before the table was read: the row is at least that recent."
    ),
    "_seqval": (
        "Position of the change in the transaction log (__$seqval), 0x + 20 hex. "
        "Tie-breaker only: order by _command_id first. NULL on snapshot rows."
    ),
    "_operation": (
        "What happened to the row: 1 = delete, 2 = insert, 3 = update (row before), "
        "4 = update (row after), 0 = snapshot (the row as read from the source table)."
    ),
    "_command_id": (
        "Order of the statement within its transaction (__$command_id). NULL on snapshot rows."
    ),
    "_commit_ts": (
        "Commit time of the source transaction, UTC (from cdc.lsn_time_mapping); on snapshot "
        "rows, the commit time of their _start_lsn."
    ),
    "_batch_id": (
        "Micro-batch that wrote the row; with the sink's app_id, the key of its row in the "
        "ingestion facts table. NULL on snapshot rows."
    ),
}


def bronze_columns(df: DataFrame) -> list[tuple]:
    """The bronze table's creation columns: ``df``'s fields with their comments."""
    return [(f.name, f.dataType, BRONZE_COLUMN_COMMENTS.get(f.name)) for f in df.schema]


FACTS_COMMENT = (
    "One row per micro-batch written by mssql-cdc-pyspark's delta_sink, including batches that "
    "read no change rows (rows = 0), so a current stream on a quiet table keeps writing rows: "
    "what was written (counts, LSN and commit-time ranges), how far the stream had read "
    "(end_lsn, end_commit_ts) and how long it took. The same facts are in each target commit's "
    "userMetadata (batches with rows only), which Delta log cleanup eventually drops. Each "
    "snapshot stream().to_delta takes (bootstrap or re-snapshot) adds one row, with event set "
    "(see its comment)."
)
FACTS_COLUMNS = [
    (
        "app_id",
        "STRING",
        (
            "Identity of the sink that wrote the batch (Delta txnAppId of the target write). "
            "Stable for the life of one streaming checkpoint; a new checkpoint needs a new app_id."
        ),
    ),
    (
        "batch_id",
        "BIGINT",
        (
            "Structured Streaming micro-batch id. With app_id, the idempotency key: a replayed "
            "batch is skipped, so it never appears twice."
        ),
    ),
    (
        "rows",
        "BIGINT",
        (
            "Change rows written to the target in this batch, all operations. 0 when the batch "
            "read none: its end offset moved only past idle entries or other tables' commits (or, "
            "on a new checkpoint's first batch, not at all), so it wrote nothing to the target, "
            "just this row (LSN and commit-time ranges NULL, counts 0). On event rows, the rows "
            "of the snapshot."
        ),
    ),
    ("min_lsn", "STRING", "Smallest source commit LSN (__$start_lsn, 0x + 20 hex) in the batch."),
    ("max_lsn", "STRING", "Largest source commit LSN in the batch; hex strings sort in LSN order."),
    ("min_commit_ts", "TIMESTAMP_NTZ", "Earliest source commit time in the batch, UTC."),
    (
        "max_commit_ts",
        "TIMESTAMP_NTZ",
        (
            "Latest source commit time in the batch, UTC. written_at minus this is the batch's "
            "ingestion latency."
        ),
    ),
    ("deletes", "BIGINT", "Rows with operation 1 (delete)."),
    ("inserts", "BIGINT", "Rows with operation 2 (insert)."),
    (
        "updates",
        "BIGINT",
        (
            "Updated rows, counted once: operation 4 (the row after). Each has an operation 3 "
            "row (the row before) that is not counted here."
        ),
    ),
    ("started_at", "TIMESTAMP_NTZ", "When the sink started processing the batch, UTC."),
    (
        "duration_ms",
        "BIGINT",
        (
            "Milliseconds from started_at to the end of the target write: the read from SQL "
            "Server, these facts and the append. Offset planning and the checkpoint commit are "
            "not included."
        ),
    ),
    *NETWORK_COLUMNS,
    *RETENTION_COLUMNS,
    *EVENT_COLUMNS,
    *LAG_COLUMNS,
    *END_COLUMNS,
    ("target", "STRING", "Table name or path the batch was written to."),
    (
        "written_at",
        "TIMESTAMP_NTZ",
        "When this facts row was written, after the target commit, UTC.",
    ),
]
_FACT_FIELDS = [name for name, _, _ in FACTS_COLUMNS]
FACTS_SCHEMA = ", ".join(f"{name} {data_type}" for name, data_type, _ in FACTS_COLUMNS)


def _utc_now() -> datetime:
    """Every time in these tables is TIMESTAMP_NTZ in UTC: comparing them (written_at minus
    max_commit_ts) never depends on the Spark session's time zone."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def batch_facts(df: DataFrame) -> dict:
    # counts, not sums: an empty batch has 0 of each, and NULL ranges
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("_start_lsn").alias("min_lsn"),
        F.max("_start_lsn").alias("max_lsn"),
        F.min("_commit_ts").alias("min_commit_ts"),
        F.max("_commit_ts").alias("max_commit_ts"),
        F.count(F.when(F.col("_operation") == 1, 1)).alias("deletes"),
        F.count(F.when(F.col("_operation") == 2, 1)).alias("inserts"),
        # updates count after-images (operation 4); before-images (3) pair with them
        F.count(F.when(F.col("_operation") == 4, 1)).alias("updates"),
    ).first()
    assert row is not None  # a global aggregate always returns one row
    return row.asDict()


def _json(facts: dict) -> str:
    """Facts as the target commit's userMetadata; commit times as ISO-8601 with milliseconds."""
    return json.dumps(
        facts, separators=(",", ":"), default=lambda v: v.isoformat(timespec="milliseconds")
    )


def _clear_metrics(path: str) -> None:
    """Remove the metrics files in ``path``, the unreadable ones too."""
    for name in glob.glob(os.path.join(path, "*.json")):
        try:
            os.remove(name)
        except OSError:
            pass


def _fold_metrics(path: str) -> dict:
    """Fold every metrics file in ``path``: all are the current batch's (see the module doc)."""
    picked = []
    for name in glob.glob(os.path.join(path, "*.json")):
        try:
            with open(name, encoding="utf-8") as fh:
                picked.append(json.load(fh))
        except (OSError, ValueError):
            continue
    if not picked:
        return {}
    end = max(picked, key=lambda m: m["to_lsn"])  # the partition that ends at the end offset
    waits = [m.get("network_wait_ms") for m in picked]
    rtts = [m["rtt_ms"] for m in picked if m.get("rtt_ms") is not None]
    marks = [m["retention_watermark_ts"] for m in picked if m.get("retention_watermark_ts")]
    tops = [m["source_max_commit_ts"] for m in picked if m.get("source_max_commit_ts")]
    lags = [m["capture_lag_seconds"] for m in picked if m.get("capture_lag_seconds") is not None]
    return {
        "end_lsn": end["to_lsn"],
        "end_commit_ts": (
            datetime.fromisoformat(end["to_commit_ts"]) if end.get("to_commit_ts") else None
        ),
        "retention_watermark_ts": datetime.fromisoformat(max(marks)) if marks else None,
        "source_max_commit_ts": datetime.fromisoformat(max(tops)) if tops else None,
        "capture_lag_seconds": round(max(lags), 3) if lags else None,
        "source_rtt_ms": round(statistics.median(rtts), 1) if rtts else None,
        "read_seconds": round(sum(m["seconds"] for m in picked), 3),
        "read_mb": round(sum(m["bytes"] for m in picked) / 1e6, 6),
        "network_wait_ms": None if None in waits else sum(waits),
    }


def _headroom(watermark: datetime | None, position: datetime | None) -> dict:
    """``position``: the commit time the stream has read up to (the batch's end offset)."""
    hours = (
        None
        if watermark is None or position is None
        else round((position - watermark).total_seconds() / 3600, 2)
    )
    return {"retention_watermark_ts": watermark, "retention_headroom_hours": hours}


def _lag(source_max: datetime | None, position: datetime | None) -> float | None:
    if source_max is None or position is None:
        return None
    return round((source_max - position).total_seconds(), 3)


def _write(df: DataFrame, target: str, app_id: str, version: int, metadata: str | None = None):
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", version)
    )
    if metadata is not None:
        writer = writer.option("userMetadata", metadata)
    if is_path(target):
        writer.save(target)
    else:
        writer.saveAsTable(target)


def delta_sink(
    target: str, app_id: str, facts_table: str | None = None, metrics_path: str | None = None
):
    """Return a ``foreachBatch`` function.

    ``app_id`` must be stable for the lifetime of a checkpoint. If the checkpoint is
    deleted, use a new ``app_id``; batch ids restart at 0 and would otherwise be
    ignored as duplicates.

    ``metrics_path`` feeds the facts table (see the module doc): the directory of the source
    option ``metricsPath``, used by no other stream. Its files are removed after each batch,
    with or without a facts table; without ``metrics_path`` nothing removes them.
    """
    created: set[str] = set()  # once per query run, not once per batch

    def ensure(spark, table: str, kind: str, columns, comment: str) -> None:
        if table not in created:
            migrations.ensure(spark, table, kind, columns, comment)
            created.add(table)

    def write_batch(df: DataFrame, batch_id: int) -> None:
        started_at, t0 = _utc_now(), time.monotonic()
        df = df.persist()
        try:
            if metrics_path:  # the batch is not read yet: any file here is a dead attempt's
                _clear_metrics(metrics_path)
            facts = batch_facts(df)  # reads the batch: its partitions write their metrics files
            facts.update({"batch_id": batch_id, "app_id": app_id})
            spark = df.sparkSession
            if facts["rows"]:  # a batch that read none writes no target commit, only its facts
                out = df.withColumn("_batch_id", F.lit(batch_id).cast("int"))
                ensure(spark, target, "bronze", bronze_columns(out), BRONZE_COMMENT)
                _write(out, target, app_id, batch_id, _json(facts))
            duration_ms = round((time.monotonic() - t0) * 1000)
            folded = _fold_metrics(metrics_path) if metrics_path else {}
            if facts_table:
                # where the stream is: the end offset, not the batch's last change, which on a
                # quiet table lags it (the batch's last change as a fallback, without metrics)
                position = folded.get("end_commit_ts") or facts["max_commit_ts"]
                facts.update(
                    folded,
                    started_at=started_at,
                    duration_ms=duration_ms,
                    **_headroom(folded.get("retention_watermark_ts"), position),
                    ingestion_lag_seconds=_lag(folded.get("source_max_commit_ts"), position),
                    target=target,
                    written_at=_utc_now(),
                )
                ensure(spark, facts_table, "facts", FACTS_COLUMNS, FACTS_COMMENT)
                facts_df = spark.createDataFrame(
                    [tuple(facts.get(k) for k in _FACT_FIELDS)], FACTS_SCHEMA
                )  # event columns stay NULL
                _write(facts_df, facts_table, f"{app_id}#facts", batch_id)
            if metrics_path:  # folded into this batch's facts; a replay rewrites them
                _clear_metrics(metrics_path)
        finally:
            df.unpersist()

    return write_batch


def write_event(
    spark,
    facts_table: str,
    event: str,
    *,
    app_id: str,
    txn_app_id: str,
    version: int,
    target: str,
    lsn: str,
    commit_ts: str,
    rows: int | None = None,
    started_at: datetime | None = None,
    duration_ms: int | None = None,
    lost_from_ts: datetime | None = None,
    lost_to_ts: datetime | None = None,
) -> None:
    """Record a snapshot (``event`` 'bootstrap' or 'resnapshot') as one facts row.

    The row has no ``batch_id``; ``lsn`` and ``commit_ts`` are the snapshot's offset.
    Idempotent like the batch rows: a rerun with the same ``txn_app_id`` and ``version``
    is skipped by Delta.
    """
    ts = datetime.fromisoformat(commit_ts) if commit_ts else None
    facts = {
        "app_id": app_id,
        "rows": rows,
        "min_lsn": lsn,
        "max_lsn": lsn,
        "min_commit_ts": ts,
        "max_commit_ts": ts,
        "deletes": 0,
        "inserts": 0,
        "updates": 0,
        "started_at": started_at,
        "duration_ms": duration_ms,
        **_headroom(lost_to_ts, ts),
        "end_lsn": lsn,  # the offset the stream starts from
        "end_commit_ts": ts,
        "event": event,
        "lost_from_ts": lost_from_ts,
        "lost_to_ts": lost_to_ts,
        "target": target,
        "written_at": _utc_now(),
    }
    migrations.ensure(spark, facts_table, "facts", FACTS_COLUMNS, FACTS_COMMENT)
    df = spark.createDataFrame([tuple(facts.get(k) for k in _FACT_FIELDS)], FACTS_SCHEMA)
    _write(df, facts_table, txn_app_id, version)
