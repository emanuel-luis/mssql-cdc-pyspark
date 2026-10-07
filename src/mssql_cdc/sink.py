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
  The directory belongs to one stream. The sink removes the partitions' files before the
  batch is read (what is left is a dead attempt's), then folds every one in it and removes
  them: micro-batches run one at a time and the partitions are read inside ``foreachBatch``,
  so every file there is the current batch's, and a retried task rewrites its file under the
  same name
  (``<from>-<to>.json``). Selecting files by the rows' LSNs would miss the partitions that
  read none, such as an idle batch's, which ends at the end offset. Only the batch's last
  partition measures the position (watermark, capture's progress, end commit time). Metrics
  never fail a batch. ``mssql_cdc.stream()`` wires both ends from one set of options.
* Changes to the source are facts too (ADR 0023): while planning a batch the reader leaves an
  ``event-<kind>-<lsn>.json`` file in the same directory for a schema change, a switch to a
  newer capture instance or a skip past purged changes (``failOnDataLoss=false``, with the
  gap, ADR 0018). The sink writes each as an event row of the batch, in the same
  commit as the batch's own row, so a replay writes both or neither, and removes the files
  only after that (the partitions' files are removed before the read; these stay). Without
  a facts table they are removed unwritten: the reader has logged them. A partition that
  finds CDC cleanup ran while it read its range (``failOnDataLoss=false``) puts its
  'data_skipped' event, the loss possible rather than certain, in its own metrics file
  instead: a retried task rewrites it, and a dead attempt's goes with the other partitions'.
* Bronze appends use ``mergeSchema``: a column that a newer capture instance captures joins
  the table (older rows read NULL). A changed type fails the append unless the table has
  ``delta.enableTypeWidening`` and the change widens.
* Snapshots are facts too: ``write_event()`` records the bootstrap and every re-snapshot
  after data loss as one row with ``event`` set and no ``batch_id`` (ADR 0018), idempotent
  the same way; a chunked snapshot also its open and, through ``write_facts()``, its chunks
  (ADR 0028). Every bronze writer adds ``_batch_id``, ``_snapshot`` and ``_chunk``
  (``bronze_rows``); change rows leave the last two NULL.
* Both tables are created on the first batch with the ``DeltaTable`` builder, with a
  comment on every metadata/facts column; existing ones get pending schema migrations
  (``mssql_cdc.migrations``).
"""

from __future__ import annotations

import glob
import json
import logging
import os
import statistics
import time
from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import migrations
from .migrations.facts import (
    DETAIL_COLUMNS,
    END_COLUMNS,
    EVENT_COLUMNS,
    LAG_COLUMNS,
    NETWORK_COLUMNS,
    POSSIBLE_COMMENTS,
    RETENTION_COLUMNS,
    SKIP_COMMENTS,
)
from .tables import is_path

_log = logging.getLogger(__name__)

BRONZE_COMMENT = (
    "Append-only change rows from SQL Server CDC, written by mssql-cdc-pyspark's delta_sink. "
    "One row per change: an update is two rows (operation 3, the row before; 4, the row after). "
    "Order changes by (_start_lsn, _command_id, _seqval, _operation). Rows with operation 0 "
    "are a snapshot of the source table, the one _snapshot names: rebuild from it with its rows "
    "and the changes with a larger _start_lsn. A whole snapshot's rows share its _start_lsn; a "
    "chunked snapshot's (_chunk set) are stamped per chunk, at or after it. A column the source "
    "table gained through a newer capture instance is added when the stream first reads it "
    "(older rows read NULL); a column it lost stays, NULL from then on."
)
BRONZE_COLUMN_COMMENTS = {
    "_capture_instance": (
        "CDC capture instance the change came from, e.g. dbo_orders: after the stream switched to "
        "a newer capture instance of the table (from that instance's start LSN on), the newer "
        "one. On snapshot rows, the instance the snapshot was taken for."
    ),
    "_start_lsn": (
        "Commit LSN of the source transaction (__$start_lsn) as 0x + 20 uppercase hex. "
        "All changes of one transaction share it; string order is commit order. On snapshot "
        "rows, the LSN recorded before the table (or the row's chunk) was read: the row is at "
        "least that recent."
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
        "Order of the statement within its transaction (__$command_id). Numbered per capture "
        "instance: another instance of the table numbers the same change differently, so it "
        "orders rows only within one _start_lsn (all rows of a commit come from one instance); "
        "(_start_lsn, _seqval, _operation) identifies a change across instances. NULL on "
        "snapshot rows."
    ),
    "_commit_ts": (
        "Commit time of the source transaction, UTC (from cdc.lsn_time_mapping); on snapshot "
        "rows, the commit time of their _start_lsn."
    ),
    "_batch_id": (
        "Micro-batch that wrote the row; with the sink's app_id, the key of its row in the "
        "ingestion facts table. NULL on snapshot rows."
    ),
    "_snapshot": (
        "On snapshot rows, the snapshot they belong to: the LSN (0x + 20 hex) recorded before "
        "any of its rows was read, where its stream generation starts; on a whole snapshot, its "
        "_start_lsn. NULL on change rows, and on snapshot rows written before this column "
        "existed, whose _start_lsn is their snapshot's."
    ),
    "_chunk": (
        "On rows of a chunked snapshot (stream().backfill()), the chunk of the key space they "
        "were read in; with _snapshot, the key of its 'snapshot_chunk' row in the ingestion "
        "facts table. NULL on change rows and whole snapshots."
    ),
}


def bronze_columns(df: DataFrame) -> list[tuple]:
    """The bronze table's creation columns: ``df``'s fields with their comments."""
    return [(f.name, f.dataType, BRONZE_COLUMN_COMMENTS.get(f.name)) for f in df.schema]


def bronze_rows(df: DataFrame, batch_id: int | None = None, snapshot=None) -> DataFrame:
    """``df`` with the columns every bronze writer adds, last and in this order: ``_batch_id``,
    ``_snapshot`` (a Column, or NULL) and ``_chunk`` (``df``'s own, else NULL)."""
    chunk = F.col("_chunk") if "_chunk" in df.columns else F.lit(None)
    return df.select(
        *(F.col("`" + c.replace("`", "``") + "`") for c in df.columns if c != "_chunk"),
        F.lit(batch_id).cast("int").alias("_batch_id"),
        (F.lit(None) if snapshot is None else snapshot).cast("string").alias("_snapshot"),
        chunk.cast("int").alias("_chunk"),
    )


FACTS_COMMENT = (
    "One row per micro-batch written by mssql-cdc-pyspark's delta_sink, including batches that "
    "read no change rows (rows = 0), so a current stream on a quiet table keeps writing rows: "
    "what was written (counts, LSN and commit-time ranges), how far the stream had read "
    "(end_lsn, end_commit_ts) and how long it took. The same facts are in each target commit's "
    "userMetadata (batches with rows only), which Delta log cleanup eventually drops. Each "
    "snapshot stream().to_delta takes (bootstrap or re-snapshot), each schema change on the "
    "source, each switch to a newer capture instance and each skip past purged changes "
    "(failOnDataLoss=false) adds one row, with event set (see its comment); a snapshot adds one "
    "more when it opens; a chunked one also adds one with the chunks its first "
    "stream().backfill() call plans and one per chunk it reads."
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
            "batch is skipped, so its row (event NULL) never appears twice. Its 'schema_change', "
            "'capture_instance_switched' and 'data_skipped' rows carry it too; snapshot event "
            "rows have none."
        ),
    ),
    (
        "rows",
        "BIGINT",
        (
            "Change rows written to the target in this batch, all operations. 0 when the batch "
            "read none: its end offset moved only past idle entries or other tables' commits (or, "
            "on a new checkpoint's first batch, not at all), so it wrote nothing to the target, "
            "just this row (LSN and commit-time ranges NULL, counts 0). On 'bootstrap' and "
            "'resnapshot' rows, the rows of the snapshot (of all its chunks); on "
            "'snapshot_chunk' rows, the chunk's; 0 on other event rows."
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
    *((n, t, POSSIBLE_COMMENTS.get(n, SKIP_COMMENTS.get(n, c))) for n, t, c in EVENT_COLUMNS),
    *LAG_COLUMNS,
    *END_COLUMNS,
    *((n, t, POSSIBLE_COMMENTS.get(n, SKIP_COMMENTS.get(n, c))) for n, t, c in DETAIL_COLUMNS),
    ("target", "STRING", "Table name or path the batch was written to."),
    (
        "written_at",
        "TIMESTAMP_NTZ",
        "When this facts row was written, after the target commit, UTC.",
    ),
]
_FACT_FIELDS = [name for name, _, _ in FACTS_COLUMNS]
FACTS_SCHEMA = ", ".join(f"{name} {data_type}" for name, data_type, _ in FACTS_COLUMNS)


def _fact_tuples(rows: list[dict]) -> list[tuple]:
    """Facts ``rows`` (column -> value, the rest NULL) in ``FACTS_SCHEMA``'s order. A key that
    is no column raises: projecting would drop it and write NULL."""
    unknown = sorted({k for r in rows for k in r} - set(_FACT_FIELDS))
    if unknown:
        raise ValueError(f"unknown facts columns: {unknown}")
    return [tuple(r.get(k) for k in _FACT_FIELDS) for r in rows]


def _last_batch(spark, facts_table: str, app_id: str) -> int | None:
    """The largest batch id ``app_id`` wrote a batch row (event NULL) for in ``facts_table``,
    an existing table; None when it wrote none."""
    from .tables import delta_table

    row = (
        delta_table(spark, facts_table)
        .toDF()
        .where((F.col("app_id") == app_id) & F.col("event").isNull())
        .agg(F.max("batch_id"))
        .first()
    )
    return None if row is None else row[0]


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


def _files(path: str, events: bool = False) -> list[str]:
    """The partitions' metrics files in ``path`` or, with ``events``, the reader's event files
    (``event-<kind>-<lsn>.json``, a data skip's ``event-data_skipped-<ci>-<from>.json``, ADR
    0023)."""
    return [
        name
        for name in glob.glob(os.path.join(path, "*.json"))
        if os.path.basename(name).startswith("event-") == events
    ]


def _remove(names: list[str]) -> None:
    for name in names:
        try:
            os.remove(name)
        except OSError:
            pass


def _read_events(path: str) -> tuple[list[str], list[dict]]:
    """The reader's events waiting in ``path``: the files read and their contents. An
    unreadable file is not returned, so it is never removed unfolded."""
    names, events = [], []
    for name in _files(path, events=True):
        try:
            with open(name, encoding="utf-8") as fh:
                events.append(json.load(fh))
        except (OSError, ValueError):
            continue
        names.append(name)
    return names, events


def _event_row(event: dict, **batch) -> dict:
    """A facts row for one of the reader's events: in the batch that read past it, 0 rows;
    a 'data_skipped' one also has the gap (ADR 0018)."""
    ts, lost_from, lost_to = (
        datetime.fromisoformat(event[k]) if event.get(k) else None
        for k in ("commit_ts", "lost_from_ts", "lost_to_ts")
    )
    lsn = event["lsn"]
    return {
        **batch,
        "event": event["event"],
        "detail": event.get("detail"),
        "lost_from_ts": lost_from,
        "lost_to_ts": lost_to,
        "rows": 0,
        "deletes": 0,
        "inserts": 0,
        "updates": 0,
        "min_lsn": lsn,
        "max_lsn": lsn,
        "end_lsn": lsn,
        "min_commit_ts": ts,
        "max_commit_ts": ts,
        "end_commit_ts": ts,
    }


def _fold_metrics(path: str) -> dict:
    """Fold every metrics file in ``path``: all are the current batch's (see the module doc).
    ``data_skipped``: the events of the partitions that found CDC cleanup had run while they
    read (ADR 0018), for the caller to take out as event rows."""
    picked = []
    for name in _files(path):
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
        "data_skipped": [m["data_skipped"] for m in picked if m.get("data_skipped")],
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


def _write(
    df: DataFrame,
    target: str,
    app_id: str | None,
    version: int | None,
    metadata: str | None = None,
    merge_schema: bool = False,
):
    """Append ``df`` to ``target``; idempotent with ``app_id`` and ``version``. With
    ``merge_schema`` (bronze), a column type the table cannot take raises
    ``SchemaChangedError`` saying what to do, instead of Delta's bare "Failed to merge
    fields" (ADR 0023)."""
    writer = df.write.format("delta").mode("append")
    if app_id is not None:
        writer = writer.option("txnAppId", app_id).option("txnVersion", version)
    if metadata is not None:
        writer = writer.option("userMetadata", metadata)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    try:
        writer.save(target) if is_path(target) else writer.saveAsTable(target)
    except Exception as exc:
        if not merge_schema or "DELTA_FAILED_TO_MERGE_FIELDS" not in str(exc):
            raise
        from .client import SchemaChangedError

        name = f"delta.`{target}`" if is_path(target) else target
        raise SchemaChangedError(
            f"{target}: the type of a column changed on the source and the table cannot take "
            f"the new one ({str(exc).splitlines()[0]}). For a widening, run ALTER TABLE {name} "
            "SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true') and restart; otherwise "
            "write to a new table or rewrite this one."
        ) from exc


def delta_sink(
    target: str, app_id: str, facts_table: str | None = None, *, metrics_path: str | None = None
) -> Callable[[DataFrame, int], None]:
    """Return a ``foreachBatch`` function; ``metrics_path`` is keyword-only.

    ``app_id`` must be stable for the lifetime of a checkpoint. If the checkpoint is
    deleted, use a new ``app_id``; batch ids restart at 0 and would otherwise be
    ignored as duplicates. With a facts table the run's first batch is checked against it:
    a batch id below the largest one ``app_id`` already wrote there (the checkpoint was
    deleted or rewound) raises ``ValueError`` instead. Without one nothing checks it.

    ``metrics_path`` feeds the facts table with each partition's read and network metrics:
    the directory of the source option ``metricsPath``, used by no other stream. Its files are removed after each batch,
    with or without a facts table; without ``metrics_path`` nothing removes them. It also
    carries the reader's schema change, capture instance switch and data skipped events to
    the facts.
    A batch that read rows but found no metrics file there logs a warning, once per run.
    """
    created: set[str] = set()  # once per query run, not once per batch
    resumed = warned = False

    def ensure(spark, table: str, kind: str, columns, comment: str) -> None:
        if table not in created:
            migrations.ensure(spark, table, kind, columns, comment)
            created.add(table)

    def write_batch(df: DataFrame, batch_id: int) -> None:
        nonlocal resumed, warned
        started_at, t0 = _utc_now(), time.monotonic()
        spark = df.sparkSession
        # ponytail: without a facts table nothing is checked; the newest bronze commit's
        # userMetadata could stand in if that case needs it
        if facts_table and not resumed:
            # a restart replays at most the last batch, and a generation has its own app_id
            ensure(spark, facts_table, "facts", FACTS_COLUMNS, FACTS_COMMENT)
            last = _last_batch(spark, facts_table, app_id)
            if last is not None and batch_id < last:
                raise ValueError(
                    f"{facts_table} holds batch {last} of app_id {app_id!r}, but this run's "
                    f"checkpoint starts at batch {batch_id}: the checkpoint was deleted or "
                    "rewound while app_id stayed the same, so Delta would skip every write up to "
                    f"batch {last} as done already. Use a new app_id, or restore the checkpoint."
                )
            resumed = True
        df = df.persist()
        try:
            if metrics_path:  # the batch is not read yet: a partition's file is a dead attempt's
                _remove(_files(metrics_path))
            facts = batch_facts(df)  # reads the batch: its partitions write their metrics files
            facts.update({"batch_id": batch_id, "app_id": app_id})
            if facts["rows"]:  # a batch that read none writes no target commit, only its facts
                out = bronze_rows(df, batch_id)
                ensure(spark, target, "bronze", bronze_columns(out), BRONZE_COMMENT)
                # one file per range read, written in parallel; compaction merges small ones
                # mergeSchema: a column a newer capture instance captures joins bronze (ADR 0023)
                _write(out, target, app_id, batch_id, _json(facts), merge_schema=True)
            duration_ms = round((time.monotonic() - t0) * 1000)
            folded = _fold_metrics(metrics_path) if metrics_path else {}
            # the reader's events, written while it planned this batch (or a dead attempt),
            # and the partitions' (a range cleanup reached while it was read)
            names, events = _read_events(metrics_path) if metrics_path else ([], [])
            events += folded.pop("data_skipped", [])
            if facts_table:
                # a batch with no range writes no file; one that read rows always does
                if metrics_path and not folded and facts["rows"] and not warned:
                    warned = True
                    _log.warning(
                        "mssql_cdc: batch %s of %s read %s rows but found no metrics file in %s: "
                        "the executors cannot write that directory, or the driver cannot see it "
                        "(a driver-local path on a multi-node cluster). Its retention and lag "
                        "facts are NULL: use a path every node sees (local, or FUSE such as a "
                        "Volume).",
                        batch_id,
                        app_id,
                        facts["rows"],
                        metrics_path,
                    )
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
                keys = {k: facts[k] for k in ("app_id", "batch_id", "target", "written_at")}
                rows = [facts, *(_event_row(e, **keys) for e in events)]
                facts_df = spark.createDataFrame(
                    _fact_tuples(rows), FACTS_SCHEMA
                )  # the batch's row has event NULL
                # one commit: the batch's row and its events are written, or skipped, together
                _write(facts_df, facts_table, f"{app_id}#facts", batch_id)
            if metrics_path:  # folded into this batch's facts; a replay rewrites them
                _remove(_files(metrics_path) + names)
        finally:
            df.unpersist()

    return write_batch


def write_event(
    spark,
    facts_table: str,
    event: str,
    *,
    app_id: str,
    txn_app_id: str | None,
    version: int,
    target: str,
    lsn: str,
    commit_ts: str,
    rows: int | None = None,
    started_at: datetime | None = None,
    duration_ms: int | None = None,
    lost_from_ts: datetime | None = None,
    lost_to_ts: datetime | None = None,
    detail: str | None = None,
) -> None:
    """Record a snapshot (``event`` 'bootstrap', 'resnapshot', the 'snapshot_open' written
    before either is read or a chunked snapshot's 'snapshot_plan') as one facts row.

    The row has no ``batch_id``; ``lsn`` and ``commit_ts`` are the snapshot's offset.
    Idempotent like the batch rows: a rerun with the same ``txn_app_id`` and ``version``
    is skipped by Delta. ``txn_app_id`` None: appended every time (a full snapshot's open).
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
        "detail": detail,
        "target": target,
    }
    write_facts(spark, facts_table, [facts], txn_app_id, version)


def write_facts(
    spark, facts_table: str, rows: list[dict], txn_app_id: str | None, version: int
) -> None:
    """Append facts ``rows`` (column -> value; the rest NULL, ``written_at`` now) in one
    commit, skipped by Delta when ``txn_app_id`` already wrote ``version`` (never when None)."""
    now = _utc_now()
    data = _fact_tuples([{"written_at": now, **r} for r in rows])  # a wrong key raises first
    migrations.ensure(spark, facts_table, "facts", FACTS_COLUMNS, FACTS_COMMENT)
    _write(spark.createDataFrame(data, FACTS_SCHEMA), facts_table, txn_app_id, version)
