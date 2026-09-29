"""Idempotent Delta sink for the CDC stream, recording per-batch facts.

* Writes are appends with ``txnAppId``/``txnVersion`` (Delta's idempotent-write
  options), keyed by the micro-batch id, so a replayed batch is skipped.
* Facts about each batch go into the Delta commit (``userMetadata``) and,
  optionally, into a facts table. The table matters: ``commitInfo`` is not kept in
  checkpoints and disappears with log cleanup (``delta.logRetentionDuration``).
* The facts table also times each batch: ``started_at`` and ``duration_ms`` cover the
  read from SQL Server, the facts aggregation and the target write. Offset planning and
  the checkpoint commit run outside ``foreachBatch`` and are not included.
* Both tables are created on the first batch with the ``DeltaTable`` builder, with a
  comment on every metadata/facts column. Existing tables are left as they are.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F

from .tables import create_if_not_exists, is_path

BRONZE_COMMENT = (
    "Append-only change rows from SQL Server CDC, written by mssql-cdc-pyspark's delta_sink. "
    "One row per change: an update is two rows (operation 3, the row before; 4, the row after). "
    "Order changes by (_start_lsn, _command_id, _seqval, _operation)."
)
BRONZE_COLUMN_COMMENTS = {
    "_capture_instance": "CDC capture instance the change came from, e.g. dbo_orders.",
    "_start_lsn": (
        "Commit LSN of the source transaction (__$start_lsn) as 0x + 20 uppercase hex. "
        "All changes of one transaction share it; string order is commit order."),
    "_seqval": (
        "Position of the change in the transaction log (__$seqval), 0x + 20 hex. "
        "Tie-breaker only: order by _command_id first."),
    "_operation": (
        "What happened to the row: 1 = delete, 2 = insert, 3 = update (row before), "
        "4 = update (row after)."),
    "_command_id": "Order of the statement within its transaction (__$command_id).",
    "_commit_ts": "Commit time of the source transaction, UTC (from cdc.lsn_time_mapping).",
    "_batch_id": (
        "Micro-batch that wrote the row; with the sink's app_id, the key of its row in the "
        "ingestion facts table."),
}

FACTS_COMMENT = (
    "One row per non-empty micro-batch written by mssql-cdc-pyspark's delta_sink: what was "
    "written (counts, LSN and commit-time ranges) and how long it took. The same facts are in "
    "each target commit's userMetadata, which Delta log cleanup eventually drops."
)
FACTS_COLUMNS = [
    ("app_id", "STRING", (
        "Identity of the sink that wrote the batch (Delta txnAppId of the target write). "
        "Stable for the life of one streaming checkpoint; a new checkpoint needs a new app_id.")),
    ("batch_id", "BIGINT", (
        "Structured Streaming micro-batch id. With app_id, the idempotency key: a replayed "
        "batch is skipped, so it never appears twice.")),
    ("rows", "BIGINT", "Change rows written to the target in this batch, all operations."),
    ("min_lsn", "STRING", "Smallest source commit LSN (__$start_lsn, 0x + 20 hex) in the batch."),
    ("max_lsn", "STRING", "Largest source commit LSN in the batch; hex strings sort in LSN order."),
    ("min_commit_ts", "TIMESTAMP_NTZ", "Earliest source commit time in the batch, UTC."),
    ("max_commit_ts", "TIMESTAMP_NTZ", (
        "Latest source commit time in the batch, UTC. written_at minus this is the batch's "
        "ingestion latency.")),
    ("deletes", "BIGINT", "Rows with operation 1 (delete)."),
    ("inserts", "BIGINT", "Rows with operation 2 (insert)."),
    ("updates", "BIGINT", (
        "Updated rows, counted once: operation 4 (the row after). Each has an operation 3 "
        "row (the row before) that is not counted here.")),
    ("started_at", "TIMESTAMP", "When the sink started processing the batch."),
    ("duration_ms", "BIGINT", (
        "Milliseconds from started_at to the end of the target write: the read from SQL "
        "Server, these facts and the append. Offset planning and the checkpoint commit are "
        "not included.")),
    ("target", "STRING", "Table name or path the batch was written to."),
    ("written_at", "TIMESTAMP", "When this facts row was written, after the target commit."),
]
_FACT_FIELDS = [name for name, _, _ in FACTS_COLUMNS[:-2]]  # target and written_at are added last
FACTS_SCHEMA = ", ".join(f"{name} {data_type}" for name, data_type, _ in FACTS_COLUMNS[:-2])


def batch_facts(df: DataFrame) -> dict:
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.min("_start_lsn").alias("min_lsn"),
        F.max("_start_lsn").alias("max_lsn"),
        F.min("_commit_ts").alias("min_commit_ts"),
        F.max("_commit_ts").alias("max_commit_ts"),
        F.sum(F.when(F.col("_operation") == 1, 1).otherwise(0)).alias("deletes"),
        F.sum(F.when(F.col("_operation") == 2, 1).otherwise(0)).alias("inserts"),
        # updates count after-images (operation 4); before-images (3) pair with them
        F.sum(F.when(F.col("_operation") == 4, 1).otherwise(0)).alias("updates"),
    ).first()
    return row.asDict()


def _json(facts: dict) -> str:
    """Facts as the target commit's userMetadata; commit times as ISO-8601 with milliseconds."""
    return json.dumps(facts, separators=(",", ":"),
                      default=lambda v: v.isoformat(timespec="milliseconds"))


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


def delta_sink(target: str, app_id: str, facts_table: str | None = None):
    """Return a ``foreachBatch`` function.

    ``app_id`` must be stable for the lifetime of a checkpoint. If the checkpoint is
    deleted, use a new ``app_id``; batch ids restart at 0 and would otherwise be
    ignored as duplicates.
    """
    created: set[str] = set()  # once per query run, not once per batch

    def ensure(spark, table: str, columns, comment: str) -> None:
        if table not in created:
            create_if_not_exists(spark, table, columns, comment)
            created.add(table)

    def write_batch(df: DataFrame, batch_id: int) -> None:
        started_at, t0 = datetime.now(timezone.utc), time.monotonic()
        df = df.persist()
        try:
            facts = batch_facts(df)
            if not facts["rows"]:
                return
            facts.update({"batch_id": batch_id, "app_id": app_id})
            spark = df.sparkSession
            out = df.withColumn("_batch_id", F.lit(batch_id))
            ensure(spark, target, [(f.name, f.dataType, BRONZE_COLUMN_COMMENTS.get(f.name))
                                   for f in out.schema], BRONZE_COMMENT)
            _write(out, target, app_id, batch_id, _json(facts))
            if facts_table:
                facts.update(started_at=started_at, duration_ms=round((time.monotonic() - t0) * 1000))
                ensure(spark, facts_table, FACTS_COLUMNS, FACTS_COMMENT)
                facts_df = spark.createDataFrame(
                    [tuple(facts[k] for k in _FACT_FIELDS)], FACTS_SCHEMA).withColumn(
                    "target", F.lit(target)).withColumn("written_at", F.current_timestamp())
                _write(facts_df, facts_table, f"{app_id}#facts", batch_id)
        finally:
            df.unpersist()

    return write_batch
