"""Idempotent Delta sink for the CDC stream, recording per-batch facts.

* Writes are appends with ``txnAppId``/``txnVersion`` (Delta's idempotent-write
  options), keyed by the micro-batch id, so a replayed batch is skipped.
* Facts about each batch go into the Delta commit (``userMetadata``) and,
  optionally, into a facts table. The table matters: ``commitInfo`` is not kept in
  checkpoints and disappears with log cleanup (``delta.logRetentionDuration``).
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, functions as F

from .finalization import table_ref

FACTS_SCHEMA = (
    "app_id STRING, batch_id BIGINT, rows BIGINT, min_lsn STRING, max_lsn STRING, "
    "min_commit_ts STRING, max_commit_ts STRING, deletes BIGINT, inserts BIGINT, updates BIGINT"
)
_FACT_FIELDS = [f.split()[0] for f in FACTS_SCHEMA.split(", ")]


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
    facts = row.asDict()
    for key in ("min_commit_ts", "max_commit_ts"):
        if facts[key] is not None:
            facts[key] = facts[key].isoformat(timespec="milliseconds")
    return facts


def _write(df: DataFrame, target: str, app_id: str, version: int, metadata: str | None = None):
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", version)
    )
    if metadata is not None:
        writer = writer.option("userMetadata", metadata)
    if table_ref(target) != target:
        writer.save(target)
    else:
        writer.saveAsTable(target)


def delta_sink(target: str, app_id: str, facts_table: str | None = None):
    """Return a ``foreachBatch`` function.

    ``app_id`` must be stable for the lifetime of a checkpoint. If the checkpoint is
    deleted, use a new ``app_id``; batch ids restart at 0 and would otherwise be
    ignored as duplicates.
    """

    def write_batch(df: DataFrame, batch_id: int) -> None:
        df = df.persist()
        try:
            facts = batch_facts(df)
            if not facts["rows"]:
                return
            facts.update({"batch_id": batch_id, "app_id": app_id})
            _write(df.withColumn("_batch_id", F.lit(batch_id)), target, app_id, batch_id,
                   json.dumps(facts, separators=(",", ":")))
            if facts_table:
                spark = df.sparkSession
                facts_df = spark.createDataFrame(
                    [tuple(facts[k] for k in _FACT_FIELDS)], FACTS_SCHEMA).withColumn(
                    "target", F.lit(target)).withColumn("written_at", F.current_timestamp())
                _write(facts_df, facts_table, f"{app_id}#facts", batch_id)
        finally:
            df.unpersist()

    return write_batch
