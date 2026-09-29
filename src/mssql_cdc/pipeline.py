"""One declaration for the common pipeline: SQL Server CDC -> Delta, with facts and metrics.

    from mssql_cdc import stream
    query = stream(spark, options).to_delta("bronze.orders", "orders-v1",
                                            checkpoint="/Volumes/cat/sch/vol/ckpt/orders",
                                            facts_table="ops.ingestion_facts",
                                            bootstrap=True)

The options go to ``readStream`` once, and the sink gets what it needs from them. With a
facts table, per-partition metrics default to ``<checkpoint>/_mssql_cdc_metrics`` when the
checkpoint is a path Python can write on every node (local, or FUSE such as a Volume);
with a URI checkpoint (``dbfs:/``, ``abfss://``...) set ``metricsPath`` yourself.

``bootstrap=True`` first writes a snapshot of the tracked table into the target (once) and
starts a new checkpoint from its LSN, so the target holds the whole table, not only what CDC
retention still has (ADR 0016).
"""

from __future__ import annotations

import json
import os
import re

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]+:")  # a scheme; a Windows drive has one letter


def _opt(options: dict, key: str):
    return next((v for k, v in options.items() if k.lower() == key.lower()), None)


class CdcStream:
    def __init__(self, spark, options: dict):
        self.spark, self.options = spark, dict(options)

    def snapshot(self, target: str, resnapshot: bool = False) -> dict:
        """Append the tracked table's current rows to ``target`` as operation 0 and return the
        offset they are stamped with, for the stream's ``startingLsn``.

        The LSN is recorded before the table is read. Once ``target`` holds a snapshot of this
        capture instance, its offset is returned and nothing is read, so a rerun cannot skip
        the changes after it. ``resnapshot=True`` takes a new one: after a DataLossError, with
        a new checkpoint and app_id; downstream, rebuild from the newest snapshot.
        """
        from pyspark.sql import functions as F

        from . import migrations, register
        from .client import make_client
        from .sink import BRONZE_COMMENT, bronze_columns
        from .source import snapshot_lsn
        from .tables import is_path

        ci = _opt(self.options, "captureInstance")
        if not ci:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        if not resnapshot:
            done = self._last_snapshot(target, ci)
            if done:
                return done
        client = make_client(self.options)
        try:
            lsn = snapshot_lsn(client, client.source_table(ci))
            offset = {"lsn": lsn, "commit_ts": client.lsn_to_time(lsn) or ""}
        finally:
            client.close()
        register(self.spark)
        rows = (self.spark.read.format("mssql_cdc_snapshot").options(**self.options)
                .option("snapshotLsn", lsn).load()
                .withColumn("_batch_id", F.lit(None).cast("int")))
        migrations.ensure(self.spark, target, "bronze", bronze_columns(rows), BRONZE_COMMENT)
        writer = (rows.write.format("delta").mode("append")
                  .option("userMetadata", json.dumps({"snapshot": ci, **offset})))
        writer.save(target) if is_path(target) else writer.saveAsTable(target)
        return offset

    def _last_snapshot(self, target: str, ci: str) -> dict | None:
        from pyspark.sql import functions as F

        from .tables import delta_table, is_path

        if is_path(target):
            from delta.tables import DeltaTable

            exists = DeltaTable.isDeltaTable(self.spark, target)
        else:
            exists = self.spark.catalog.tableExists(target)
        if not exists:
            return None
        row = (delta_table(self.spark, target).toDF()
               .where((F.col("_operation") == 0) & (F.col("_capture_instance") == ci))
               .agg(F.max("_start_lsn").alias("lsn"), F.max("_commit_ts").alias("ts")).first())
        if row is None or row["lsn"] is None:
            return None
        ts = row["ts"].isoformat(timespec="milliseconds") if row["ts"] else ""
        return {"lsn": row["lsn"], "commit_ts": ts}

    def to_delta(self, target: str, app_id: str, checkpoint: str, facts_table: str | None = None,
                 trigger: dict | None = None, query_name: str | None = None, bootstrap: bool = False):
        """Start the stream into ``target`` through ``delta_sink``; returns the StreamingQuery.

        ``trigger``: keyword arguments for ``DataStreamWriter.trigger``, e.g.
        ``{"availableNow": True}``. ``bootstrap``: snapshot the table first (see
        ``snapshot``); a checkpoint that already has offsets ignores the starting LSN.
        """
        from . import register
        from .sink import delta_sink

        register(self.spark)
        options = dict(self.options)
        if bootstrap:
            if _opt(options, "startingLsn"):
                raise ValueError("bootstrap=True sets startingLsn itself; pass one or the other")
            options["startingLsn"] = self.snapshot(target)["lsn"]
        metrics = _opt(options, "metricsPath")
        if facts_table and metrics is None and not _URI.match(checkpoint):
            metrics = options["metricsPath"] = os.path.join(checkpoint, "_mssql_cdc_metrics")
        writer = (
            self.spark.readStream.format("mssql_cdc").options(**options).load()
            .writeStream.foreachBatch(delta_sink(target, app_id, facts_table,
                                                 metrics_path=metrics if facts_table else None))
            .option("checkpointLocation", checkpoint)
        )
        if trigger:
            writer = writer.trigger(**trigger)
        if query_name:
            writer = writer.queryName(query_name)
        return writer.start()


def stream(spark, options: dict) -> CdcStream:
    """The CDC stream described by ``options`` (the data source options)."""
    return CdcStream(spark, options)
