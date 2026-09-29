"""One declaration for the common pipeline: SQL Server CDC -> Delta, with facts and metrics.

    from mssql_cdc import stream
    query = stream(spark, options).to_delta("bronze.orders", "orders-v1",
                                            checkpoint="/Volumes/cat/sch/vol/ckpt/orders",
                                            facts_table="ops.ingestion_facts")

The options go to ``readStream`` once, and the sink gets what it needs from them. With a
facts table, per-partition metrics default to ``<checkpoint>/_mssql_cdc_metrics`` when the
checkpoint is a path Python can write on every node (local, or FUSE such as a Volume);
with a URI checkpoint (``dbfs:/``, ``abfss://``...) set ``metricsPath`` yourself.
"""

from __future__ import annotations

import os
import re

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]+:")  # a scheme; a Windows drive has one letter


class CdcStream:
    def __init__(self, spark, options: dict):
        self.spark, self.options = spark, dict(options)

    def to_delta(self, target: str, app_id: str, checkpoint: str, facts_table: str | None = None,
                 trigger: dict | None = None, query_name: str | None = None):
        """Start the stream into ``target`` through ``delta_sink``; returns the StreamingQuery.

        ``trigger``: keyword arguments for ``DataStreamWriter.trigger``, e.g.
        ``{"availableNow": True}``.
        """
        from . import register
        from .sink import delta_sink

        register(self.spark)
        options = dict(self.options)
        metrics = next((v for k, v in options.items() if k.lower() == "metricspath"), None)
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
