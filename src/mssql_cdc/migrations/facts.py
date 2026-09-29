"""Migrations for the ingestion facts table (sink.FACTS_COLUMNS).

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration, add_columns

# Migration 1 (2026-09-29): network and read metrics. Frozen here as shipped; the sink's
# creation columns reuse it so new tables are born with the same definitions.
NETWORK_COLUMNS = [
    ("source_rtt_ms", "DOUBLE", (
        "Median round trip in milliseconds of 3 trivial queries from the Spark driver to SQL "
        "Server, taken while writing this batch: the network latency at that moment. NULL "
        "unless delta_sink(source_options=...) is set.")),
    ("read_seconds", "DOUBLE", (
        "Seconds the batch's partitions spent reading from SQL Server, summed over partitions "
        "(task-seconds: with parallel partitions it can exceed wall time), including Spark "
        "taking the rows as they arrive. NULL unless the source option metricsPath and "
        "delta_sink(metrics_path=...) are set.")),
    ("read_mb", "DOUBLE", (
        "Megabytes of Arrow data the partitions read, after the cast to the Spark schema, "
        "summed. NULL under the same condition as read_seconds.")),
    ("network_wait_ms", "BIGINT", (
        "Milliseconds SQL Server waited for the client to take the rows (ASYNC_NETWORK_IO of "
        "each partition's session), summed. Close to read_seconds * 1000 means the network, "
        "not the server, set the pace. NULL under the same condition, or where the server "
        "does not expose sys.dm_exec_session_wait_stats.")),
]

MIGRATIONS: list[Migration] = [
    Migration("network and read metrics",
              lambda spark, table: add_columns(spark, table, NETWORK_COLUMNS)),
]
