"""Migrations for the ingestion facts table (sink.FACTS_COLUMNS).

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration, add_columns

# Migration 1 (2026-09-29): network and read metrics. Frozen here as shipped; the sink's
# creation columns reuse it so new tables are born with the same definitions. (Revised the
# same day, before any release: source_rtt_ms moved from a sink ping to the partitions.)
NETWORK_COLUMNS = [
    ("source_rtt_ms", "DOUBLE", (
        "Network latency to SQL Server during the batch: the median, over its partitions, of "
        "one round trip (SELECT 1) each made on its own connection just before reading, in "
        "milliseconds. NULL unless the source option metricsPath and "
        "delta_sink(metrics_path=...) are set.")),
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
