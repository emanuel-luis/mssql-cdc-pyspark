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
    (
        "source_rtt_ms",
        "DOUBLE",
        (
            "Network latency to SQL Server during the batch: the median, over its partitions, of "
            "one round trip (SELECT 1) each made on its own connection just before reading, in "
            "milliseconds. NULL unless the source option metricsPath and "
            "delta_sink(metrics_path=...) are set."
        ),
    ),
    (
        "read_seconds",
        "DOUBLE",
        (
            "Seconds the batch's partitions spent reading from SQL Server, summed over partitions "
            "(task-seconds: with parallel partitions it can exceed wall time), including Spark "
            "taking the rows as they arrive. NULL unless the source option metricsPath and "
            "delta_sink(metrics_path=...) are set."
        ),
    ),
    (
        "read_mb",
        "DOUBLE",
        (
            "Megabytes of Arrow data the partitions read, after the cast to the Spark schema, "
            "summed. NULL under the same condition as read_seconds."
        ),
    ),
    (
        "network_wait_ms",
        "BIGINT",
        (
            "Milliseconds SQL Server waited for the client to take the rows (ASYNC_NETWORK_IO of "
            "each partition's session), summed. Close to read_seconds * 1000 means the network, "
            "not the server, set the pace. NULL under the same condition, or where the server "
            "does not expose sys.dm_exec_session_wait_stats."
        ),
    ),
]

# Migration 2 (2026-09-29): retention headroom (ADR 0017).
RETENTION_COLUMNS = [
    (
        "retention_watermark_ts",
        "TIMESTAMP_NTZ",
        (
            "How far CDC cleanup had deleted when the batch was read: the commit time (UTC) of "
            "sys.fn_cdc_get_min_lsn for the capture instance, the latest seen by the batch's "
            "partitions after reading. Changes committed before it are gone from the source. NULL "
            "unless the source option metricsPath and delta_sink(metrics_path=...) are set."
        ),
    ),
    (
        "retention_headroom_hours",
        "DOUBLE",
        (
            "Hours between retention_watermark_ts and max_commit_ts: how far the stream is ahead of "
            "what cleanup has deleted. A current stream sits near the retention period (3 days by "
            "default); it shrinks as the stream falls behind, and at 0 the next changes to read are "
            "being purged. Cleanup moves the watermark in steps (the default job runs daily), so "
            "alert with more margin than that interval, and also when facts stop arriving: a "
            "stopped stream keeps its last value while the real headroom keeps shrinking. NULL "
            "under the same condition as retention_watermark_ts."
        ),
    ),
]

# Migration 3 (2026-09-29): snapshot events, the bootstrap and re-snapshots after data
# loss (ADR 0018).
EVENT_COLUMNS = [
    (
        "event",
        "STRING",
        (
            "What the row records: NULL for a micro-batch; 'bootstrap' for the initial snapshot of "
            "the target; 'resnapshot' for a snapshot taken because CDC cleanup purged changes "
            "before the stream read them. Event rows have no batch_id; min_lsn = max_lsn is the "
            "LSN the snapshot is stamped with, and the only trace of a snapshot of an empty table "
            "(rows = 0), which writes no target rows."
        ),
    ),
    (
        "lost_from_ts",
        "TIMESTAMP_NTZ",
        (
            "On 'resnapshot' rows, the UTC commit time of the last offset the stream had processed. "
            "Changes committed after it and before lost_to_ts were purged unread: the target has "
            "the rows as of the snapshot, but those changes are missing from its change history. "
            "NULL on other rows."
        ),
    ),
    (
        "lost_to_ts",
        "TIMESTAMP_NTZ",
        (
            "On 'resnapshot' rows, the UTC commit time of the CDC retention watermark "
            "(sys.fn_cdc_get_min_lsn) when the loss was detected: where the gap in the change "
            "history ends. NULL on other rows."
        ),
    ),
]

MIGRATIONS: list[Migration] = [
    Migration(
        "network and read metrics", lambda spark, table: add_columns(spark, table, NETWORK_COLUMNS)
    ),
    Migration(
        "retention headroom", lambda spark, table: add_columns(spark, table, RETENTION_COLUMNS)
    ),
    Migration("snapshot events", lambda spark, table: add_columns(spark, table, EVENT_COLUMNS)),
]
