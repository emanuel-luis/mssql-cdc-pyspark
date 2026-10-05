"""Migrations for the ingestion facts table (sink.FACTS_COLUMNS).

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration, add_columns, set_comments

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
            "Hours between retention_watermark_ts and end_commit_ts: how far the stream's position "
            "(the batch's end offset) is ahead of what cleanup has deleted. A current stream sits "
            "near the retention period (3 days by default), on a quiet table too, since a batch "
            "that read no rows also writes its row; it shrinks as the stream falls behind, and at 0 "
            "the next changes to read are being purged. Cleanup moves the watermark in steps (the "
            "default job runs daily), so alert with more margin than that interval, and also when "
            "facts stop arriving: the stream or CDC capture has stopped, and the real headroom "
            "keeps shrinking from the last value. Rows written before end_commit_ts existed "
            "measured from max_commit_ts, the batch's last change, which on a quiet table made a "
            "current stream look behind. NULL under the same condition as retention_watermark_ts."
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
            "What the row records: NULL for a micro-batch. Snapshots, with no batch_id: "
            "'bootstrap' for the initial snapshot of the target; 'resnapshot' for a snapshot taken "
            "because CDC cleanup purged changes before the stream read them; min_lsn = max_lsn is "
            "the LSN the snapshot is stamped with, and the only trace of a snapshot of an empty "
            "table (rows = 0), which writes no target rows. Changes to the source (ADR 0023), with "
            "the batch_id of the batch that read past them and rows = 0: 'schema_change' for DDL "
            "on the source table, 'capture_instance_switched' when the stream first read a newer "
            "capture instance of the table (the older one can be dropped once the same app_id has "
            "a row with a larger batch_id: Spark commits the batch after this row); min_lsn = "
            "max_lsn is the change's LSN, detail says what changed. Downstream rebuilds only from "
            "'bootstrap' and 'resnapshot' rows."
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

# Migration 4 (2026-09-30): capture and ingestion lag (ADR 0020).
LAG_COLUMNS = [
    (
        "source_max_commit_ts",
        "TIMESTAMP_NTZ",
        (
            "How far CDC capture had got when the batch was read: the commit time (UTC) of "
            "sys.fn_cdc_get_max_lsn, the latest seen by the batch's partitions after reading. The "
            "stream can read nothing newer than this. NULL unless the source option metricsPath "
            "and delta_sink(metrics_path=...) are set."
        ),
    ),
    (
        "capture_lag_seconds",
        "DOUBLE",
        (
            "How stale CDC capture was: seconds from source_max_commit_ts to the moment a "
            "partition read it (its own clock, UTC), the largest over the batch's partitions. "
            "Seconds on a busy database; up to about 5 minutes on a quiet one, where capture "
            "writes an idle entry that often, unless the heartbeat job runs. Growing beyond that "
            "means capture is slow (a large log backlog), whatever the stream does. A stopped "
            "capture (capture job or SQL Server Agent down) does not show here: max_lsn freezes, "
            "no batch runs and the last value stays small; only facts stop arriving. For that, "
            "use now minus latestOffset.commit_ts in the query progress. Clock skew between the "
            "Spark nodes and SQL Server shifts it. NULL under the same condition as "
            "source_max_commit_ts."
        ),
    ),
    (
        "ingestion_lag_seconds",
        "DOUBLE",
        (
            "Seconds between end_commit_ts and source_max_commit_ts: how far the stream's position "
            "(the batch's end offset) is behind what CDC capture had processed. Near 0 for a "
            "current stream, on a quiet table too, since a batch that read no rows also writes its "
            "row. Growing means the stream is falling behind, and as it grows "
            "retention_headroom_hours shrinks. Only moves while the stream runs: also alert when "
            "facts stop arriving (the stream or CDC capture has stopped). Rows written before "
            "end_commit_ts existed measured from max_commit_ts, which also counted the time from "
            "the table's last change to the database's newest commit. NULL under the same "
            "condition as source_max_commit_ts."
        ),
    ),
]

# Migration 5 (2026-09-30): the batch's end offset. Headroom and ingestion lag are measured
# from it, not from the batch's last change, and a batch that read no rows writes its row too
# (ADR 0014 amendment 3, ADRs 0017 and 0020 amended). The migration also gives existing tables
# the new comments of the columns whose meaning changed.
END_COLUMNS = [
    (
        "end_lsn",
        "STRING",
        (
            "The batch's end offset: the commit LSN (0x + 20 hex) the stream had processed up to "
            "after this batch, the largest to_lsn of its partitions. At or after max_lsn: offsets "
            "follow CDC capture (sys.fn_cdc_get_max_lsn), which moves with idle entries and with "
            "other tables' commits, so on a quiet table it keeps moving while its batches read no "
            "rows. On event rows, the snapshot's or the change's LSN. NULL unless the source "
            "option metricsPath and delta_sink(metrics_path=...) are set, and on a batch that "
            "planned no range to read (a new checkpoint's first batch when nothing is new)."
        ),
    ),
    (
        "end_commit_ts",
        "TIMESTAMP_NTZ",
        (
            "Commit time (UTC) of end_lsn: how far through the source's commit history the stream "
            "had read after this batch, whether the batch had rows or not. retention_headroom_hours "
            "and ingestion_lag_seconds are measured from it. Later than max_commit_ts, the batch's "
            "last change, when the table changed less recently than the database. On event rows, "
            "the snapshot's or the change's commit time. NULL under the same condition as end_lsn."
        ),
    ),
]

# Migration 6 (2026-09-30): schema changes and capture instance switches as event rows of the
# batch that read past them (ADR 0023). The migration also gives existing tables the new
# comments of the columns event rows now use differently.
DETAIL_COLUMNS = [
    (
        "detail",
        "STRING",
        (
            "On 'schema_change' rows, the DDL statement; on 'capture_instance_switched' rows, "
            "'old -> new' capture instance, plus the columns the query reads that the new one "
            "does not capture (NULL from then on). NULL on other rows."
        ),
    ),
]


# Migration 7 (2026-10-02): chunked snapshots (ADR 0028) add the 'snapshot_open' and
# 'snapshot_chunk' rows, with JSON in detail. The shipped definitions above stay as they were;
# the creation columns take these comments, and the migration gives them to existing tables.
SNAPSHOT_COMMENTS = {
    "event": (
        "What the row records: NULL for a micro-batch. Snapshots, with no batch_id: "
        "'bootstrap' for the initial snapshot of the target; 'resnapshot' for a snapshot taken "
        "because CDC cleanup purged changes before the stream read them; min_lsn = max_lsn is "
        "the LSN the snapshot is stamped with, and the only trace of a snapshot of an empty "
        "table (rows = 0), which writes no target rows. A chunked snapshot writes its "
        "'bootstrap' or 'resnapshot' row once its last chunk is in, after 'snapshot_open' when "
        "it opened (min_lsn = max_lsn = its LSN S, recorded before any chunk was read, where its "
        "stream generation starts) and one 'snapshot_chunk' row per chunk read (min_lsn = the "
        "LSN the chunk was stamped with, recorded before it was read, at or after S; max_lsn = "
        "max_lsn after the read); an open snapshot that a newer one supersedes is abandoned "
        "and never gets that row. Changes to the source (ADR 0023), with the batch_id of the "
        "batch that read past them and rows = 0: 'schema_change' for DDL on the source table, "
        "'capture_instance_switched' when the stream first read a newer capture instance of the "
        "table (the older one can be dropped once the same app_id has a row with a larger "
        "batch_id: Spark commits the batch after this row); min_lsn = max_lsn is the change's "
        "LSN, detail says what changed. Downstream rebuilds only from 'bootstrap' and "
        "'resnapshot' rows."
    ),
    "detail": (
        "On 'schema_change' rows, the DDL statement; on 'capture_instance_switched' rows, "
        "'old -> new' capture instance, plus the columns the query reads that the new one does "
        "not capture (NULL from then on). JSON on the rows of a chunked snapshot: "
        "'snapshot_open' {mode, keys, plan, generation, lost_from_ts, lost_to_ts}; "
        "'snapshot_chunk' {snapshot, chunk, wave, lo, hi, last}, the chunk's key range from lo "
        "(inclusive) to hi (exclusive), null for an open end and a list for a composite key, "
        "last true on the plan's final chunk, whose read completes the snapshot; its "
        "'bootstrap' or 'resnapshot' row {snapshot, chunks, rows, last_lsn}. NULL on other rows."
    ),
}


# Migration 8 (2026-10-03): a chunked snapshot's chunks are planned once, by its first
# backfill() call, and recorded in a 'snapshot_plan' row; a full snapshot writes
# 'snapshot_open' too, and its detail says the mode ('full' or 'chunked') and the kind
# ('bootstrap' or 'resnapshot'): while a snapshot of one mode is open, no snapshot of the other
# is taken (ADR 0028). Comments only; the creation columns take these, over migration 7's.
MODE_COMMENTS = {
    "event": (
        "What the row records: NULL for a micro-batch. Snapshots, with no batch_id: "
        "'bootstrap' for the initial snapshot of the target; 'resnapshot' for a snapshot taken "
        "because CDC cleanup purged changes before the stream read them; min_lsn = max_lsn is "
        "the LSN the snapshot is stamped with, and the only trace of a snapshot of an empty "
        "table (rows = 0), which writes no target rows. Before reading the table, a snapshot "
        "stream().to_delta takes writes 'snapshot_open' (min_lsn = max_lsn = an LSN S recorded "
        "first; detail says its mode, full or chunked): it stays open until a 'bootstrap' or "
        "'resnapshot' row of the same stream has max_lsn at or after S, and while it is open no "
        "snapshot of the other mode is taken. A full snapshot writes that row once its read "
        "ends (a rerun after a crash reads it again); a chunked one once its last chunk is in, "
        "after 'snapshot_plan' when its first backfill() call planned every chunk (min_lsn = "
        "max_lsn = S; started_at and duration_ms the planning's) and one 'snapshot_chunk' row "
        "per chunk read (min_lsn = the LSN the chunk was stamped with, recorded before it was "
        "read, at or after S; max_lsn = max_lsn after the read), and its stream generation "
        "starts at S; an open chunked snapshot that a newer one supersedes is abandoned and "
        "never gets that row. Changes to the source (ADR 0023), with the batch_id of the batch "
        "that read past them and rows = 0: 'schema_change' for DDL on the source table, "
        "'capture_instance_switched' when the stream first read a newer capture instance of the "
        "table (the older one can be dropped once the same app_id has a row with a larger "
        "batch_id: Spark commits the batch after this row); min_lsn = max_lsn is the change's "
        "LSN, detail says what changed. Downstream rebuilds only from 'bootstrap' and "
        "'resnapshot' rows."
    ),
    "detail": (
        "On 'schema_change' rows, the DDL statement; on 'capture_instance_switched' rows, "
        "'old -> new' capture instance, plus the columns the query reads that the new one does "
        "not capture (NULL from then on). JSON on the rows of snapshots: 'snapshot_open' "
        "{mode ('full' or 'chunked'), kind ('bootstrap' or 'resnapshot'), generation, "
        "lost_from_ts, lost_to_ts, and on a chunked one keys and plan, the key's extent read "
        "just after S ({kind: 'int', lo, hi, rows} for one integer key, else {kind: 'keyset', "
        "max})}; 'snapshot_plan' {snapshot, kind, keys, chunk_rows, chunks}, chunks the [lo, hi] "
        "of every chunk in order, fixed while the snapshot is open; 'snapshot_chunk' "
        "{snapshot, chunk, wave, lo, hi, last}, the chunk's key range from lo (inclusive) to hi "
        "(exclusive), null for an open end and a list for a composite key, last true on the "
        "plan's final chunk, whose read completes the snapshot; a chunked snapshot's "
        "'bootstrap' or 'resnapshot' row {snapshot, chunks, rows, last_lsn}. NULL on other rows."
    ),
}


# Migration 9 (2026-10-03): a full snapshot's 'snapshot_open' stops holding the mode once CDC
# cleanup passes its S, and each full run writes its own (ADR 0028). Comment only; the
# creation columns take it, over migration 8's.
LOCK_COMMENTS = {
    "event": MODE_COMMENTS["event"]
    .replace(
        "no snapshot of the other mode is taken.",
        "no snapshot of the other mode is taken, though a full one stops counting once CDC "
        "cleanup passes S, as it can then never complete.",
    )
    .replace(
        "(a rerun after a crash reads it again)",
        "(a rerun after a crash reads it again, under a 'snapshot_open' of its own)",
    ),
}


# Migration 10 (2026-10-05): a batch that failOnDataLoss=false made skip purged changes writes
# a 'data_skipped' row with the gap (ADR 0018, Amendment 3). Comments only, with batch_id's
# and the table's; the creation columns take these, over the earlier ones.
SKIP_COMMENTS = {
    "event": LOCK_COMMENTS["event"].replace(
        "Downstream rebuilds only from 'bootstrap' and 'resnapshot' rows.",
        "'data_skipped' (failOnDataLoss=false), with the batch_id of the batch that skipped them "
        "and rows = 0: CDC cleanup had purged changes the stream had not read, and the batch "
        "went on from what it left; min_lsn = max_lsn is the capture instance's min_lsn it "
        "resumed at, lost_from_ts and lost_to_ts the gap, detail the LSNs skipped. Downstream "
        "rebuilds only from 'bootstrap' and 'resnapshot' rows: after a 'data_skipped' row the "
        "target lacks the skipped changes (a row deleted in the gap stays) until a new snapshot.",
    ),
    "lost_from_ts": (
        "On 'resnapshot' rows, the UTC commit time of the last offset the stream had processed. "
        "Changes committed after it and before lost_to_ts were purged unread: the target has "
        "the rows as of the snapshot, but those changes are missing from its change history. On "
        "'data_skipped' rows, the same for the batch's start offset: the changes it skipped were "
        "committed after it and before lost_to_ts, and are missing from the target. NULL on "
        "other rows, and when the stream had processed no offset since an explicit startingLsn."
    ),
    "lost_to_ts": (
        "On 'resnapshot' and 'data_skipped' rows, the UTC commit time of the CDC retention "
        "watermark (sys.fn_cdc_get_min_lsn) when the loss was detected: where the gap in the "
        "change history ends. NULL on other rows."
    ),
    "detail": MODE_COMMENTS["detail"].replace(
        "NULL on other rows.",
        "On 'data_skipped' rows, 'from..to': the first LSN not read and the min_lsn reading "
        "resumed at. NULL on other rows.",
    ),
}


def _end_offset(spark, table: str) -> None:
    from ..sink import FACTS_COLUMNS, FACTS_COMMENT  # the comments new tables are created with

    add_columns(spark, table, END_COLUMNS)
    changed = ("rows", "retention_headroom_hours", "ingestion_lag_seconds")
    set_comments(spark, table, {n: c for n, _, c in FACTS_COLUMNS if n in changed}, FACTS_COMMENT)


def _source_events(spark, table: str) -> None:
    from ..sink import FACTS_COLUMNS, FACTS_COMMENT

    add_columns(spark, table, DETAIL_COLUMNS)
    changed = ("batch_id", "rows", "event", "end_lsn", "end_commit_ts")
    set_comments(spark, table, {n: c for n, _, c in FACTS_COLUMNS if n in changed}, FACTS_COMMENT)


def _chunked_snapshots(spark, table: str) -> None:
    from ..sink import FACTS_COLUMNS, FACTS_COMMENT

    changed = ("rows", "event", "detail")
    set_comments(spark, table, {n: c for n, _, c in FACTS_COLUMNS if n in changed}, FACTS_COMMENT)


def _snapshot_modes(spark, table: str) -> None:
    from ..sink import FACTS_COLUMNS, FACTS_COMMENT

    set_comments(
        spark, table, {n: c for n, _, c in FACTS_COLUMNS if n in MODE_COMMENTS}, FACTS_COMMENT
    )


def _skipped_changes(spark, table: str) -> None:
    from ..sink import FACTS_COLUMNS, FACTS_COMMENT

    changed = ("batch_id", *SKIP_COMMENTS)
    set_comments(spark, table, {n: c for n, _, c in FACTS_COLUMNS if n in changed}, FACTS_COMMENT)


MIGRATIONS: list[Migration] = [
    Migration(
        "network and read metrics", lambda spark, table: add_columns(spark, table, NETWORK_COLUMNS)
    ),
    Migration(
        "retention headroom", lambda spark, table: add_columns(spark, table, RETENTION_COLUMNS)
    ),
    Migration("snapshot events", lambda spark, table: add_columns(spark, table, EVENT_COLUMNS)),
    Migration("lag metrics", lambda spark, table: add_columns(spark, table, LAG_COLUMNS)),
    Migration("end offset", _end_offset),
    Migration("source change events", _source_events),
    Migration("chunked snapshot events", _chunked_snapshots),
    Migration("snapshot plans and modes", _snapshot_modes),
    Migration(
        "full snapshot opens per run",
        lambda spark, table: set_comments(spark, table, LOCK_COMMENTS),
    ),
    Migration("skipped changes", _skipped_changes),
]
