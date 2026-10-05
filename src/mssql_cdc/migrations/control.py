"""Migrations for the finalization control table (finalization.CONTROL_COLUMNS).

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration, add_columns, set_comments

# Migration 1 (2026-09-30): the position of tables built by silver.apply_changes (ADR 0019).
APPLIED_COLUMNS = [
    (
        "applied_lsn",
        "STRING",
        (
            "Tables built by mssql_cdc.apply_changes: the highest source commit LSN (0x + 20 "
            "hex) of the bronze changes applied to the table; the next call reads the changes "
            "after it. NULL for other tables."
        ),
    ),
    (
        "snapshot_lsn",
        "STRING",
        (
            "Tables built by mssql_cdc.apply_changes: the LSN of the bronze snapshot the table "
            "was last rebuilt from; a newer snapshot rebuilds it. NULL for other tables and "
            "before the first snapshot."
        ),
    ),
]

# Migration 2 (2026-10-02): the position of apply_changes in an open chunked bootstrap
# snapshot. Its chunk rows are stamped above the snapshot's LSN but land after changes
# silver has applied, so applied_lsn cannot track them; the waves can, per snapshot.
WAVE_COLUMNS = [
    (
        "open_snapshot_lsn",
        "STRING",
        (
            "Tables built by mssql_cdc.apply_changes: the LSN of the chunked bootstrap snapshot "
            "still being read (its 'snapshot_open' facts row, no completion row yet) whose chunks "
            "are applied as they arrive. NULL for other tables and when none is open."
        ),
    ),
    (
        "snapshot_wave",
        "INT",
        (
            "Tables built by mssql_cdc.apply_changes: the last wave of open_snapshot_lsn's chunks "
            "applied to the table; the next call applies the later ones. NULL when none is."
        ),
    ),
]

# Migration 3 (2026-10-03): a chunked re-snapshot's waves are applied as a bootstrap's are
# (ADR 0019 Amendment 2), so open_snapshot_lsn names either. Comment only; the creation
# columns take it, over migration 2's.
OPEN_COMMENTS = {
    "open_snapshot_lsn": (
        "Tables built by mssql_cdc.apply_changes: the LSN of the chunked snapshot, bootstrap or "
        "re-snapshot, still being read (its 'snapshot_open' facts row, no completion row yet) "
        "whose chunks are applied as they arrive. NULL for other tables and when none is open."
    ),
}

# Migration 4 (2026-10-04): over a re-snapshot's loss gap the verdict of a change-log table
# says less than "complete" (ADR 0018). Comment only; the creation columns take it.
VERDICT_COMMENTS = {
    "finalized_until": (
        "The verdict, UTC. Every period that ends at or before this instant is complete in the "
        "table: no source commit at or before it can still arrive. Over a recorded loss gap "
        "(lost_from_ts..lost_to_ts of the facts' 'resnapshot' rows), a change-log table's "
        "verdict means only that nothing more will arrive, not that the gap's changes are in "
        "it. It only moves forward. A consumer of the period [start, end) waits for "
        "finalized_until >= end."
    ),
}


MIGRATIONS: list[Migration] = [
    Migration(
        "add applied_lsn and snapshot_lsn",
        lambda spark, table: add_columns(spark, table, APPLIED_COLUMNS),
    ),
    Migration(
        "add open_snapshot_lsn and snapshot_wave",
        lambda spark, table: add_columns(spark, table, WAVE_COLUMNS),
    ),
    Migration(
        "open re-snapshots too", lambda spark, table: set_comments(spark, table, OPEN_COMMENTS)
    ),
    Migration(
        "the verdict over a loss gap",
        lambda spark, table: set_comments(spark, table, VERDICT_COMMENTS),
    ),
]
