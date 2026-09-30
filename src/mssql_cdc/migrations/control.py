"""Migrations for the finalization control table (finalization.CONTROL_COLUMNS).

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration, add_columns

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

MIGRATIONS: list[Migration] = [
    Migration(
        "add applied_lsn and snapshot_lsn",
        lambda spark, table: add_columns(spark, table, APPLIED_COLUMNS),
    ),
]
