"""Migrations for bronze tables: the metadata columns (sink.BRONZE_COLUMN_COMMENTS); captured columns follow the source.

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration, add_columns, set_comments


# Migration 1 (2026-09-30): a stream follows a newer capture instance of its table (ADR 0023):
# _capture_instance names the one each row came from, and _command_id is numbered per instance.
def _capture_instance_comments(spark, table: str) -> None:
    from ..sink import BRONZE_COLUMN_COMMENTS, BRONZE_COMMENT
    from ..tables import delta_table

    present = set(delta_table(spark, table).toDF().columns)  # includeCommandId=false: none
    changed = [n for n in ("_capture_instance", "_command_id") if n in present]
    set_comments(spark, table, {n: BRONZE_COLUMN_COMMENTS[n] for n in changed}, BRONZE_COMMENT)


# Migration 2 (2026-10-02): chunked snapshots (ADR 0028). Snapshot rows say which snapshot they
# belong to (_snapshot) and which chunk of it read them (_chunk); a chunk's rows are stamped
# with their own LSN, so the snapshot is no longer the largest _start_lsn of operation 0.
def _snapshot_columns(spark, table: str) -> None:
    from ..sink import BRONZE_COLUMN_COMMENTS, BRONZE_COMMENT

    added = [("_snapshot", "STRING"), ("_chunk", "INT")]
    add_columns(spark, table, [(n, t, BRONZE_COLUMN_COMMENTS[n]) for n, t in added])
    set_comments(spark, table, {"_start_lsn": BRONZE_COLUMN_COMMENTS["_start_lsn"]}, BRONZE_COMMENT)


MIGRATIONS: list[Migration] = [
    Migration("capture instance comments", _capture_instance_comments),
    Migration("snapshot and chunk columns", _snapshot_columns),
]
