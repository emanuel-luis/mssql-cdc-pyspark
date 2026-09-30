"""Migrations for bronze tables: the metadata columns (sink.BRONZE_COLUMN_COMMENTS); captured columns follow the source.

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration, set_comments


# Migration 1 (2026-09-30): a stream follows a newer capture instance of its table (ADR 0023):
# _capture_instance names the one each row came from, and _command_id is numbered per instance.
def _capture_instance_comments(spark, table: str) -> None:
    from ..sink import BRONZE_COLUMN_COMMENTS, BRONZE_COMMENT
    from ..tables import delta_table

    present = set(delta_table(spark, table).toDF().columns)  # includeCommandId=false: none
    changed = [n for n in ("_capture_instance", "_command_id") if n in present]
    set_comments(spark, table, {n: BRONZE_COLUMN_COMMENTS[n] for n in changed}, BRONZE_COMMENT)


MIGRATIONS: list[Migration] = [
    Migration("capture instance comments", _capture_instance_comments),
]
