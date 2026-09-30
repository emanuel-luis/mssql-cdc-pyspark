"""Migrations for bronze tables: the metadata columns (sink.BRONZE_COLUMN_COMMENTS); captured columns follow the source.

Append only; see ``mssql_cdc.migrations``. For example::

    MIGRATIONS = [
        Migration("add source_host", lambda spark, table: add_columns(
            spark, table, [("source_host", "STRING", "SQL Server the batch was read from.")])),
    ]
"""

from .base import Migration

MIGRATIONS: list[Migration] = []
