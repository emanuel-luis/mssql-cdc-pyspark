"""Migrations for silver tables: the metadata columns (silver.SILVER_COLUMNS); captured columns
follow the source.

Append only; see ``mssql_cdc.migrations``.
"""

from .base import Migration

MIGRATIONS: list[Migration] = []
