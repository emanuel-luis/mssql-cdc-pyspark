"""Migrations for a bronze table's snapshot plans table (events.PLANS_COLUMNS), from 0.7.0.

Append only; see ``mssql_cdc.migrations``.
"""

from .base import Migration

MIGRATIONS: list[Migration] = []
