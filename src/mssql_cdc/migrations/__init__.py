"""Schema migrations for the tables this library creates, one module per table kind.

A table is created in its latest shape and stamped with the number of migrations its
kind has (table property ``mssql_cdc.schema_version``). A table with a lower number gets
the missing ones, in order, the next time a stream or ``finalization.advance()`` opens
it. The property is only written when a migration runs.

To change a table kind:

1. change its creation columns (``sink.FACTS_COLUMNS``, ``sink.BRONZE_COLUMN_COMMENTS``,
   ``finalization.CONTROL_COLUMNS``, ``silver.SILVER_COLUMNS``,
   ``reconcile.REPORT_COLUMNS``), so new tables are born with the change;
2. append a ``Migration`` to ``migrations/<kind>.py``, so existing tables get it. For a
   new column, ``add_columns()`` (an empty append with ``mergeSchema``) is usually enough;
   for a column whose meaning changes, ``set_comments()`` gives existing tables its new
   comment.

Never edit, reorder or remove a migration that has shipped: its position is its version.

An older release keeps writing a table a newer one migrated (it logs a WARNING), so jobs that
share a table can upgrade one at a time. A migration after which an older release would
misread or miswrite the rows must also set ``mssql_cdc.min_version`` to its own number, both
when it runs and on the tables created afterwards: releases that know fewer migrations then
refuse the table instead (ADR 0013).
"""

from .base import (
    MIN_VERSION_PROPERTY,
    SCHEMA_VERSION_PROPERTY,
    Migration,
    add_columns,
    current_version,
    ensure,
    migrate,
    set_comments,
)

__all__ = [
    "MIN_VERSION_PROPERTY",
    "SCHEMA_VERSION_PROPERTY",
    "Migration",
    "add_columns",
    "current_version",
    "ensure",
    "migrate",
    "set_comments",
]
