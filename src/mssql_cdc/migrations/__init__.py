"""Schema migrations for the tables this library creates, one module per table kind.

A table is created in its latest shape and stamped with the number of migrations its
kind has (table property ``mssql_cdc.schema_version``). A table with a lower number gets
the missing ones, in order, the next time a stream or ``finalization.advance()`` opens
it. The property is only written when a migration runs.

To change a table kind:

1. change its creation columns (``sink.FACTS_COLUMNS``, ``sink.BRONZE_COLUMN_COMMENTS``,
   ``finalization.CONTROL_COLUMNS``), so new tables are born with the change;
2. append a ``Migration`` to ``migrations/<kind>.py``, so existing tables get it. For a
   new column, ``add_columns()`` (an empty append with ``mergeSchema``) is usually enough.

Never edit, reorder or remove a migration that has shipped: its position is its version.
"""

from .base import SCHEMA_VERSION_PROPERTY, Migration, add_columns, current_version, ensure, migrate

__all__ = [
    "SCHEMA_VERSION_PROPERTY",
    "Migration",
    "add_columns",
    "current_version",
    "ensure",
    "migrate",
]
