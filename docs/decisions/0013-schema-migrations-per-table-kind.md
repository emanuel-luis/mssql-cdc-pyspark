# 0013: Schema migrations per table kind

**Status:** accepted  
**Date:** 2026-09-28T21:26:25-03:00

## Context
ADR 0012 creates the control, facts and bronze tables in a fixed shape and leaves
existing tables alone. The next change to one of those shapes would have no way to reach
tables that already exist in users' catalogs.

## Decision
* `mssql_cdc/migrations/` holds one module per table kind (`control.py`, `facts.py`,
  `bronze.py`), each with an append-only `MIGRATIONS` list. It starts empty.
* A table is created in its kind's latest shape and stamped with the number of migrations
  of that kind in the table property `mssql_cdc.schema_version`. A table without the
  property counts as version 0.
* When the sink (first batch of a run) or `finalization.advance()` opens a table, the
  migrations past its version run in order, and the property is bumped after each one.
  Nothing is written when the table is current.
* `add_columns()` covers the common case: nullable columns, with their comments, added by
  an empty append with `mergeSchema` (a metadata-only commit; old rows read NULL).
  Anything else is a function `(spark, table)` of its own.
* Changing a kind means both: its creation columns (new tables) and a new migration
  (existing tables). A shipped migration is never edited, reordered or removed: its
  position is its version.

## Consequences
* Schema changes reach existing tables without manual DDL, once, in order.
* One extra metadata read (`DeltaTable.detail()`) each time a table is opened.
* The version lives in a table property. Setting it is a metadata commit that can conflict
  with concurrent writers, which is why ADR 0004 keeps `finalized_until` out of
  properties; here it only happens while migrating.
* `tests/test_delta_sink.py` injects a migration and checks it runs once and stamps the
  table; new tables are born at version 0.
