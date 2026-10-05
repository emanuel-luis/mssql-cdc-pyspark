# 0013: Schema migrations per table kind

**Status:** accepted  
**Date:** 2026-09-28T21:26:25-03:00  
**Amended:** 2026-10-05T06:01:09-03:00, an older release keeps writing a table a newer one migrated unless `mssql_cdc.min_version` asks for more; migrations retry concurrent commits (see Amendment)

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

## Amendment: older releases keep writing, unless a migration says otherwise
Many jobs share a facts or control table: every stream of a fan-out, every tracker and
silver job (ADR 0026, ADR 0027). They upgrade one at a time, and a job rolled back to the
previous release must still run. So:

* A release that finds a table stamped with a schema version above its own count of
  migrations writes it as it is and logs one WARNING per table and version. The migrations
  so far only add nullable columns or change comments: an append without the new columns
  reads NULL in them, like the rows written before the migration, and a MERGE leaves them
  alone.
* A migration after which an older release would misread or miswrite the rows sets the
  table property `mssql_cdc.min_version` to its own number, when it runs and on the tables
  created from then on. A release that knows fewer migrations of the kind than that number
  raises `ValueError` instead of writing. No migration sets it yet; the check ships first,
  since a release cannot learn it later.
* Every job migrates a shared table on the same upgrade, and a migration's commit can lose
  to another job's (Delta's `MetadataChangedException`, `ConcurrentTransactionException`).
  `migrate` runs again on such a conflict, with the control table MERGE's backoff, for up to
  a minute: re-running a migration changes nothing.
