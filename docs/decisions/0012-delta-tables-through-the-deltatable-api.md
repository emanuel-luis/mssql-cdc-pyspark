# 0012: Delta tables through the `DeltaTable` API, created typed and commented

**Status:** accepted  
**Date:** 2026-09-28T21:09:56-03:00

## Context
The control table was created with a `CREATE TABLE IF NOT EXISTS` string and advanced
with a SQL `MERGE` whose source row had to be smuggled in as a DataFrame, because
parameter markers stay unbound on Delta sessions. The bronze and facts tables were
created implicitly by their first append, so their types followed whatever the first
batch held and no column said what it meant.

## Decision
* Delta tables are handled with delta-spark's `DeltaTable` API (`mssql_cdc.tables`):
  `DeltaTable.createIfNotExists` to create, `forName`/`forPath` to open, and
  `DeltaTable.merge` for the monotonic `finalized_until` update. Appends stay on the
  DataFrame writer, which is what carries `txnAppId`/`txnVersion`.
* Every table is created with explicit types and a comment on the table and on each
  column that explains what it holds and how to use it:
  * control table: all columns (`finalized_until` is `TIMESTAMP_NTZ`, UTC);
  * facts table: all columns; commit times are `TIMESTAMP_NTZ` (UTC), timings `TIMESTAMP`
    and `BIGINT`;
  * bronze: the metadata columns (`_start_lsn`, `_operation`, ...); captured columns keep
    the source's names and types.
* No migrations: tables are created in this shape. An existing table is left as it is
  (`createIfNotExists` is a no-op), and one with an older facts schema must be recreated.

## Consequences
* No SQL strings for Delta DDL or MERGE in the core, so no parameter markers to work
  around and no identifier quoting for paths.
* `finalization` and `sink` import `delta.tables` at call time: the `delta-spark` Python
  package must be installed (the `spark` extra; Databricks runtimes ship it).
* Comments are English, like the rest of the project; they show up in
  `DESCRIBE TABLE` and in catalog UIs.
* `tests/test_delta_sink.py` checks the types and that every control and facts column is
  commented.
