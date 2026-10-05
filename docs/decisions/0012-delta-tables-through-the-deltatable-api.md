# 0012: Delta tables through the `DeltaTable` API, created typed and commented

**Status:** accepted  
**Date:** 2026-09-28T21:09:56-03:00  
**Amended:** 2026-09-28T21:15:37-03:00, every time column is `TIMESTAMP_NTZ` in UTC  
**Amended:** 2026-09-28T21:26:25-03:00, existing tables are migrated (ADR 0013)  
**Amended:** 2026-10-05T12:04:55-03:00, column mapping on a table created with a column name Delta refuses without it; every other Delta property is the user's (see the Amendment)

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
  * control table: all columns;
  * facts table: all columns;
  * bronze: the metadata columns (`_start_lsn`, `_operation`, ...); captured columns keep
    the source's names and types.
* Every time column is `TIMESTAMP_NTZ` in UTC: commit times, `finalized_until`, and the
  moments the library records (`started_at`, `written_at`, `updated_at`, taken from the
  Python clock in UTC). Mixing `TIMESTAMP` and `TIMESTAMP_NTZ` makes a difference such as
  `written_at - max_commit_ts` depend on the Spark session's time zone.
* Tables are created in this shape; `createIfNotExists` leaves an existing one as it is.
  Changes to existing tables go through schema migrations (ADR 0013).

## Consequences
* No SQL strings for Delta DDL or MERGE in the core, so no parameter markers to work
  around and no identifier quoting for paths.
* `finalization` and `sink` import `delta.tables` at call time: the `delta-spark` Python
  package must be installed (the `spark` extra; Databricks runtimes ship it).
* Comments are English, like the rest of the project; they show up in
  `DESCRIBE TABLE` and in catalog UIs.
* `tests/test_delta_sink.py` checks the types and that every control and facts column is
  commented.

## Amendment (2026-10-05): table properties

SQL Server allows any character in a bracketed column name (`[Unit Price]`, `[Qty (kg)]`),
and captured columns keep the source's names. Delta refuses a space or one of `,;{}()\n\t=`
in a column name unless the table has column mapping, so the first bronze write of such a
table failed (`DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`), and the `columns` option could
not rename the column: the change table is read by the schema's own names.

* `tables.create_if_not_exists` creates a table with `delta.columnMapping.mode = 'name'`
  when one of its columns has such a name. Only bronze and silver hold captured columns, so
  only they can get it; a table whose names Delta takes as they are is created as before.
* Existing tables are left alone: setting the property is a metadata commit that conflicts
  with running streams, and no table that needed it could have been created. A column with
  such a name that a newer capture instance adds later fails the append until the user
  enables column mapping on the table.
* No other Delta property is set by default. Deletion vectors (with row tracking on
  Databricks), liquid clustering or `ZORDER` by silver's keys, `OPTIMIZE`, optimized writes
  and auto compaction, and type widening each raise the protocol or change the table's file
  layout: they are documented as opt-ins in
  [Tables](../reference/tables.md#table-properties).
* Column mapping raises the table's protocol: every reader and writer of that table needs
  a Delta that supports it, a cost paid only where nothing worked before.
* `tests/test_silver.py` streams a column named `Qty (kg)` into bronze and silver, and checks
  that a table without such a name gets no column mapping.
