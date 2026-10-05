# 0007: Infer captured columns from CDC metadata

**Status:** accepted  
**Date:** 2026-09-28T17:03:21-03:00  
**Amended:** 2026-09-28T18:11:50-03:00, metadata from `sys.sp_cdc_get_captured_columns`
(see Consequences)  
**Amended:** 2026-10-01T17:55:59-03:00, the mapped types also round-trip through `arrow-odbc`  
**Amended:** 2026-10-05T12:19:45-03:00, computed columns are left out of the inferred schema
(see the Amendment)

## Context
The source required a `columns` option: a hand-written DDL of the captured columns. It
duplicates what SQL Server already knows (`cdc.captured_columns`), drifts when a new
capture instance adds columns, and is easy to get wrong (a `DECIMAL(18,2)` typed as
`DOUBLE`, a column missing from the list).

## Decision
When `columns` is omitted, `schema()` asks the client for the captured columns at
`load()` time, on the driver. `SqlCdcClient` runs `sys.sp_cdc_get_captured_columns`,
sorts by `column_ordinal`, and maps each SQL Server type to a default Spark type.
`columns` stays as an override (subset of columns, different types). The fake backend
has no type metadata and still requires `columns`.

## Consequences
* One call per `load()`, with the permissions of the CDC query functions: `SELECT` on
  the captured source columns and, if the capture instance has one, membership in its
  gating role. The first version read `cdc.change_tables` and `cdc.captured_columns`,
  which a login with only those permissions cannot read (checked against SQL Server
  2022); the procedure is the documented API for the same metadata.
* The schema is fixed for the life of a query; a new capture instance needs a restart,
  as before.
* Types without a sensible default (`sql_variant`, CLR types) fail loudly at `load()`
  instead of guessing; the error points to `columns`.
* Every mapped type round-trips through `mssql-python` on SQL Server 2022
  (`tests/integration`), and through `arrow-odbc` since ADR 0003's Amendment 2.

## Amendment: computed columns are left out
SQL Server CDC captures a computed column but stores NULL for it in every change row,
persisted or not (`tests/integration`). Inferred like any other column, it had its value in
snapshot rows and NULL in every change after them: silver drifted to NULL on every key
changed since the snapshot, and `reconcile()` reported those keys as `RECORD_DIFF` forever.

* The inferred schema leaves computed columns out, and `load()` logs a warning naming them.
  The client tells them by `sys.columns.is_computed`, matched to the captured columns by
  `column_id`, as `present_columns` already matches them. `sys.columns` shows the columns of
  a table the login can `SELECT`, so least privilege (invariant 11) needs no new grant
  (`tests/integration` checks it with that login).
* Listed in `columns`, a computed column stays in the schema and reads NULL in every row: a
  snapshot does not read it either, so every row agrees. The first planning logs a warning.
* `reconcile()` does not compare computed columns, so a silver table built before this
  change, with values on the keys not changed since its snapshot, still matches.
* Keeping the column in the inferred schema, NULL in snapshots too, would have kept the
  schema stable for downstream SQL; but a column that never carries a change's value is
  better computed downstream from the columns it derives from. A bronze table written before
  keeps the column: Delta appends the rows without it as NULL.
