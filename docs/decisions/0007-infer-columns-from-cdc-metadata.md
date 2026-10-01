# 0007: Infer captured columns from CDC metadata

**Status:** accepted  
**Date:** 2026-09-28T17:03:21-03:00  
**Amended:** 2026-09-28T18:11:50-03:00, metadata from `sys.sp_cdc_get_captured_columns`
(see Consequences)  
**Amended:** 2026-10-01T17:55:59-03:00, the mapped types also round-trip through `arrow-odbc`

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
