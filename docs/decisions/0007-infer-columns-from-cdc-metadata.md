# 0007: Infer captured columns from CDC metadata

**Status:** accepted (2026-09)

## Context
The source required a `columns` option: a hand-written DDL of the captured columns. It
duplicates what SQL Server already knows (`cdc.captured_columns`), drifts when a new
capture instance adds columns, and is easy to get wrong (a `DECIMAL(18,2)` typed as
`DOUBLE`, a column missing from the list).

## Decision
When `columns` is omitted, `schema()` asks the client for the captured columns at
`load()` time, on the driver. `SqlCdcClient` reads `cdc.change_tables`,
`cdc.captured_columns` and `sys.columns`, in `column_ordinal` order, and maps each SQL
Server type to a default Spark type. `columns` stays as an override (subset of columns,
different types). The fake backend has no type metadata and still requires `columns`.

## Consequences
* One query per `load()`, with the same permissions the reader already needs on the
  `cdc` schema.
* The schema is fixed for the life of a query; a new capture instance needs a restart,
  as before.
* Types without a sensible default (`sql_variant`, CLR types) fail loudly at `load()`
  instead of guessing; the error points to `columns`.
* The mapping of `time`, `uniqueidentifier` and `datetimeoffset` depends on the Arrow
  types each driver returns, and still needs a lab check.
