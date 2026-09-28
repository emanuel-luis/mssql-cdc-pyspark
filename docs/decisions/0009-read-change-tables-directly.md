# 0009: Read the change table directly

**Status:** accepted (2026-09). Supersedes the read path described in invariants 2–4 of
`CLAUDE.md` before this change.

## Context
The reader queried `cdc.fn_cdc_get_all_changes_<ci>(from, to, N'all update old')` and,
by default (`includeCommandId=true`), selected and ordered by `__$command_id`. On SQL
Server 2022 the function does not return that column: every default read failed with
`Invalid column name '__$command_id'` (CI lab t3, and a strict xfail in
`tests/integration`).

`__$command_id` is not optional detail. The change table documentation says
`__$seqval` "should not be used for ordering. Instead, use the `__$command_id`
column", and that `__$start_lsn`, `__$command_id` and `__$operation` together preserve
the commit order. The column exists in `cdc.<ci>_CT` (added by a cumulative update in
2012–2016, KB 3030352), just not in the function's result.

The function also did one thing for us: it rejects a range below the capture instance's
low watermark (Msg 313). A range that CDC cleanup purged therefore failed the read.

## Decision
`SqlCdcClient.iter_changes` reads `cdc.[<ci>_CT]` with
`WHERE __$start_lsn BETWEEN from AND to`, joined to `cdc.lsn_time_mapping` for the commit
time, ordered by `(__$start_lsn, __$command_id, __$seqval, __$operation)`. `'all update
old'` needs no filter: the change table holds operations 1–4.

A purged range now reads as empty instead of failing, so the retention guard runs twice:

1. on the driver, in `partitions()`, as before;
2. on the executor, **after** a range is read: if `fn_cdc_get_min_lsn(ci)` is now past the
   range's `from_lsn`, raise `DataLossError` (unless `failOnDataLoss=false`).

The second check is sound because `sys.sp_cdc_cleanup_change_table` first moves the
capture instance's `start_lsn` (what `fn_cdc_get_min_lsn` returns) and only then deletes
change rows below it. Any deletion that could have touched the range is visible as a
moved low watermark by the time the read finishes.

## Consequences
* `includeCommandId=true` works (verified on SQL Server 2022) and ordering follows the
  documented key. Servers without the column need `includeCommandId=false`.
* Microsoft recommends the query functions over the change tables. We depend on the
  documented change-table columns and on the documented cleanup order. t3 and
  `tests/integration` check the columns, and that cleanup moves `min_lsn` and deletes
  the rows below it; the move-before-delete order is taken from the documentation (see
  `docs/REFERENCES.md`) and cannot be observed from a test.
* The login needs `SELECT` on `cdc.<ci>_CT`. The reader already reads
  `cdc.lsn_time_mapping`, `cdc.change_tables` and `cdc.captured_columns`.
* One more `fn_cdc_get_min_lsn` query per task.
* `FakeCdcDatabase.cleanup` now also deletes the rows below the watermark, like the real
  procedure, and `FakeCdcClient.iter_changes` no longer emulates Msg 313.
