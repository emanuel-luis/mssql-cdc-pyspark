# 0016: Bootstrap with a snapshot stamped with an LSN recorded before the read

**Status:** accepted  
**Date:** 2026-09-29T12:38:28-03:00  
**Amended:** 2026-09-30T11:02:01-03:00, NTILE tiles for composite and non-integer keys (see the Amendment)

## Context
The stream starts from what CDC retention still holds (`startingLsn=earliest`), which is
three days by default. Rows written before CDC was enabled, or changed only before
retention's window, never reach the target. A table needs a full load that meets the
stream at an exact point, without a gap and without a lock on the source.

## Decision
* Record `L0 = sys.fn_cdc_get_max_lsn()` **before** reading the table (or the capture
  instance's first LSN minus one, when capture has not reached it yet). That first LSN comes
  from `start_lsn` in `sys.sp_cdc_help_change_data_capture`: on a quiet database `max_lsn`
  stays below a new instance's first LSN for up to ~5 minutes after the enable, and
  `fn_cdc_get_min_lsn` returns NULL all that time (seen in `tests/integration`). Every commit up to
  `L0` is already in the table. A commit the read also sees came later, so its LSN is above
  `L0` and the stream replays it.
* Read the table's current rows through the same backend (`format("mssql_cdc_snapshot")`),
  in the stream's schema: `_operation = 0`, `_start_lsn = L0`, `_commit_ts` its commit time,
  `_seqval`, `_command_id` and `_batch_id` NULL. Operation 0 is ours: 1–4 are SQL Server's,
  and a snapshot row is not an insert (facts count inserts; a consumer that ignores unknown
  codes misses the rows visibly rather than miscounting them).
* READ COMMITTED, never `NOLOCK`: a dirty read can keep a row that a rollback removes, and
  no change row would ever correct it. A non-repeatable scan can still miss or repeat a row
  that moves while it is read; each such move is a commit after `L0`, so the stream carries it.
* Partitions: uniform ranges over MIN..MAX of the leading column of the capture instance's
  unique index (`sys.sp_cdc_help_change_data_capture`, the documented API, invariant 11),
  when it is an integer and captured; otherwise one partition. The open ends take rows
  inserted beyond the range meanwhile, and the first also takes NULL keys.
* `CdcStream.snapshot(target)` appends the rows to the bronze table in one Delta commit
  (userMetadata `{"snapshot": ci, "lsn", "commit_ts"}`) and returns `{lsn, commit_ts}`;
  `to_delta(..., bootstrap=True)` passes that LSN as `startingLsn`. When the target already
  holds a snapshot of the capture instance, `snapshot` returns its LSN and reads nothing:
  a rerun must not return a newer LSN than the rows it wrote, or the changes in between
  would be skipped. `resnapshot=True` takes a new one (recovery after `DataLossError`).

## Consequences
* The target converges to the source table once a MERGE applies the latest image per key by
  `(_start_lsn, _command_id, _seqval, _operation)`: the snapshot row sorts before every change
  read after it. The bronze table itself holds the overlap (a row can appear in the snapshot
  and again as a change): consumers of bronze must deduplicate, as they already must for
  updates.
* No new permission: the CDC query functions already need SELECT on the captured columns,
  and the snapshot reads only those (`tests/integration`, with a least-privilege login).
* After a re-snapshot, rows deleted during the purged gap have no delete row: downstream
  should rebuild from the newest snapshot (rows with `_start_lsn` at or above its LSN).
* ponytail: uniform key ranges skew on sparse keys; NTILE over the key, or tiles for
  composite and non-integer keys, when a table needs it.
* The fake keeps each keyed capture instance's current table rows (cleanup does not touch
  them). `tests/test_source_fake.py` checks the key ranges and the stamps;
  `tests/test_delta_sink.py` and `tests/integration` check that bootstrap runs once and the
  latest image per key equals the source table.

## Amendment: tiles for composite and non-integer keys
A table keyed by a composite or a string key was read in one partition, by one task on one
connection: slow on a big table.

* The key is every column of the capture instance's unique index, all in the stream's
  schema. One integer column keeps the uniform ranges over MIN..MAX: two seeks, where NTILE
  reads the whole key and spools it (a Table Spool in its plan, `tests/integration`). Sparse
  or skewed integer keys still give uneven ranges.
* Any other key, with `numPartitions` > 1: `NTILE(n) OVER (ORDER BY <every key column>)` on the
  source table, server-side, like `split_points` on the change table (ADR 0015). Only the
  first key tuple of tiles 2..n comes back; partition i reads `start(i) <= key < start(i+1)`,
  the first one open below and the last open above. Fewer rows than `n`: one row per
  partition. Empty table, or one row: one partition.
* T-SQL has no row-value comparison: `(a, b) >= (x, y)` is written
  `a >= x AND (a > x OR (a = x AND b >= y))`. The leading `a >= x` is redundant but
  sargable: with it a range of a 20000-row table reads about its own rows through a seek;
  without it the plan scans all 20000 (`tests/integration`).
* The bounds come back typed through Arrow and are bound as parameters,
  `CAST(? AS <declared type>)`, with the type built from `sys.sp_cdc_get_captured_columns`
  (length, precision and scale; documented API, invariant 11) and validated before it is
  inlined (invariant 13). A bare string parameter is nvarchar and converts a varchar key
  column instead (`CONVERT_IMPLICIT` in the plan); the CAST keeps the column as it is. LSNs
  still cross as hex strings (invariant 6). `KeyRange` holds only plain Python values, so it
  pickles, and `read()` stays stateless (invariant 5).
* A bound need not be exact, only monotonic: consecutive ranges use complementary
  predicates, so every row lands in exactly one of them. datetime2(7) and datetimeoffset(7)
  bounds come back truncated to microseconds, which only moves a boundary. A key with a
  `time` column, or with a type that has no Spark mapping, stays one partition: `time` comes
  back as Arrow time64[ns] (nine digits in `test_inferred_columns_round_trip_every_mapped_type`),
  which has no Python value to bind.
* NULL sorts first in every column, as in ORDER BY, so the first range would take rows with
  a NULL leading key. That is defensive: SQL Server refuses a CDC index over nullable columns
  (`tests/integration`). The fake allows NULL keys and follows the same order.
* `FakeCdcDatabase(keys=...)` takes a column or a list of them, and the fake tiles like
  NTILE. `tests/test_client_sql.py` pins the queries; `tests/test_source_fake.py` checks
  that composite and string keys read every row exactly once, NULL keys and more partitions
  than rows included; `tests/integration` reads a composite and a varchar primary key with
  `numPartitions=3` in tiles of 4, 3 and 3 rows that together equal the table.
* ponytail: the NTILE query reads and spools the whole key; a range would be cheaper with
  `ROW_NUMBER` over the index and a separate `COUNT(*)`, if a large table shows it. Typed
  bounds are untested on the `arrow-odbc` backend, like the rest of it (ADR 0003).
