# 0016: Bootstrap with a snapshot stamped with an LSN recorded before the read

**Status:** accepted  
**Date:** 2026-09-29T12:38:28-03:00  
**Amended:** 2026-09-30T11:02:01-03:00, NTILE tiles for composite and non-integer keys (see the Amendment)  
**Amended:** 2026-09-30T15:21:04-03:00, the capture instance matches ignoring case (see Amendment 2)  
**Amended:** 2026-10-01T17:55:59-03:00, key bounds bound as text for either backend (ADR 0003 Amendment 2)  
**Amended:** 2026-10-02T20:30:12-03:00, a snapshot is named by `_snapshot`; chunked snapshots (see Amendment 3, ADR 0028)  
**Amended:** 2026-10-03T18:10:05-03:00, chunk bounds planned once, from row counts (Amendment 3's last bullet, superseded by ADR 0028's Amendment)

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
* T-SQL has no row-value comparison, and a seek takes equalities on leading key columns
  plus a range on the next one. Written in one WHERE,
  `a >= x AND (a > x OR (a = x AND b >= y)) AND ...` seeks on `a` alone: a range inside one
  value of `a` reads all of it (2500 rows out of 10000 read, `tests/integration`), and with
  (company, id) keys and one company, n partitions read the table n times. So a range is
  cut into disjoint pieces of the seekable shape, one SELECT each, joined by UNION ALL: the
  leading values both bounds share become equalities, then from (x, y) to (u, v) the
  pieces are `a = x AND b >= y`, `a > x AND a < u` and `a = u AND b < v`. Every range of
  8 over 40000 rows, with one, three or twenty leading values, reads exactly its 5000 rows
  (`tests/integration`).
* x and u can differ in Python and be equal in SQL: `'n'` and `'N'` under a
  case-insensitive collation. The first piece then also checks
  `(a, b) < (u, v)` and the last `a > x`, which empties it, so the first piece alone reads
  the range and every row still comes once (`tests/integration`, table `snap_case`).
* The bounds come back typed through Arrow and are bound as text parameters (arrow-odbc binds
  nothing else; binary as hex through `CONVERT(<type>, ?, 1)`, ADR 0003 Amendment 2),
  `CAST(? AS <declared type>)`, with the type built from `sys.sp_cdc_get_captured_columns`
  (length, precision and scale; documented API, invariant 11) and validated before it is
  inlined (invariant 13). A bare string parameter is nvarchar and converts a varchar key
  column instead (`CONVERT_IMPLICIT` in the plan); the CAST keeps the column as it is. LSNs
  still cross as hex strings (invariant 6). `KeyRange` holds only plain Python values, so it
  pickles, and `read()` stays stateless (invariant 5).
* A char or varchar bound is converted in its column's collation,
  `CAST(? COLLATE <collation> AS varchar(n))`, the collation read from `sys.columns` (which
  shows the columns of a table the login can SELECT; not a `cdc.*` table). Without it the
  conversion uses the database default's code page: a Greek_CI_AS key in a CP1252 database
  turns every letter into `?`, the bounds lose their order and the partitions come out 0, 0
  and 10 rows instead of 4, 3 and 3 (`tests/integration`, table `snap_greek`). A column
  whose collation is not visible stays one partition.
* A bound need not be exact, only monotonic: consecutive ranges use complementary
  predicates, so every row lands in exactly one of them. datetime2(7) and datetimeoffset(7)
  bounds come back truncated to microseconds, which keeps their own column's order but can
  swap two bounds when another key column follows: (t, 5) < (t + 100 ns, 3) come back as
  (T, 5) > (T, 3). So only the last key column may have such a type; elsewhere the key stays
  one partition. So does a key with a `time` column, or with a type that has no Spark
  mapping: `time` comes back as Arrow time64[ns] (nine digits in
  `test_inferred_columns_round_trip_every_mapped_type`), which has no Python value to bind.
* The tiling query's helper columns are `[__$tile]` and `[__$prev]`, CDC's own prefix: named
  `g` and `p` they collided with key columns of those names, and SQL Server refused the query.
* NULL sorts first in every column, as in ORDER BY, so the first range would take rows with
  a NULL leading key. That is defensive: SQL Server refuses a CDC index over nullable columns
  (`tests/integration`). The fake allows NULL keys and follows the same order.
* `FakeCdcDatabase(keys=...)` takes a column or a list of them, and the fake tiles like
  NTILE. `tests/test_client_sql.py` pins the queries; `tests/test_source_fake.py` checks
  that composite and string keys read every row exactly once, NULL keys and more partitions
  than rows included; `tests/integration` reads composite and varchar primary keys (a
  non-default collation and mixed case included) with `numPartitions=3` in tiles of 4, 3
  and 3 rows that together equal the table.
* ponytail: the NTILE query reads and spools the whole key; a range would be cheaper with
  `ROW_NUMBER` over the index and a separate `COUNT(*)`, if a large table shows it. The
  tiles run on both backends, with a `snap_kinds` table for the other bound types (ADR 0003).

## Amendment 2: the capture instance matches ignoring case
A production SQL Server 2016 stores `dbo_ORDER_ITEMS` and the config says
`dbo_order_items`. The stream worked: `fn_cdc_get_min_lsn`,
`sp_cdc_get_captured_columns` and the change table resolve the name under the database's
collation, case-insensitive by default. `source_table` compared the rows of
`sp_cdc_help_change_data_capture` exactly and reported the instance as not found, which
broke the snapshot, `bootstrap`, `on_data_loss="resnapshot"` and silver's key inference.

* `source_table` takes the exact name first, else the one name equal to it ignoring case;
  two such names (a case-sensitive database can hold both) raise an error naming them.
* "A snapshot of the capture instance" in the target, and silver's rows of it (ADR 0019),
  match `_capture_instance` ignoring case, so a rerun spelled differently finds the snapshot
  and takes no second one. The column keeps the name as the options gave it.
* The fake resolves names the same way. `tests/test_client_sql.py`,
  `tests/test_delta_sink.py`, `tests/test_silver.py` and `tests/integration` (a bootstrap
  with the name upper-cased, then a rerun in its own case) check it.

## Amendment 3: the snapshot a row belongs to, and chunked snapshots
A chunked snapshot (ADR 0028) reads the table in chunks next to the running stream, each
stamped with its own LSN L at or after the snapshot's LSN S, so "the snapshot's LSN is the
largest `_start_lsn` of the operation-0 rows" no longer holds.

* Bronze gains `_snapshot` (bronze migration 2): on snapshot rows, the LSN of the snapshot
  they belong to, recorded before any of its rows was read; NULL on change rows. A whole
  snapshot, as this ADR takes it, writes `_snapshot = _start_lsn`; rows written before the
  column exist read `coalesce(_snapshot, _start_lsn)`. `_chunk` numbers a chunked
  snapshot's chunks and is NULL on a whole one.
* `snapshot()` and `to_delta(bootstrap=True)` find the target's snapshot among whole ones
  only (`_chunk` NULL). A chunked one is complete only when its `'bootstrap'` facts row is
  written, after its last chunk; until then `to_delta(bootstrap=True, snapshot="chunked")`
  returns the S of the snapshot it opened, never a newer LSN: the rule of this ADR, a
  rerun never skips the changes after the snapshot, holds for both.
* The stamp is still recorded before the read: per wave of chunks instead of once, and never
  below S (P2 of ADR 0028). A chunk's rows can be newer than its stamp, as a whole
  snapshot's can be newer than its LSN; READ COMMITTED (or SNAPSHOT where allowed), never
  `NOLOCK`.
* Chunks do not use the NTILE tiles above: an integer key steps over [MIN, MAX], any other
  key takes keyset bounds per wave, a `TOP (n + 1)` per seekable piece of `_key_select`
  (`tests/integration` checks it reads at most `n + 1` rows per piece). Superseded by ADR
  0028's Amendment: the first `backfill()` call plans every chunk, an integer key from row
  counts per slice and any other key by those keyset seeks, all before the first wave.
