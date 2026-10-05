# 0015: Split batches by the change table's rows

**Status:** accepted  
**Date:** 2026-09-29T10:31:46-03:00  
**Amended:** 2026-10-05T13:53:56-03:00, tiles merged until a range holds 50,000 rows; captured columns cached between plannings (see the Amendment)

## Context
With `numPartitions > 1`, `split_points` cut each batch into ranges holding the same number
of commits from `cdc.lsn_time_mapping`. Those commits are database-wide, and a capture
instance's changes are not spread evenly over them. On a production table (ERP invoice
lines, batches of 100k commits, 8 ranges) the largest range held 1.23x to 1.92x the mean
rows; since a batch ends when its slowest partition does, up to half the parallelism was
lost.

## Decision
`split_points(capture_instance, from, to, n)` tiles the capture instance's own change rows:
`NTILE(n) OVER (ORDER BY __$start_lsn)` on `cdc.<ci>_CT` in the range, and each range ends at
the largest commit LSN of its tile. A commit whose rows straddle two tiles stays whole in
the first range, so ranges remain commit-aligned (invariant 2).

## Consequences
* Balanced ranges on the same production batches: 1.00x the mean (one trailing range can
  be empty when the last tile ends before the batch end; it reads nothing).
* Planning got cheaper there too: 2.1–2.5 s against 6.2–6.4 s for the commit tiles.
* Planning now scans the change table's range on the server (no transfer); the read that
  follows finds those pages cached.
* The fake tiles its change rows the same way. `tests/test_source_fake.py` and
  `tests/integration` check that eight one-row commits plus one eight-row commit split
  into two ranges of eight rows.

## Amendment: small tiles merged, fewer round trips per batch
Every batch, however small, was cut into up to `numPartitions` tiles, all the cores with
`auto` (ADR 0011), and every range costs a task, a login and the queries around its read
(ADR 0014). On the production link of ADR 0014 (180–950 ms a round trip) that fixed cost is
far above reading tens of rows, the steady size of a busy table's batches; on a quiet table
the last range was often an empty tail of idle entries.

Now:
* `split_points` also returns each tile's rows (`COUNT_BIG(*)`, free in the grouped query).
  The reader merges adjacent tiles until a range holds `MIN_ROWS_PER_PARTITION` rows,
  50,000; a tail short of it, the idle range past the batch's last change included, joins
  the range before. A batch of fewer than about 100,000 change rows is one range; a large
  one keeps the balanced tiles above. A fixed constant, not an option. ponytail: one floor
  for every table and link; an option if a workload needs batches below twice it split.
* The planner caches each capture instance's captured columns
  (`sys.sp_cdc_get_captured_columns`) by name and `create_date`, one query less per instance
  and planning. ALTER COLUMN changes the captured types without changing either, so when
  `sys.sp_cdc_get_ddl_history` finds DDL inside the batch the reader drops the cache and
  reads the columns again before ADR 0023's type check (D1). The fake caches the same way,
  so a missed refetch fails `test_a_running_query_stops_at_a_type_change`.
* Only the batch's last range measures the stream's position for the facts (ADR 0014,
  Amendment 5).

Measured in the local lab (SQL Server 2022 CU27 in Docker, Spark `local[16]`,
`numPartitions=auto`, the default driver, `to_delta` with a facts table, so metrics are on).
Each run wrote 10 small transactions and read them in one `availableNow` batch of 21 to 40
change rows; a probe counted every login and query by reader method. Three runs after a
warm-up, before and after the change:

| Per batch | Before | After |
|---|---|---|
| Ranges (tasks) | 8 | 1 |
| Logins | 8 | 1 |
| Queries in the ranges | 72, 9 each | 9 |
| Planning queries (`latestOffset`, `partitions`) | 7 | 7 |
| Trigger time, median | 20.5 s | 17.7 s |
| `read_seconds` (task-seconds) | 0.52–0.54 | 0.02 |

With `maxCommitsPerBatch=3` (four batches of 2 to 13 rows a run, two runs each) a batch of
three commits went from 3 ranges, 3 logins and 27 queries to 1, 1 and 9, and planning from 6
queries to 5 after the run's first batch, with the captured columns cached. On the lab's
sub-millisecond link the time saved is Spark's, seven fewer tasks and metrics files; at 3
ranges or fewer the trigger times overlap (about 10 to 13 s a batch either way). On a slow
link the round trips are what count: ranges run in parallel, so a batch still waits for one
range's login and 9 queries (about 2–10 s at 180–950 ms), but the source serves an eighth of
the logins and queries, and the stream holds one task slot instead of eight, which is what
many streams on one cluster compete for (ADR 0027).
`tests/test_reader_units.py` pins the merge and the queries of a range;
`tests/test_source_fake.py` reads a small batch in one partition through the engine, and
the tests of balanced ranges lower the floor for their few rows.
