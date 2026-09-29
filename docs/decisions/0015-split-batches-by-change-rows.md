# 0015: Split batches by the change table's rows

**Status:** accepted  
**Date:** 2026-09-29T10:31:46-03:00

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
