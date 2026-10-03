# 0025: Seed a target from an existing copy of the table

**Status:** accepted  
**Date:** 2026-10-01T16:53:59-03:00  
**Amended:** 2026-10-01T19:44:50-03:00, a rerun finds its seed after cleanup and under a newer snapshot; a fall-back hour maps earlier (see the Amendment)  
**Amended:** 2026-10-02T20:30:12-03:00, a seed's rows carry `_snapshot`; seed or a chunked snapshot (see Amendment 2, ADR 0028)

## Context
A snapshot has to finish within the CDC retention, or the changes after its LSN are purged
before the stream reads them (ADR 0016, 0018). At the 4,000 to 11,000 rows per second
measured against a production source, three days hold roughly 1 to 3 billion rows: tables
of billions of rows cannot be snapshotted, and a stream from `earliest` lacks every row not
changed within the retention. Such tables usually have a copy already, an existing lake
table loaded by another tool.

The bootstrap guide documented a manual path: record `max_lsn` before the copy starts, load
the copy yourself, start the stream at that `startingLsn`. It needs an LSN recorded before
the copy, which a copy someone else took rarely has (its start time is known), and it
leaves the copy out of the target, where neither `to_delta(bootstrap=True)` nor silver's
rebuild (ADR 0019) sees it as a snapshot.

## Decision
* `CdcStream.seed(target, df, as_of)` appends the copy to the target as a snapshot, the
  rows a bootstrap writes: `_operation = 0`, `_start_lsn` the LSN `L` of `as_of`,
  `_commit_ts` its commit time, `_seqval`, `_command_id` and `_batch_id` NULL,
  `_capture_instance` the configured name. One Delta commit, `mergeSchema` as the stream's,
  userMetadata `{"seed": ci, "lsn", "commit_ts"}`. It returns `{lsn, commit_ts}`.
  `to_delta(bootstrap=True)` then finds it as the target's snapshot and starts at `L`
  without reading the table; silver rebuilds from it like from any snapshot.
* `as_of` is an LSN recorded before the copy started, or the time it started, in UTC (an
  aware `datetime` is converted). Every commit at or before it must be in the copy; later
  ones may be, the stream replays them and the latest image per key absorbs the overlap
  (ADR 0016). A time maps to `sys.fn_cdc_map_time_to_lsn('largest less than or equal', t)`
  (`client.time_to_lsn`), `t` on the server's clock: `tran_end_time` is local (ADR 0008),
  so UTC is converted with `AT TIME ZONE 'UTC'` then `AT TIME ZONE <zone>`, or by the fixed
  offset before SQL Server 2022, the inverse of what commit times get. To the second: the
  function takes `datetime`, which rounds milliseconds to 1/300 s, upwards too. Every
  approximation goes earlier: an earlier `L` replays commits the copy has, a later one would
  skip commits it may lack.
* Checked before anything is written: a time with no commit at or before it, or an `L`
  whose next changes CDC no longer holds (the pre-flight's test, ADR 0018 and 0023), raise
  `DataLossError`: the copy is older than the retention. An `L` above `max_lsn` is a
  `ValueError`. The copy's columns match the captured ones by name ignoring case, as SQL
  Server resolves names; other columns are dropped and each value is cast to the stream's
  type. A captured column the copy lacks is a `ValueError`, or NULL with
  `allow_missing_columns=True`, as a snapshot reads a column dropped from the table.
* The facts event is `'bootstrap'`, with the key `to_delta` uses (`<app_id>#events`,
  version 0), so `seed` takes `app_id` with `facts_table`. A seed is the target's initial
  snapshot; silver, the facts documentation and invariant 14 already count exactly
  `'bootstrap'` and `'resnapshot'` rows as snapshots, and a new name would have to be added
  to every consumer's filter. `to_delta(bootstrap=True)` with the same `app_id` then writes
  no second row: Delta skips it.
* A rerun at the same `L` returns the seed already in the target and writes nothing. Any
  other snapshot of the table there raises: a second seed appended silently would move the
  point downstream rebuilds from. `reseed=True` appends a copy newer than it, the manual
  recovery after `DataLossError` for a table no re-snapshot can read, followed by a new
  checkpoint and `app_id` (invariant 8); an older one still raises.
  - Considered: replacing the target's rows. It deletes what the stream wrote and needs a
    rewrite of billions of rows; appending a newer snapshot is what re-snapshots already do.

## Consequences
* The copy's consistency is the user's: a commit at or before `as_of` missing from it is
  never corrected. A time read on another clock than SQL Server's needs the skew subtracted;
  earlier is always safe.
* No new permission: `fn_cdc_map_time_to_lsn` reads `cdc.lsn_time_mapping`, which needs no
  grant (invariant 11), and the other checks are the stream's own.
* `tests/test_client_sql.py` pins the query for a named zone, UTC and the pre-2022 offset.
  `tests/test_delta_sink.py` seeds a copy of the fake's table with an aware `as_of`, its
  columns in another case plus an extra one, and checks the stamps, that the stream
  continues from `L` with no second snapshot and one `'bootstrap'` row, the rerun, the
  refusal of a second seed, `reseed`; and the refusals of a purged point, a time before CDC,
  an LSN after `max_lsn` and missing columns. `tests/integration` seeds a copy taken by
  SELECT on a server whose clock is UTC-3, maps its UTC start to an LSN at or after the
  copy's last commit and committed no later than the start, and the latest image per key
  equals the table after the changes made after the copy.
* ponytail: whether the target holds a snapshot is the same aggregate over its
  operation-0 rows `snapshot` runs, three columns of billions of seeded rows. Keep the
  newest snapshot LSN elsewhere if it shows up.

## Amendment: reruns after cleanup, and the fall-back hour
* Cleanup deletes the `cdc.lsn_time_mapping` rows below the lowest low watermark too
  (`sys.sp_cdc_cleanup_change_table`, checked on SQL Server 2022), so once it passes a time
  `as_of`, `time_to_lsn` returns nothing and the job's rerun raised `DataLossError` instead
  of returning its seed. The rerun is now resolved first: with an `L`, the target's
  operation-0 rows at `L`, not its newest snapshot, so a snapshot appended later (a switch's,
  a reseed) does not turn the rerun into a `ValueError`; with no `L` for the time (and no
  `reseed`), the newest snapshot of the table committed at or before `as_of` is taken as
  that seed. Only with no such snapshot does the missing commit raise `DataLossError`. The
  fake's `cleanup` deletes those mapping rows as well.
* A fall-back repeats an hour of a named zone's clock. For an `as_of` in the first
  occurrence of that hour, commits up to an hour later have earlier local times and larger
  LSNs, so `'largest less than or equal'` picked one after `as_of` and the stream skipped the
  commits in between. The time passed is now the earlier of `as_of`'s local time and the
  local time an hour later less that hour: off a fall-back that is `as_of`'s own, and in it,
  an hour earlier, which only replays. `tests/integration` checks the conversion on SQL
  Server for `Eastern Standard Time` around the 2026 changes.

## Amendment 2: `_snapshot`, and a seed or a chunked snapshot
* A seed is a whole snapshot: its rows get `_snapshot` = L (bronze migration 2, ADR 0016
  Amendment 3), and the rerun and refusal checks look for whole snapshots only, by
  `coalesce(_snapshot, _start_lsn)`, never at a chunked snapshot's chunk rows.
* A table too big to snapshot within the retention now has two ways in: this seed, when a
  copy exists (hours, for a table whose chunked snapshot would take weeks), or a chunked
  snapshot read next to the stream (ADR 0028), when none does. `reconcile()` validates
  either against the source.
