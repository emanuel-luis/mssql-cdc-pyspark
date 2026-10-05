# 0018: Automatic re-snapshot after CDC data loss

**Status:** accepted  
**Date:** 2026-09-29T17:46:41-03:00  
**Amended:** 2026-10-02T20:30:12-03:00, a chunked re-snapshot opens at S and is read by `backfill()` (see the Amendment, ADR 0028)  
**Amended:** 2026-10-03T18:10:05-03:00, a full re-snapshot opens too, under the mode lock of ADR 0028's Amendment (see Amendment 2)  
**Amended:** 2026-10-05T12:46:11-03:00, a skip with `failOnDataLoss=false` leaves a `'data_skipped'` facts row with the gap (see Amendment 3)

## Context
CDC cleanup deletes change rows by age (three days by default) whether or not the stream
has read them. A stream stopped or behind for longer than that finds its next range purged,
and the retention guard raises `DataLossError` (invariant 4). ADR 0017 warns before that
point; after it, recovery was manual: take a new snapshot (`snapshot(target,
resnapshot=True)`, ADR 0016), move to a new checkpoint and a new `app_id` (invariant 8), and
start from the snapshot's LSN. Until a person does all of that in the right order, a
scheduled job fails on every run.

## Decision
* `to_delta(..., on_data_loss="resnapshot")` recovers on its own; `"fail"`, the default,
  keeps the old behaviour. The check runs **before** the query starts: read the last
  offset the checkpoint processed, and apply the driver guard's test,
  `increment_lsn(start) < min_lsn(ci)`, under the guard's own precondition, a range to read
  (`max_lsn > start`; with nothing captured after `start` nothing can be purged, and
  `fn_cdc_get_min_lsn` can still be NULL on a new instance). If the range after it is purged, take a new
  snapshot, move to a new generation and start from the snapshot's LSN. `to_delta` still
  returns the `StreamingQuery` without blocking. A purge during a run fails that query with
  `DataLossError` as before; the next run detects it and recovers.
  - Considered: a blocking `run()` that awaits the query and restarts it on `DataLossError`
    (changes `to_delta`'s contract and never returns under a continuous trigger), and a
    manual `recover()` (still needs a person, or glue in every job). The pre-flight fits the
    existing call and runs while nothing uses the checkpoint.
  - The last processed offset is the third line of `offsets/<n>` for the highest `n` in
    `commits/` (Spark's offset log format `v1`: the version, the batch metadata, the source
    offset). An offset file with no commit is a batch Spark will replay, not a processed one.
    Any other version line is a `ValueError` naming it. With no committed batch, the start
    is where the generation's stream starts: its snapshot LSN (`n > 0`), the newest snapshot
    in the target (generation 0 with `bootstrap=True`), or an explicit `startingLsn`;
    `earliest` and `latest` cannot start in a purged range.
  - The checkpoint must be a path that Python and Spark resolve to the same directory:
    local, or a Volume. A URI is a `ValueError`, and so is `/dbfs/...`, which Spark resolves
    as `dbfs:/dbfs/...` and Python as `dbfs:/...`: the pre-flight would see no commits and
    treat a healthy stream as purged.
* Generations. State file `<checkpoint>/_mssql_cdc_generation.json`,
  `{"generation", "snapshot_lsn", "commit_ts", "at"}` (plus `recovering` and `failed_at`
  while a recovery is open, see below; a generation-0 state holds only those), written atomically (temporary file
  and `os.replace`) and read on every `to_delta` call, whatever `on_data_loss` is.
  Generation `n > 0` uses the Spark checkpoint `<checkpoint>/_generations/<n>` and the
  `app_id` `<app_id>.g<n>`, and starts at the state's `snapshot_lsn`. No state file is
  generation 0: the checkpoint and `app_id` as given, so existing streams change nothing.
  The default `metricsPath` goes under the checkpoint of the live generation.
  - Considered: a sibling directory (`<checkpoint>.g1`) spreads one stream over paths the
    caller never named; renaming the old checkpoint aside and reusing the path is not atomic
    on object stores or FUSE, and a crash in the middle leaves no checkpoint at all. Nested
    keeps everything under the one path the caller passed, as `_mssql_cdc_metrics` already
    is, and one small file says which generation is live.
  - A new `app_id` because Delta's `txnVersion` is the batch id, which restarts at 0 with a
    new checkpoint. Deriving it (`.g<n>`) keeps the caller's `app_id` the only name to
    configure.
* Recovery, in order: (1) reuse the newest snapshot in the target when its LSN is above
  `start` and not purged itself (a crash after the snapshot, before the state file; a purged
  one is the fuzzy image of step 3 and is never reused); (2) otherwise the anti-loop guard
  below, then record `start` in the state file as `recovering` and take a new snapshot with
  `snapshot(target, resnapshot=True)`; (3) if the snapshot's own LSN is already purged (the
  read took longer than the retention), record `failed_at` and raise `DataLossError`; (4)
  write the event row; (5) write state `n + 1`, without `recovering` or `failed_at`. The
  state file is the commit point: a crash before it repeats the recovery from the recorded
  `start` (not from the new snapshot, which in generation 0 with no committed batch would
  look like the stream's start) without a second snapshot or a second event row.
* Events are rows in the facts table, facts migration 3 (ADR 0013): `event` (NULL for a
  micro-batch, `'bootstrap'` for the initial snapshot, `'resnapshot'` after a loss),
  `lost_from_ts` (commit time of the last offset processed) and `lost_to_ts` (commit time
  of `min_lsn` when the loss was found; also its `retention_watermark_ts`). An event row has
  no `batch_id`, `min_lsn = max_lsn` = the snapshot's LSN, `rows` and `duration_ms` of the
  snapshot read (NULL when it was reused; 0 for an empty table, whose snapshot writes no
  target rows and no Delta commit, so the event row is its only trace). `rows` comes from
  the snapshot's own commit, found by its `userMetadata` (auto compaction can commit right
  after it). It is appended with `txnAppId` `<app_id>#events` and `txnVersion` the
  generation it opens (0 for the bootstrap), so a rerun writes it once; the bootstrap event
  is offered on every generation-0 run, so a crash between the snapshot and its event only
  delays it. `on_data_loss="resnapshot"` requires a facts table: without the event rows,
  downstream cannot see an empty table's re-snapshot.
  - Considered: a separate events table (another table to create, migrate, document and
    join) and `userMetadata` on the snapshot commit only (lost with Delta log cleanup, the
    reason the facts table exists). The facts table is where the headroom alert already
    looks: one query shows the headroom falling to zero and the re-snapshot that followed.
* Anti-loop guard: at most one automatic re-snapshot per `resnapshot_interval_days`
  (default 7; set it above the CDC retention, which the reader cannot see, ADR 0017). A
  loss within that interval of the previous re-snapshot, or of a failed one (step 3, in any
  generation), raises `DataLossError` naming it: the stream cannot keep up, stops for
  longer than the retention over and over, or cannot read the table within it, and a
  person has to decide (a rerun with `resnapshot_interval_days=0` re-snapshots now).
  - Considered: one-shot (a second loss a year later would need a person for no reason);
    progress-based, re-snapshot only if the last generation committed a batch (a stream
    that cannot keep up does commit batches, and would loop forever); 24 hours (misses the
    stream that cannot keep up: it restarts at the snapshot's LSN with a full retention of
    headroom, so its next loss comes only after the retention period, never within a day).
    An interval longer than the retention catches the fastest recurrence: a stream that
    makes no progress, or that stops for longer than the retention again.
* No lock file. Two runs recovering one stream at once waste a snapshot: the event row is
  written once (same `txnVersion`), the state file holds one of the two LSNs (either is a
  valid start, and downstream rebuilds from the newest snapshot), and Spark fails one of
  two queries that write one checkpoint. A lock on a FUSE path has no reliable exclusive
  create, and one left by a crashed run blocks recovery until someone deletes it, the
  manual step this removes. Run one job per stream.

## Consequences
* After a `'resnapshot'` event, downstream rebuilds from the newest snapshot, then the
  changes after it: rows deleted during the gap have no delete row (ADR 0016). The newest
  snapshot's LSN is the highest `_start_lsn` of the `_operation = 0` rows or of the event
  rows' `max_lsn`, whichever is higher: an empty table's snapshot has only its event. A
  snapshot that failed step 3 stays in the target with no event; downstream rebuilds only
  on an event, and the next successful re-snapshot is newer.
* The change history between `lost_from_ts` and `lost_to_ts` is gone for good. The
  snapshot restores the current rows, not the intermediate versions; the facts record the
  gap.
* `finalized_until` stays monotonic: the new generation starts at the snapshot's LSN, after
  the last offset the old one processed, and `advance` is keyed by target, not `app_id`.
  Over the gap it still means that nothing more will arrive, not that the gap's changes are
  in the target.
* Statistics over micro-batches in the facts filter on `event IS NULL`; event rows carry the
  snapshot's `rows`.
* `on_data_loss="resnapshot"` needs a facts table and a checkpoint Python and Spark read
  alike (local, or a Volume); a URI checkpoint (`dbfs:/`, `abfss://`) or `/dbfs/...` is a
  `ValueError`. Earlier generations' checkpoint files stay in place, unread.
* The pre-flight reads Spark's offset log format `v1`; a new format needs a change here.
* ponytail: a fixed interval misses slow drift. A stream at 70% of the source's rate loses
  again only after retention / 0.3 (10 days at the default), so it re-snapshots every 10
  days; the event rows show the pattern. A progress-rate check if that shows up in practice.

## Amendment: chunked re-snapshots
With `to_delta(..., on_data_loss="resnapshot", snapshot="chunked")` (ADR 0028) the
recovery reads nothing from the table:

* The pre-flight, the anti-loop guard and `recovering` are unchanged. Instead of steps 2 to
  4, it opens a chunked snapshot at S = the snapshot LSN recorded now: a `'snapshot_open'`
  facts row with `mode` `'resnapshot'`, the generation and the gap (`lost_from_ts`,
  `lost_to_ts`), keyed `<app_id>#snapshots` and the generation (now `mode` `'chunked'` and
  `kind` `'resnapshot'`: Amendment 2). Then state `n + 1` with
  `snapshot_lsn` = S, and the generation's stream starts at S at once.
* A crash between the open and the state finds the open row of the next generation's
  `app_id` and reuses it: no second open.
* Step 3 disappears for it: the stream reads the changes from S while `backfill()` reads
  the chunks, so the snapshot's own LSN cannot be purged before the read ends. The
  `'resnapshot'` row comes when its last chunk is in, with min = max = S and the rows of all
  its chunks; downstream rebuilds from it as from a whole one.
* A loss while a chunked snapshot is still open opens a newer one in the next generation;
  `backfill()` reads the newest open snapshot, and the older one never completes.
* The default, `snapshot="full"`, keeps every step above (Amendment 2 adds its open row).

## Amendment 2: one snapshot mode per run
ADR 0028's Amendment locks the snapshot mode while a snapshot is open. For recoveries:

* The chunked open's detail has `mode` `'chunked'` and `kind` `'resnapshot'`; the first
  version, unreleased, put `'resnapshot'` in `mode`. A consumer of the facts filters on
  `get_json_object(detail, '$.kind') = 'resnapshot'`.
* A full re-snapshot writes a `'snapshot_open'` row (detail `mode` `'full'`) before step 2
  reads the table, one per attempt, with no `txnAppId`. Until its `'resnapshot'` row a
  chunked run of the stream raises, unless CDC cleanup has passed its S: then it can never
  complete, which is step 3's failure, and the rerun may switch to `snapshot="chunked"`
  (with `resnapshot_interval_days=0`, as after any failed attempt), which opens the same
  generation. The `DataLossError` of step 3 says so.
* A chunked recovery that finds a full open of the next generation still being read raises
  instead of starting from it; a complete one, an emptied table's, it starts from.

## Amendment 3: a skip leaves the gap in the facts
`failOnDataLoss=false` skipped purged changes with a WARNING only, and a log line does not
last: the facts showed an ordinary batch, so nobody could later tell when the loss happened,
on which capture instance or how much was lost.

* When the driver guard skips a range ahead to `min_lsn` M while planning, the reader leaves
  a `'data_skipped'` event file through the path that carries `'schema_change'` and
  `'capture_instance_switched'` (ADR 0023), and the sink writes it as an event row of the
  batch in the batch's own commit: `batch_id` the batch's, `rows` 0, `min_lsn` = `max_lsn` =
  M, `detail` `'<from>..<M>'`, `lost_from_ts` the commit time of the batch's start offset,
  `lost_to_ts` M's commit time, the same columns a `'resnapshot'` row fills. Written after
  the schema checks, so a batch they fail leaves none; it needs `metricsPath`, as the other
  events do. Facts migration 10 gives `event`, `lost_from_ts`, `lost_to_ts`, `detail`,
  `batch_id` and the table their new comments.
* The executor guard, which finds cleanup running mid-read, only logs: rows of the range
  may be missing, not certainly, and the task cannot tell which.
* `apply_changes` and `finalized_until` are unchanged: a skip leaves no snapshot to rebuild
  from, so silver keeps what the skipped changes would have changed until a new snapshot,
  and the verdict moves past the gap. A helper that reads the gaps waits for a consumer
  that asks.
  - Considered: holding silver's verdict at a `'data_skipped'` row. It would hold it until
    a snapshot nothing schedules, which `failOnDataLoss=false` was set to avoid.
