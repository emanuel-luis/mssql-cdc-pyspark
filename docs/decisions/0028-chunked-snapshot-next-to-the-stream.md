# 0028: Chunked snapshots read next to the running stream

**Status:** accepted  
**Date:** 2026-10-02T20:30:12-03:00  
**Amended:** 2026-10-03T14:30:38-03:00, chunks planned once from per-slice counts and fixed in a `'snapshot_plan'` row; a wave's facts rebuilt from bronze; one snapshot mode per run, locked while a snapshot is open; per-wave range deletes in silver, in re-snapshots and on datetime2 keys (see the Amendment)
**Amended:** 2026-10-03T18:10:05-03:00, a full snapshot opens once per run and stops holding the mode once CDC cleanup passes it; a wave rebuilt from the chunks bronze holds; a keyset plan's open last chunk closed when read (see the Amendment)
**Amended:** 2026-10-04T17:20:07-03:00, the plan counted and sought under the backfill's isolation, so `isolation="snapshot"` planning does not wait for writers' locks (see the Amendment)
**Amended:** 2026-10-06T21:14:15-03:00, `reconcile` takes a difference the change table explains, after what the stream read, as IN_FLIGHT too, and finds a NULL key's change (see Amendment 2)  
**Amended:** 2026-10-07T00:06:28-03:00, bronze's position is never a whole snapshot's S, which `snapshot_on_switch` takes above the stream's (see Amendment 2)  
**Amended:** 2026-10-08T13:47:54-03:00, `backfill()` reads the next wave while the one before commits, and a wave takes as many rounds of `numPartitions` chunks as `target_wave_seconds` holds; the plan's chunks unchanged (see Amendment 3)

## Context
A snapshot taken before the stream starts (ADR 0016) has to be read within the CDC
retention: the stream starts at its LSN, and whatever cleanup purges after that LSN while
the table is still being read is lost, which an automatic re-snapshot can only repeat
(ADR 0018). The read time is the table's size over the link: runs against a production
source moved 1.8 to 5.9 MB/s of source data over four connections, 18,000 to 105,000 rows
per second depending on the row width. At those rates a table of ten billion rows takes 1 to 6
days, against a default retention of 3 days, and longer for wider rows or a slower link
shared with the stream. Seeding from a copy (ADR 0025) needs a copy someone already has.
A long snapshot also shows no progress until its one Delta
commit at the end, and a failed task reads its whole range again.

What removes the retention limit is starting the stream **first**, at a recorded LSN S,
and keeping it running without a gap while the table is read in chunks next to it. Each
chunk then only needs a stamp L ≥ S recorded before its SELECT. That is the market's
pattern:

* Netflix's [DBLog](https://arxiv.org/abs/2010.12597) interleaves chunk reads with the log
  between low and high watermarks it writes to the source;
  [Debezium's incremental snapshots](https://debezium.io/blog/2021/10/07/incremental-snapshots/)
  ([DDD-3](https://github.com/debezium/debezium-design-documents/blob/main/DDD-3.md)) implement it.
* [Flink CDC's SQL Server connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/flink-sources/sqlserver-cdc/)
  reads its watermark without writing (the largest `start_lsn` of
  `cdc.lsn_time_mapping`), and with `scan.incremental.snapshot.backfill.skip` it leaves the
  changes made during a chunk to the change-log phase instead of merging them into the
  chunk: this design.
* [Airbyte's WASS](https://airbyte.com/blog/supporting-very-large-cdc-syncs-with-wass) reads
  the log between snapshot chunks so a long initial load does not outlive its retention;
  [AWS DMS](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Introduction.HighLevelView.html)
  caches the changes made during a full load and applies them after it; Oracle GoldenGate's
  [`HANDLECOLLISIONS`](https://docs.oracle.com/goldengate/c1230/gg-winux/GWURF/handlecollisions-nohandlecollisions.htm)
  lets the replicated changes overwrite the rows an initial load wrote; Databricks
  [AUTO CDC](https://docs.databricks.com/aws/en/data-engineering/what-is-cdc) orders changes
  by a `sequence_by` column and builds changes from snapshots.

DBLog and Debezium write watermarks and keep dedup windows because they emit one ordered
stream. Here bronze is a change log ordered by LSN downstream (silver's MERGE keeps the
newer image, ADR 0019), so a lower bound read from the source is enough, and nothing is
written to it (invariant 11 unchanged). The conditions the design rests on:

* **P1, no stale chunk:** a read stamped L sees every commit at or before L. L is
  `max_lsn` read before the SELECT; capture processes commits in order and only committed
  ones; READ COMMITTED and RCSI see what was committed before the statement started.
* **P2:** every stamp is at or after S (`max_lsn` only moves forward, S is read first).
* **P3:** bronze holds every change after S without a gap: the stream generation starts at S.
* **P4:** the chunks tile the key space up to MAX: complementary predicates
  (`client._key_select`), the first open below, the last ending just above MAX. Keys
  outside [MIN, MAX] read after S were inserted after S, so the stream has them.
* **P5:** removing a key always reaches bronze as a row: a delete (operation 1), a key
  update recorded as a delete and an insert (1 and 2), or a before-image (3) with the old key.

Rebuild rule: the rows of the snapshot S and the changes with `_start_lsn` above S; the
latest row per key by `(_start_lsn, _command_id, _seqval, _operation)`; a key whose latest
row is a delete is dropped, and a key in neither is absent. A chunk row can be newer than
its stamp (a commit after L the SELECT saw); that commit's change row comes after S and
ties or wins, so the overlap only replays. Three places took the snapshot's LSN as the
largest `_start_lsn` of operation-0 rows (`_last_snapshot`, `_recover`'s reuse check,
silver's rebuild point), which chunk stamps break.

## Decision
1. **Execution: `CdcStream.backfill()`, a batch call in waves, called again until done.**
   `to_delta(..., snapshot="chunked")` (with `bootstrap=True` or
   `on_data_loss="resnapshot"`) only opens a snapshot: it records S and the key's extent and
   starts the stream generation at S at once, reading nothing from the table; the first
   `backfill()` call plans the chunks (Amendment).
   `backfill(target, *, app_id, facts_table, chunk_rows=None, max_waves=None,
   max_seconds=None, min_headroom_hours=None, isolation=None)` reads the newest open
   snapshot in waves of `numPartitions` chunks (rounds of them, read ahead: Amendment 3), in
   its own task next to the stream task, and returns
   `{snapshot, chunks_done, chunks_total, done, paused, reason}`.
   - Considered: a second streaming query over the chunks. Its offsets would be chunk
     indices in a second checkpoint, with a query that ends; a batch call is the same reads
     with a bound (`max_waves`, `max_seconds`) and a retry the scheduler already gives.
   - Considered: a thread inside `to_delta`. It dies with the stream's driver, cannot be
     scheduled or bounded apart, and two writers in one process share one failure.
2. **State: facts event rows and two bronze columns; no new table.** Bronze gains
   `_snapshot` (S of the snapshot a row belongs to; NULL on change rows; on rows written
   before it, `_start_lsn` stands in) and `_chunk` (bronze migration 2). The facts carry
   `'snapshot_open'` (min = max = S, detail: mode, kind, keys, the key's extent, generation,
   the gap; a full snapshot writes one too since the Amendment), `'snapshot_plan'` with every
   chunk (Amendment), one `'snapshot_chunk'` row per chunk (rows, min = its stamp L, max =
   `max_lsn` after the read, detail: snapshot, chunk, wave, bounds, last) and, once the last
   chunk is in, the usual `'bootstrap'` or `'resnapshot'` row with min = max = S (facts
   migrations 7 and 8 rewrite the comments). Only those two remain snapshots (invariant 14).
   The chunked mode requires `facts_table`. A wave is one bronze append (`txnAppId
   <app_id>#snap.<S>`, `txnVersion` the wave, its chunks in `userMetadata`), then its facts
   rows (`<app_id>#snapchunks.<S>`, the wave): a crash between the two reruns the wave, Delta
   skips the append and the facts rows are rebuilt from the commit that holds it, or from
   the wave's rows in bronze once log cleanup has dropped that commit (Amendment). Every
   reader of "the snapshot" now uses `coalesce(_snapshot, _start_lsn)` and the completion
   events, never the largest operation-0 `_start_lsn`.
   - Considered: a state table of its own. Another kind to migrate, and a third table to
     keep consistent with bronze and the facts, which already record every other event.
   - Considered: state in the checkpoint directory. Local or Volume paths only, and
     downstream (silver) could not read it. Bronze `userMetadata` alone: log cleanup drops it.
3. **Silver: waves applied as they arrive, rebuild at completion by absence.**
   At the completion row silver is rebuilt from the rows of S and the changes after S, with
   `whenNotMatchedBySourceDelete`: absence from the snapshot deletes, whatever the key type.
   While a snapshot is open, a bootstrap or (since the Amendment) a re-snapshot, its waves
   are applied as their `'snapshot_chunk'` rows arrive (position `open_snapshot_lsn` and
   `snapshot_wave`, control migration 2), each chunk row with every bronze change of its key
   after S, so a chunk row never brings back a key the stream deleted after its stamp. With
   one integer, date or timestamp key that the chunks are cut on, each wave also deletes the
   silver keys of its chunks' ranges that the chunks lack (Amendment). Silver's verdict is
   held while a snapshot is open, and never advanced without `facts_table`, the only place a
   snapshot shows before its first wave.
   Operation 3 now deletes its own key, outranked by the 4 of the
   same key and commit: a key update SQL Server records as 3 and 4 no longer leaves the
   old key behind.
   - Considered: only the rebuild at completion. A weeks-long bootstrap would leave silver
     without most rows for weeks.
   - Considered at first and rejected, then taken by the Amendment for the keys Spark
     orders as SQL Server does: each chunk deleting the silver keys of its range it lacks. A
     silver table fresh at the open only gets keys from chunk rows and changes after S,
     whose removals reach bronze (P5); the rest are stale keys of a silver built before a
     lost history, which the rebuild removes for every key type while the verdict is held,
     but only at the completion, weeks away for a large re-snapshot. Spark also orders
     strings by bytes, SQL Server by the column's collation.
4. **Isolation: READ COMMITTED, optionally SNAPSHOT, never `NOLOCK`.** A chunk reads as a
   whole snapshot does. `isolation="snapshot"` (reader option `isolationLevel=snapshot`)
   prefixes `SET TRANSACTION ISOLATION LEVEL SNAPSHOT` where the DBA set
   `ALLOW_SNAPSHOT_ISOLATION`; SQL Server refuses it otherwise.
   - Considered: `NOLOCK`. A dirty read breaks P1 and the absence the deletes rely on.
   - Under locking READ COMMITTED a chunk waits for a writer's locks and delays writers
     behind it; SNAPSHOT and RCSI hold the version store instead. The chunk size bounds both.
5. **Validation: `reconcile()`.** Tier 1 counts rows and sums keys per bucket of one
   integer or date key on both sides (one scan on SQL Server, `COUNT_BIG` and `SUM`
   as `decimal(38,0)`); any other key gets a whole-table count. Tier
   2 reads every mismatched bucket and a sample of the rest through the snapshot reader and
   compares `sha2(to_json(struct(...)), 256)` computed by the same Spark function on both
   sides, classifying keys as AWS DMS validation does (MISSING_TARGET, MISSING_SOURCE,
   RECORD_DIFF), or IN_FLIGHT when bronze holds a change (no snapshot row) newer than
   silver's `applied_lsn` (`control_table`, not the newest stamp silver holds) or the source
   read: check again later (also when only the change table holds it yet: Amendment 2).
   Its ranges are cut on the key it compares (`snapshotKeys`), not always the unique index.
   With `facts_table`, the newest chunked snapshot's chunks are checked against bronze
   (CHUNK_TILING, CHUNK_ROWS, CHUNK_STAMP). The report is a new table kind, `reconcile`.
   - Considered: server-side `HASHBYTES` per bucket (a later tier, not now); never
     `CHECKSUM_AGG` or `BINARY_CHECKSUM`, which XOR and collide.

Planning, after S: the open records the key's extent (`client.snapshot_plan`) and the first
`backfill()` call plans every chunk (`client.plan_chunks`) into a `'snapshot_plan'` row that
no later call changes. One integer key gets chunks packed from row counts per slice of a
fixed grid; any other key gets keyset bounds, the key `chunk_rows` rows after the previous
bound (`key_bound`: a `TOP (n + 1)` per seekable piece of the range), walked up front below
the MAX recorded at the open. This supersedes arithmetic steps over [MIN, MAX] from a
`sys.sp_spaceused` estimate and keyset bounds found wave by wave (Amendment). The final
chunk ends at MAX + 1, or at the first key after MAX (open when there is none, or when it
reads back as MAX: Amendment): a table written while it is read does not pile the rows
inserted since S into the last chunk. Facts mark it `last`, which is what
completes the snapshot. Each wave is stamped with `snapshot_lsn()` before it is read and
fails if that is below S (a readable secondary). `min_headroom_hours` pauses `backfill()`
while the stream's newest facts row has less retention headroom, or there is none. A loss
while a chunked snapshot is open opens a newer one in the next generation, which abandons
the older. A chunked re-snapshot (ADR 0018) records its state and starts the generation at S
before anything is read, so the failure of a re-snapshot whose own LSN is purged before its
read ends no longer exists for it.

## Consequences
* A table no longer has to be read within the CDC retention, only while the stream runs
  without a gap. The chunks share the link with the stream: size a snapshot by its data
  size over the measured MB/s, and keep `min_headroom_hours` above the cleanup interval.
* Operation-0 rows are no exact history points: a chunk's image can be newer than change
  rows stamped after it. The rebuilt state is right; as-of queries over bronze must not
  read a chunk row as the state at its stamp.
* It breaks when the stream has a gap after S (data loss, `failOnDataLoss=false`, an older
  capture instance dropped early): the snapshot is abandoned and a newer one supersedes it.
  Also with a stamp below S (a manual `snapshotLsn`), `NOLOCK`, chunks read from a readable
  secondary with S from the primary, and a type change during a weeks-long snapshot, which
  fails the chunk's cast like the stream's (ADR 0023).
* State: bronze migration 2, facts migrations 7 and 8, control migration 2 and the
  `reconcile` kind; existing tables migrate when next opened, and the legacy snapshots read
  `coalesce(_snapshot, _start_lsn)`.
* `to_delta(bootstrap=True)` in either `snapshot` mode, and `snapshot()`, return the S of a
  chunked snapshot opened for the stream instead of reading the table again (with the facts
  table, a full run only once it is complete: the Amendment's lock); seeding refuses
  a target that holds one. A recovery that opened a chunked re-snapshot and stopped before
  writing its state reuses it, unless cleanup has passed it too: then it opens a newer one, a
  generation on.
* Tests: `tests/test_source_fake.py` and `tests/test_delta_sink.py` run the plans, the waves,
  a commit between a chunk's stamp and its read (the fake's `commit_before_read`), a crash
  between a wave's append and its facts rows, the throttle and a loss while a snapshot is
  open; `tests/test_silver.py` the waves and the rebuild;
  `tests/test_reconcile.py` the tiers and the chunk checks. `tests/integration`, on SQL
  Server 2022 with a least-privilege login: a chunked bootstrap next to a running stream and
  a writer making inserts, updates, deletes and key updates ends with silver equal to the
  table; keyset chunks on a composite key (case-insensitive leading values) and a varchar key
  under changes; a chunk that waits under READ COMMITTED (`LCK_M_S`) for a transaction
  holding locks in its range; SNAPSHOT isolation refused until allowed, then reading the
  committed rows without waiting; `sp_spaceused`, `key_max`, `key_bound` and a keyset plan
  with a table grant or column grants only; `key_bound` reading at most `n + 1` rows per
  piece; reconcile matching a quiet table and classifying injected differences; and, since
  the Amendment, the tests listed there. Lab check t10 runs it under a continuous writer and
  a held range, and `--resnapshot` through a purged gap.
* Not verified: the RCSI-versus-capture visibility window behind P1 (theoretical,
  microseconds; the tests run under locking READ COMMITTED and SNAPSHOT); key updates on SQL
  Server 2017 (t9 covers 2017 for switches only); readable secondaries; Databricks, the
  production link and weeks-long runs; a schema change during a backfill; the first wave
  creating bronze while the stream's first batch does (one retry on Delta's protocol or
  metadata conflict).
* ponytail: `backfill()` and `apply_changes` read the snapshot's facts rows on every call;
  filter by wave if that shows up.

## Amendment: a fixed plan, one mode per run, range deletes
The first version cut an integer key in equal steps over [MIN, MAX], so chunks were as
uneven as the key, and re-planned keyset chunks on every call, so a later `backfill()` with
another `chunk_rows` changed an open snapshot's chunks. A full and a chunked snapshot could
both open on one generation, and silver kept the keys a re-snapshot's purged gap deleted
until the completion, weeks away for a large table. Four changes, each superseding the
passage of the Decision that points here.

### Chunks planned once, sized by row counts
* The first `backfill()` call plans every chunk (`client.plan_chunks`) and records them in a
  `'snapshot_plan'` facts row (`txnAppId <app_id>#snapplan.<S>`, version 0; detail
  `{snapshot, kind, keys, chunk_rows, chunks}`). Later calls read it: their `chunk_rows` is
  ignored with a warning, and `chunks_total` is exact from the first call on. A rerun's plan
  row is skipped by Delta and the rerun reads the stored one; for two calls committing it at
  the same instant this rests on Delta's conflict check on the `txnAppId`, which fails the
  second (not tested here). The open still records only
  the key's extent (the `plan` of `'snapshot_open'`), so the stream starts at S without
  waiting for the planning.
* One integer key: its rows are counted per slice of a fixed grid over [MIN, MAX] in one
  server-side `GROUP BY` (`key_buckets`, about 16 slices per `chunk_rows`, at most 100,000 in
  one query); a slice holding more than `chunk_rows` is counted again on a finer grid over
  its own MIN..MAX (`key_range`), down to one value. Consecutive slices are then packed, in
  key order, into chunks of at most `chunk_rows`. The ranges stay fixed and the sizes follow
  the counts, not the key's spread: a dense cluster is split, a sparse region or a sentinel
  far above the ids joins its neighbours, and no chunk starts on an empty slice. When
  planned, every chunk but the last holds at most `chunk_rows` rows and more than
  `chunk_rows` less the slice after it (on an even key a slice is about a sixteenth of
  `chunk_rows`); rows written since then change that a little. The `sys.sp_spaceused`
  estimate only sizes the first grid.
* Other keys keep keyset bounds, `chunk_rows` rows apart when planned, now all found before
  the first wave. The drivers return a `datetime2(7)` key truncated to the microsecond (only
  the last key column may be one, `key_types`), so its bounds sit on a microsecond, and when
  the key after MAX shares MAX's microsecond its bound reads back as MAX's, at or below MAX:
  the last chunk is then open, or the keys of that microsecond would be in no chunk (found
  while amending, `tests/integration`).
* The end of a keyset plan's last chunk is the first key after MAX. When there is none at
  planning, `backfill()` seeks it again before each wave (`client.last_bound`), so the rows
  inserted above MAX while a long backfill runs, the stream's, do not pile up in the last
  chunk (a key that grows at the top: a `datetime2` creation time, a sequential
  `uniqueidentifier`). Every other bound stays as planned.
* Cost: planning reads the whole key once before the first wave (one `GROUP BY` over the
  narrowest index on the key plus the recounts, or one `TOP (n + 1)` seek per chunk), under
  the backfill's isolation: READ COMMITTED by default, where a writer holding locks delays
  it as it delays a chunk read. With `isolation="snapshot"` the counts, the seeks and
  `last_bound`'s search before each wave read under SNAPSHOT too, as the chunks do, and do
  not wait (the first version planned under READ COMMITTED whatever the isolation).
* Considered: equal steps over [MIN, MAX] (the first version). One dense cluster or a
  sentinel puts nearly every row in one chunk, and the switch to keyset bounds past 4
  values per row was a guess.
* Considered: planning wave by wave from the last bound. Cheaper up front, but the chunks
  of an open snapshot then depend on each call's arguments, and `chunks_total` is unknown.

### A wave's facts rebuilt from bronze
When a rerun's append is skipped and the commit that holds the wave is no longer in the
target's history (Delta log cleanup), its `'snapshot_chunk'` rows are rebuilt from its rows
in bronze: the chunks of `_snapshot` = S from the wave's first to the last that has rows
there, whatever the rerun planned (its `numPartitions` may differ from the attempt's), with
the bounds of the plan, the stamp and the counts of the rows, and `read_seconds` and
`read_mb` NULL. The attempt's empty chunks after those are read again by the next wave. A
rerun whose own read is empty looks for that commit too, and a skipped append whose rows
are nowhere raises. Before, `backfill()` raised and the snapshot had to be taken again,
though all its rows were in bronze. The history lookup stays the first try.

### One snapshot mode per run, locked while a snapshot is open
* A run (one `to_delta` call: its bootstrap and its re-snapshot) takes its snapshots in one
  `snapshot` mode. The mode may change from one run to the next, never while a snapshot of
  the other mode is open: a full read over a chunked snapshot still being backfilled, or a
  chunked open over an unfinished full one, would put two snapshots in one generation.
* A full snapshot now writes `'snapshot_open'` too, before it reads the table: one per run,
  at the run's own S and with no `txnAppId`; a chunked one stays one per generation
  (`<app_id>#snapshots`, the generation). Its detail `mode` is `'full'` or `'chunked'`, and
  `kind` holds `'bootstrap'` or `'resnapshot'` (the first version, unreleased, put the kind
  in `mode`). A snapshot is open until a `'bootstrap'` or `'resnapshot'` row of the same
  stream, any generation, has `max_lsn` at or after its S.
* A full one holds the mode only while a run may still be reading it: not once CDC cleanup
  has passed its S, as it can then never complete (a re-snapshot that outlived the
  retention, a run killed long ago), nor next to a chunked one of its generation. A chunked
  run then opens the generation over it. That is why a full run writes its own open: a
  rerun under the first attempt's S would count as dead while it reads.
* With a facts table, `to_delta` (with `bootstrap=True` or `on_data_loss="resnapshot"`),
  `snapshot(app_id=, facts_table=)` and `backfill()` raise on an open snapshot of the other
  mode, and `seed(app_id=, facts_table=)` on one of either mode. The error says how to
  finish it: `backfill()` until done for a chunked one, a rerun with `snapshot="full"`
  (which reads the table again) for a full one. A run that finds an open of the other mode
  committed after its own check stops too: a chunked run whose generation holds only a
  full open still being read, and a full run that reads back a chunked open of its
  generation after writing its own. Two chunked opens of one generation at the same
  instant rest on Delta's conflict check on the shared `txnAppId` (not tested here).
  `backfill()` checks once per call, first, so a full open made during a call stops the
  next one.
* Considered: no lock, the newest open wins. Silver's rebuild point and reconcile's chunk
  checks would then mix two snapshots.
* Considered: one mode for the stream's lifetime. A table that grows past what the
  retention allows needs to move to chunked without a new checkpoint.
* Considered: a row closing a full open when its run raises, so that a chunked run could
  follow at once. A killed process writes none, so the cleanup test is needed anyway; the
  row would only shorten the wait after an exception.
* Consequence: a full snapshot no run reads any more holds the mode until CDC cleanup passes
  its S, the retention at most: the library cannot tell it from one still being read. A
  full re-snapshot that outlived the retention is past that already, so its rerun may be
  chunked at once.

### Range deletes per wave, in re-snapshots and on datetime2 keys
* An open re-snapshot's waves are applied as a bootstrap's are: chunk rows with every later
  change of their keys, tracked by `open_snapshot_lsn` and `snapshot_wave`, the verdict held
  until the completion. Before, it applied changes only and waited for the rebuild.
* Each wave also deletes, at each chunk's stamp L, the silver keys of its range [lo, hi)
  that the chunk does not hold and whose image is older than L: the chunk saw every commit
  up to L, so those keys were gone by then. They are ranked as delete rows stamped L, so a
  later change of the key still wins. The keys a re-snapshot's purged gap deleted leave
  silver wave by wave instead of at the completion.
* Only when silver's key is the snapshot's own (the open row's `keys`: the bounds are cut on
  it) and is one column of an integer, date or timestamp type (`datetime2`, `datetime`,
  `smalldatetime`), which Spark orders as SQL Server does. Other keys (strings, whose
  collation Spark ignores, composite keys, other types) keep leaving stale keys to the
  rebuild at the completion.
* Integer bounds are read as BIGINT; the plan's last bound, MAX + 1, counts as open when it
  is past BIGINT's range.
* A `datetime2(7)` key holds 100 ns, Spark only microseconds (the drivers truncate). The
  plan's bounds are truncated the same way, so each key falls on the same side of them in
  Spark as on SQL Server. A bound with digits below the microsecond would not place the keys
  of its microsecond: a lower one moves up one microsecond and an upper one is truncated, so
  no chunk deletes those keys and the rebuild does, never the wrong chunk.

### Tests
`tests/test_source_fake.py`: an integer plan packing counted slices whatever the skew.
`tests/test_delta_sink.py`: the plan kept across calls and a wave's facts rebuilt from
bronze without its commit, a rerun planning more chunks than the commit holds; a rerun
reading nothing finding the commit; a keyset plan's open last chunk closed at the first key
after MAX when read; a full snapshot a crash left open holding its mode until a full run
completes it; a full re-snapshot past the retention giving its generation to a chunked
rerun; runs finding the other mode opened after their check. `tests/test_silver.py`: each
wave deleting the stale keys of its ranges, a chunked re-snapshot deleting the gap's keys
wave by wave, an integer plan's last bound past BIGINT, no range deletes on a composite key
or on a key the bounds were not cut on. `tests/test_client_sql.py`: the
counting and seeking SQL. `tests/integration`: an integer plan counted on the server for a
skewed BIGINT key (a dense cluster, a sparse region, a key at BIGINT's maximum) and
backfilled; a `datetime2(7)` plan reading the keys in MAX's microsecond; a chunked
re-snapshot deleting stale `datetime2(7)` keys wave by wave but not one in a bound's
microsecond; integer and keyset plans under SNAPSHOT isolation not waiting for the locks a
writer holds, which a READ COMMITTED plan waits on. Lab check t10 passes in both modes with
the plan (LAB.md).

## Amendment 2: in flight up to what the change table holds
`reconcile` took a difference as IN_FLIGHT only when bronze held a change to it. With the
stream behind the source, every key changed in between came out as MISMATCH,
MISSING_TARGET, MISSING_SOURCE or RECORD_DIFF: stream lag reported as an integrity failure.
And the join that finds a key's changes compared keys with `=`, so a change of a NULL key
never made its difference IN_FLIGHT. CDC refuses a unique index over nullable columns, but
`keys` may name other columns.

* Once the source is read (the counts, then the rows of the buckets compared), bronze is
  pinned and `sys.fn_cdc_get_max_lsn()` read. The change table's changes after bronze's
  position (its newest change, or the S of a chunked snapshot, which the stream reads from)
  up to that LSN are what the stream has not read yet; their keys count with bronze's
  changes after the older of E and M. They are read through each capture instance's piece
  of the range, as the stream reads them (ADR 0023), with `iter_changes` on the key columns:
  the change table the stream already reads (invariant 11), no new grant and no new T-SQL.
* Not a whole snapshot's S: `to_delta(snapshot_on_switch=True)` takes one at `max_lsn` after
  the batch that read the switch, while the stream's checkpoint stays at the batch's end, so
  a commit in between would be in neither bronze's changes nor the read. Reading from the
  newest change instead only finds more IN_FLIGHT, and costs more only while silver has not
  applied a bootstrap or a re-snapshot.
* Still unseen: a commit the capture job has not harvested yet, in no change table (its lag,
  seconds). A MISMATCH that causes clears on the next run.
* That join is null-safe (`<=>`), as reconcile's and silver's other key joins already were,
  and the report's `key` writes a NULL key column as `null` instead of leaving it out (`{}`).
* Cost: every change row of the stream's lag crosses to the driver, keys only. A stream far
  behind makes that large, and most buckets IN_FLIGHT, which they are.
* Considered: the change table from silver's `applied_lsn` E, without bronze. One read, but
  of every change silver has not applied (hours of them for an hourly `apply_changes`), and
  blind to the ones cleanup purged that bronze still holds.
* Considered: a `SELECT DISTINCT` of the keys on the server, a new client method. Less to
  transfer, but new T-SQL on every client for a window that is a stream's lag; added if a
  stream far behind shows up.
* The report's column comments on `status` and `failure_type` still name bronze alone: a
  comment migration of the `reconcile` kind waits for a minor release.

### Tests
`tests/test_reconcile.py`: an insert, a delete and an update only the change table holds
IN_FLIGHT, then with bronze holding part of the lag, a real difference next to them still
RECORD_DIFF, and a commit after M that the count sees; the change table read per capture
instance from bronze's position up to the LSN read after the source (pure, on the fake); an
insert below a whole snapshot appended above the stream's position IN_FLIGHT; a
NULL key's change in bronze IN_FLIGHT, a real difference of it RECORD_DIFF, both named
`null`. `tests/integration`: the same lag on SQL Server 2022, read by the stream's
least-privilege login.

## Amendment 3: waves read ahead, sized toward a duration
Against a production source (DBR 18.2, `numPartitions` 4, `chunk_rows` 1,000,000), a chunked
snapshot of 34 million rows took 943 s where a full one took 325 s, and one of 100 million rows
5,096 s where a full one took 3,364 s: the waves read 58,000 and 24,500 rows/s, the full
snapshots 105,000 and 30,000. Per wave of 4 chunks, read in 46 to 78 s and in 98 to 197 s,
15 to 25 s went from its slowest chunk's read to the end of its append (the Spark jobs, the
cache, the commit, the history read back), 6 to 8 s more before the next wave's read (its
facts rows, the throttle, the stamp), and its slowest chunk took 12 to 15 s longer than the
mean of the four, which the other three connections spent idle. The chunks stay as planned
(the Amendment's plan): what changes is how a wave is read and how many of them it takes.

* **Read ahead.** Once a wave is read (cached and counted, its metrics folded into its
  tag), its append and then its facts rows go to one background thread, and the next wave
  is stamped and read meanwhile. That wave's commit is handed over only once the one before
  has finished: at most one wave reads while one commits, bronze takes the waves in order
  (`txnVersion` the wave, as before), a wave's facts rows still follow its commit, and its
  stamp is still recorded before its read. A crash still leaves at most the last committed
  wave without its facts rows, which a rerun rebuilds as before. The read ahead assumes the
  wave committing holds the chunks it read; when its commit holds others (a rerun's first
  wave, which Delta skips for an earlier attempt's of another width, or a concurrent call's),
  the wave read ahead is dropped and read again after the commit's last chunk. A failure in
  either thread fails the call once the other has finished: a commit under way completes.
  The commit thread inherits the caller's job group and scheduler pool
  (`inheritable_thread_target`), and looks up commit times on a connection of its own, one
  more per call.
* **Waves of rounds, sized toward `target_wave_seconds`.** A wave takes whole rounds of
  `numPartitions` chunks; its one partition per chunk is coalesced into `numPartitions`
  partitions, partition p reading chunks p, p + `numPartitions`... one after the other, so
  the connections stay `numPartitions` and neighbouring chunks are still read side by side.
  The rounds are `target_wave_seconds` (300 when None) over the pace of the last wave: its
  read, from its stamp to its rows cached, per round; for a call's first wave the facts
  rows of the last wave committed (`duration_ms`, its read and its append); one round
  without either. No more rounds than the rest of `max_seconds` holds, and at least one;
  `target_wave_seconds=0` reads one round per wave, as before.
* The default: production waves of 4 chunks read in 1 to 3 minutes, so 300 s means about 4
  rounds on the 34-million-row table and 2 on the 100-million-row one, 3 waves instead of 9
  and about 14 instead of 27, each still a few minutes of work lost to a crash.
* Cost: a wave's rows stay cached until its commit, two waves at once while one reads
  ahead (`MEMORY_AND_DISK`): about `numPartitions` × `target_wave_seconds` × a connection's
  rate, 2 to 4 GB of wire bytes on that source at 300 s. `max_seconds` now also bounds a
  wave's size; a call may still pass it by up to a wave and the commit under way.
* Considered: adapting `chunk_rows`. Ruled out: the plan's chunks never change while the
  snapshot is open, and a rerun or `reconcile` reads them back as planned.
* Considered: one partition per chunk, scheduled by Spark. A cluster with more cores than
  `numPartitions` would open as many connections as a wave has chunks.
* Considered: the FAIR scheduler pool for the commit. Pools do not preempt running tasks:
  with fewer partitions than cores the append finds free cores anyway, and with as many
  the read's tasks hold them in either mode. The thread keeps the caller's pool.
* Considered: reading two waves ahead. More rows cached for no more overlap: a commit is
  shorter than a read.
* Considered: capping a wave's growth. The target bounds its duration already; a first wave
  much faster than the rest (a warm buffer pool) makes the next one longer, which the one
  after corrects.
* Not yet measured against SQL Server: on the fake almost all of a wave is its fixed cost, so
  a local run shows that cost going, not the production gain. The Databricks benchmark
  against the production source runs after the release candidate.

### Tests
`tests/test_delta_sink.py`: a wave of four chunks read two at a time, one file per partition;
waves of one round, then of the rounds `target_wave_seconds` holds at an earlier call's pace
from its facts rows, then of what is left of `max_seconds`; the next wave stamped while the
wave before still writes its facts rows, and that commit completing when the next wave's
read fails; a wave read ahead of a rerun's commit that holds fewer chunks than it read,
dropped and read again from the commit's last chunk.
