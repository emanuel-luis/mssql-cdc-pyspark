# 0028: Chunked snapshots read next to the running stream

**Status:** accepted  
**Date:** 2026-10-02T20:30:12-03:00

## Context
A snapshot taken before the stream starts (ADR 0016) has to be read within the CDC
retention: the stream starts at its LSN, and whatever cleanup purges after that LSN while
the table is still being read is lost, which an automatic re-snapshot can only repeat
(ADR 0018). The read time is the table's size over the link: runs against a production
source moved 1.8 to 5.9 MB/s of source data over four connections, 18,000 to 105,000 rows
per second depending on the row width. At those rates a table of 13.7 billion rows takes 1.5
to 9 days, against a default retention of 3 days, and longer for wider rows or a slower link
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
   `on_data_loss="resnapshot"`) only opens a snapshot: it records S, plans the chunks and
   starts the stream generation at S at once, reading nothing from the table.
   `backfill(target, *, app_id, facts_table, chunk_rows=1_000_000, max_waves=None,
   max_seconds=None, min_headroom_hours=None, isolation=None)` reads the newest open
   snapshot in waves of `numPartitions` chunks, in its own task next to the stream task, and
   returns `{snapshot, chunks_done, chunks_total, done, paused, reason}`.
   - Considered: a second streaming query over the chunks. Its offsets would be chunk
     indices in a second checkpoint, with a query that ends; a batch call is the same reads
     with a bound (`max_waves`, `max_seconds`) and a retry the scheduler already gives.
   - Considered: a thread inside `to_delta`. It dies with the stream's driver, cannot be
     scheduled or bounded apart, and two writers in one process share one failure.
2. **State: facts event rows and two bronze columns; no new table.** Bronze gains
   `_snapshot` (S of the snapshot a row belongs to; NULL on change rows; on rows written
   before it, `_start_lsn` stands in) and `_chunk` (bronze migration 2). The facts carry
   `'snapshot_open'` (min = max = S, detail: mode, keys, plan, generation, the gap),
   one `'snapshot_chunk'` row per chunk (rows, min = its stamp L, max = `max_lsn` after the
   read, detail: snapshot, chunk, wave, bounds, last) and, once the last chunk is in, the usual
   `'bootstrap'` or `'resnapshot'` row with min = max = S (facts migration 7 rewrites the
   comments). Only those two remain snapshots (invariant 14). The chunked mode requires
   `facts_table`. A wave is one bronze append (`txnAppId <app_id>#snap.<S>`, `txnVersion`
   the wave, its chunks in `userMetadata`), then its facts rows
   (`<app_id>#snapchunks.<S>`, the wave): a crash between the two reruns the wave, Delta
   skips the append and the facts rows are rebuilt from the commit that holds it. Every
   reader of "the snapshot" now uses `coalesce(_snapshot, _start_lsn)` and the completion
   events, never the largest operation-0 `_start_lsn`.
   - Considered: a state table of its own. Another kind to migrate, and a third table to
     keep consistent with bronze and the facts, which already record every other event.
   - Considered: state in the checkpoint directory. Local or Volume paths only, and
     downstream (silver) could not read it. Bronze `userMetadata` alone: log cleanup drops it.
3. **Silver: waves applied as they arrive, rebuild at completion by absence.**
   At the completion row silver is rebuilt from the rows of S and the changes after S, with
   `whenNotMatchedBySourceDelete`: absence from the snapshot deletes, whatever the key type.
   While a bootstrap is open, its waves are applied as their `'snapshot_chunk'` rows arrive
   (position `open_snapshot_lsn` and `snapshot_wave`, control migration 2), each chunk row
   with every bronze change of its key after S, so a chunk row never brings back a key the
   stream deleted after its stamp. An open re-snapshot keeps applying changes and rebuilds
   at completion; silver's verdict is held while a snapshot is open, and never advanced
   without `facts_table`, the only place a snapshot shows before its first wave.
   Operation 3 now deletes its own key, outranked by the 4 of the
   same key and commit: a key update SQL Server records as 3 and 4 no longer leaves the
   old key behind.
   - Considered: only the rebuild at completion. A weeks-long bootstrap would leave silver
     without most rows for weeks.
   - Considered: each chunk deleting the silver keys of its range it lacks. A silver table
     fresh at the open only gets keys from chunk rows and changes after S, whose removals
     reach bronze (P5); the rest are stale keys of a silver built before a lost history,
     which the rebuild removes for every key type while the verdict is held. Spark also
     orders strings by bytes, SQL Server by the column's collation.
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
   read: check again later.
   Its ranges are cut on the key it compares (`snapshotKeys`), not always the unique index.
   With `facts_table`, the newest chunked snapshot's chunks are checked against bronze
   (CHUNK_TILING, CHUNK_ROWS, CHUNK_STAMP). The report is a new table kind, `reconcile`.
   - Considered: server-side `HASHBYTES` per bucket (a later tier, not now); never
     `CHECKSUM_AGG` or `BINARY_CHECKSUM`, which XOR and collide.

Planning, in `client.snapshot_plan` and `client.next_chunks`, after S: one integer key gets
arithmetic chunks over [MIN, MAX] with a step from a row estimate (`sys.sp_spaceused`,
public), while it spans at most 4 values per row; a sparser one (a sentinel far above the ids
would put every row in the first step) and any other key get keyset bounds found per wave,
the key `chunk_rows` rows after the previous bound (`key_bound`: a `TOP (n + 1)` per seekable
piece of the range), below the MAX recorded at the open. The final chunk ends at MAX + 1, or
at the first key after MAX (open when there is none): a table written while it is read does
not pile the rows inserted since S into the last chunk. Facts mark it `last`, which is what
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
* State: bronze migration 2, facts migration 7, control migration 2 and the `reconcile`
  kind; existing tables migrate when next opened, and the legacy snapshots read
  `coalesce(_snapshot, _start_lsn)`.
* `to_delta(bootstrap=True)` in either `snapshot` mode, and `snapshot()`, return the S of a
  chunked snapshot opened for the stream instead of reading the table again; seeding refuses
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
  committed rows without waiting; `sp_spaceused`, `key_max` and `key_bound` with a table
  grant or column grants only; `key_bound` reading at most `n + 1` rows per piece; reconcile
  matching a quiet table and classifying injected differences. Lab check t10 runs it under a
  continuous writer and a held range, and `--resnapshot` through a purged gap.
* Not verified: the RCSI-versus-capture visibility window behind P1 (theoretical,
  microseconds; the tests run under locking READ COMMITTED and SNAPSHOT); key updates on SQL
  Server 2017 (t9 covers 2017 for switches only); readable secondaries; Databricks, the
  production link and weeks-long runs; a schema change during a backfill; the first wave
  creating bronze while the stream's first batch does (one retry on Delta's protocol or
  metadata conflict).
* ponytail: `backfill()` and `apply_changes` read the snapshot's facts rows on every call;
  filter by wave if that shows up.
