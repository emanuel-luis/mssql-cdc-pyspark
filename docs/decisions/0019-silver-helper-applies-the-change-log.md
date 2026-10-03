# 0019: A silver helper applies the bronze change log to a current-state table

**Status:** accepted  
**Date:** 2026-09-30T11:07:22-03:00  
**Amended:** 2026-10-02T20:30:12-03:00, chunked snapshots, snapshots named by `_snapshot` and completion events, operation 3 deletes its own key (see the Amendment, ADR 0028)  
**Amended:** 2026-10-03T14:30:38-03:00, an open re-snapshot's waves applied, and per-wave range deletes on one integer, date or timestamp key (see Amendment 2, ADR 0028)

## Context
Bronze is an append-only change log: one row per change, updates as two rows, the snapshot
overlapping the stream (ADR 0016), and after a re-snapshot no delete rows for the purged gap
(ADR 0018). Every consumer that wants the source table's current state wrote its own MERGE,
and each had to get the same details right: the ordering key, before-images, NULL keys,
reruns after a crash, and the rebuild after a re-snapshot, which is easy to miss because
nothing fails when it is skipped.

## Decision
* `apply_changes(spark, bronze, target, capture_instance, keys, control_table=...,
  facts_table=None, options=None, granularity="hour")` in `mssql_cdc.silver`, exported from
  the package. A batch call, run after the stream (same job or another), one job per silver
  table. It reads the capture instance's bronze rows beyond its position, keeps the latest
  image per key by `(_start_lsn, _command_id, _seqval, _operation)`, and applies it with one
  `DeltaTable.merge` (ADR 0012): operation 3 (before-image) is ignored, 1 deletes, 0
  (snapshot), 2 and 4 upsert. Keys match with `<=>`, since a unique index admits one NULL.
  - Considered: a stream over bronze with `foreachBatch`. Its position would be a second
    checkpoint, a large snapshot can span micro-batches (a rebuild needs all of it), an
    emptied table's re-snapshot writes no bronze rows to trigger a batch, and the verdict
    could only be capped by the rows of the batch, which stalls it on quiet tables.
  - Ignoring operation 3 is safe for key changes: SQL Server records an update of the
    primary key as a delete of the old key and an insert of the new one (`tests/integration`).
* Silver holds the captured columns plus `_start_lsn` and `_commit_ts` of each row's
  current image, created typed and commented through the migrations of a new `silver` kind
  (ADR 0013). Deletes are hard: the table is the source's current state and needs no filter;
  bronze keeps the history, and Delta's change data feed can expose silver's deletes to
  incremental consumers.
  - Considered: soft deletes. Tombstones grow without bound on queue-like tables and every
    reader has to filter them.
* Position: a new control-table column `applied_lsn` (control migration 1), the highest
  `_start_lsn` applied (or the snapshot rebuilt from, when it had no rows), written after the
  MERGE. A crash in between leaves it behind, and applying again from behind is idempotent:
  each key takes the latest image of a range that always reaches the head of bronze, so a
  deleted key's latest image is still its delete, and a row only takes an image newer than
  its own `_start_lsn`.
  - Considered: the silver table itself (the highest `_start_lsn` of its rows). Hard deletes
    lose it: a table whose newest changes are deletes, or one left empty, would re-read (or
    rebuild from a snapshot months old) on every call. Table history (`userMetadata` of the
    MERGE commit): atomic with the data, but `commitInfo` does not survive log cleanup
    (ADR 0004), and a MERGE can only get it from the session conf, which parallel threads
    share. The control table is durable, one row per table, and already holds silver's
    verdict: one query shows how far bronze and silver are.
  - `advance` now also moves a verdict that is NULL, on a row `apply_changes` created before
    bronze had one.
* Re-snapshot: the newest snapshot is the highest `_start_lsn` of the capture instance's
  operation-0 rows or `max_lsn` of the facts' event rows whose `target` is `bronze`,
  whichever is higher (ADR 0018). When it is newer than `snapshot_lsn`, a second control
  column (same migration) holding the snapshot silver was last rebuilt from, or there is no
  position yet, silver is rebuilt: the latest image per key of the rows from that snapshot
  on, and `whenNotMatchedBySourceDelete` removes every other key. `facts_table` is optional:
  the operation-0 rows mark every snapshot with rows; only an emptied table's re-snapshot
  needs its event.
  - Not against `applied_lsn`: a bootstrap added to an existing checkpoint on a quiet
    database is stamped with `max_lsn`, which can be exactly the `applied_lsn` of the changes
    silver already has, and its rows would never be applied.
  - The events carry no capture instance, so a bronze table holds one capture instance, as
    its verdict already requires (one row per table in the control table).
* Consistency: the bronze verdict and the facts are read first, then bronze at one pinned
  version (`VERSION AS OF`) for the snapshot check, the range and the MERGE. Bronze commits
  its rows before its verdict (ADR 0005) and a snapshot's rows before its event (ADR 0018),
  so the pinned version holds what both point to; a re-snapshot committed meanwhile is seen
  on the next call, never half-applied.
* Verdict: `finalization.advance(control, target, <bronze's end_lsn and end_commit_ts>)`
  after the MERGE and the position. That is the cap: what silver claims is the bronze verdict
  as it stood before the rows were read, all of which are applied. Capping at the commit
  time of the last applied row instead would stall the verdict on quiet tables, where it
  moves on idle entries that bring no rows.

## Consequences
* Consumers read silver and gate on `finalization.is_final(spark, control, target, end)`,
  like bronze. The bronze verdict must be advanced under the same name or path passed as
  `bronze`, and the facts `target` must match it too.
* The control table gains `applied_lsn` and `snapshot_lsn` (NULL for other tables) through
  its first migration; silver needs a control table even when nobody reads its verdict.
* Until the stream has written bronze (its first non-empty batch, or the bootstrap), a call
  does nothing and silver does not exist yet.
* Silver's schema is bronze's at creation; a new captured column needs a new capture
  instance anyway (roadmap: schema changes).
* The MERGE joins against the whole silver table, and the rebuild check scans bronze for
  operation 0 (file statistics skip the change-only files). ponytail: keep the newest
  snapshot's LSN in the control table if that scan shows up.
* One call per silver table at a time, like one job per stream (ADR 0018).
* `tests/test_silver.py` covers the ordering, reruns, a position left behind, the snapshot,
  the rebuild after a re-snapshot (with and without rows) or a bootstrap stamped at
  `applied_lsn`, and the verdict with a bronze batch committed during the call;
  `tests/integration` checks a composite key read from SQL Server and a key update.

## Amendment: chunked snapshots
Chunk rows (ADR 0028) are stamped per chunk at or above S and arrive in waves after the
snapshot's `'snapshot_open'` row, so the rebuild point and the apply change:

* The newest snapshot is the newest `max_lsn` of the facts' `'bootstrap'` and
  `'resnapshot'` rows, or the newest `coalesce(_snapshot, _start_lsn)` of whole snapshots'
  operation-0 rows (`_chunk` NULL). A rebuild applies the operation-0 rows of that snapshot
  (`coalesce(_snapshot, _start_lsn)` = S, whatever their stamps) and the changes after S,
  with `whenNotMatchedBySourceDelete` as before.
* `facts_table` is required once bronze holds chunk rows: the facts say which chunks are in
  and when the snapshot is complete.
* While a bootstrap is open (a chunked `'snapshot_open'` newer than every complete
  snapshot, its `kind` `'bootstrap'`), each call applies the waves its `'snapshot_chunk'` rows
  announced since the last call, tracked by `open_snapshot_lsn` and `snapshot_wave` (control
  migration 2): the chunks land below `applied_lsn`, so the position alone cannot track them.
  Each chunk's rows are ranked with every bronze change of their keys after S: a chunk row
  never outranks a delete the stream committed after its stamp. Stale keys wait for the
  rebuild (superseded for some keys by Amendment 2).
* An open re-snapshot keeps applying changes; the rebuild comes at its completion row
  (superseded by Amendment 2: its waves are applied too).
* Silver's verdict is held while a snapshot is open: silver lacks keys or holds stale ones.
  Without `facts_table` it is never advanced: a chunked snapshot shows in the facts alone
  until its first wave, and so does an emptied table's re-snapshot.
* Operation 3 now deletes its own key; the 4 of the same key and commit outranks it
  (`_operation` descending), so only a 3 whose update moved the row to another key is the
  latest row. SQL Server records a primary-key update as 1 and 2 (`tests/integration`), but
  a key update recorded as 3 and 4 would have left the old key in silver forever.

## Amendment 2: range deletes per wave
The first amendment left a silver table's stale keys to the rebuild at a chunked
snapshot's completion, and applied only the changes while a re-snapshot was open. For a
large re-snapshot that leaves the keys deleted in the purged gap in silver for weeks. Now
([ADR 0028](0028-chunked-snapshot-next-to-the-stream.md)'s Amendment):

* An open re-snapshot's waves are applied as a bootstrap's are, tracked by
  `open_snapshot_lsn` and `snapshot_wave`; the verdict stays held until its completion row.
* Each wave deletes, at each chunk's stamp L, the silver keys of the chunk's range [lo, hi)
  that the chunk does not hold and whose `_start_lsn` is below L. They join the ranking as
  operation-1 rows stamped L, so a later change of the key outranks them, and a key whose
  image is already newer than L is not touched.
* Only when silver's key is the snapshot's own (the `keys` of its `'snapshot_open'` row) and
  one column of an integer, date or timestamp type (`datetime2`, `datetime`,
  `smalldatetime`): Spark compares those as SQL Server does. Strings (byte order against the
  column's collation), composite keys and other types leave stale keys to the rebuild, as
  before. Integer bounds are compared as BIGINT, and the plan's last bound (MAX + 1) is open
  when it does not fit.
* The microsecond rule: SQL Server keeps `datetime2(7)` to 100 ns, Spark to the microsecond
  (the drivers truncate). The plan's bounds are truncated too, so they sit on a microsecond
  and Spark places every key as SQL Server does. A bound with digits below the microsecond
  would not: a lower one moves up one microsecond and an upper one is truncated, so the keys
  in its microsecond are deleted by no chunk and left to the rebuild.
* The rebuild at completion is unchanged: it still removes every key absent from the
  snapshot and the changes after it, whatever the key type.
