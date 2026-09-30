# 0019: A silver helper applies the bronze change log to a current-state table

**Status:** accepted  
**Date:** 2026-09-30T11:07:22-03:00

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
  whichever is higher (ADR 0018). When it is above `applied_lsn` (or there is no position
  yet), silver is rebuilt: the latest image per key of the rows from that snapshot on, and
  `whenNotMatchedBySourceDelete` removes every other key. `facts_table` is optional: the
  operation-0 rows mark every snapshot with rows; only an emptied table's re-snapshot needs
  its event.
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
* The control table gains `applied_lsn` (NULL for other tables) through its first migration;
  silver needs a control table even when nobody reads its verdict.
* Silver's schema is bronze's at creation; a new captured column needs a new capture
  instance anyway (roadmap: schema changes).
* The MERGE joins against the whole silver table, and the rebuild check scans bronze for
  operation 0 (file statistics skip the change-only files). ponytail: keep the newest
  snapshot's LSN in the control table if that scan shows up.
* One call per silver table at a time, like one job per stream (ADR 0018).
* `tests/test_silver.py` covers the ordering, reruns, a position left behind, the snapshot,
  the rebuild after a re-snapshot (with and without rows) and the verdict;
  `tests/integration` checks a composite key read from SQL Server and a key update.
