# Design notes

## The problem: presence is not completeness

In batch ingestion, "the job finished and the partition has data" meant "the
partition is complete". With CDC, a commit can be processed after its hour has
passed, so a downstream job reading the 14:00 hour at 15:05 may read it
incomplete. Consumers need an explicit signal.

Pinterest's partition finalization (Flink + Iceberg) records event-time statistics
per commit and derives a monotonic watermark from the minimum event time seen over
recent commits. It is a heuristic: a minimum over records you *saw* cannot account
for records you have not seen (a stalled upstream partition), and it cannot advance
on an idle table.

## Why SQL Server CDC gives a stronger signal

From Microsoft's documentation:

* "Entries are added to the change table in the same order that they were
  committed to the source table."
* "the capture process opens and commits its own transaction on each scan cycle"
  to ensure "a transactionally consistent boundary across all the change data
  capture change tables".
* `sys.fn_cdc_get_max_lsn()` "is the last LSN processed by the capture process".
* "In periods of inactivity, a dummy entry is added to the table
  cdc.lsn_time_mapping to mark the fact that the capture process has processed the
  changes up to a given commit time."

Together (an inference, not a single documented sentence): every transaction
committed at or before the commit time of `max_lsn` is already in the change
tables, and `max_lsn` keeps advancing while the database is idle. The source hands
you a low watermark on commit time. `lab/checks/t1` and `t4` test exactly this.

How often the idle entries come is not documented. On SQL Server 2022 (CU27) t1
measures one about every 5 minutes; writes to tables without CDC and `CHECKPOINT` do
not add any. So on a quiet database `finalized_until` lags by up to ~5 minutes, which is
fine for hourly periods. For a tighter bound, `sql/heartbeat.sql` updates a one-row
CDC-tracked table every 10 seconds from a SQL Server Agent job; every update is a
captured commit, so `max_lsn` moves at that pace (ADR 0010). The reader itself never
writes to the source.

## Offsets

`{"lsn": "0x<20 hex>", "commit_ts": "<UTC ISO-8601>"}`

* The LSN is kept as a fixed-width uppercase hex string: JSON-serializable, and
  string order equals LSN order. The LSN space is database-wide.
* A batch reads `[fn_cdc_increment_lsn(start), end]` with `end <= max_lsn`, so
  every batch is a prefix of the commit history and ends on a commit boundary.
* `commit_ts` is the commit time of `end`. `end` may be a dummy entry with no change
  rows, so the batch's rows cannot tell how far capture progressed; the offset can.

## Facts and verdict

* **Facts** per micro-batch: row counts per operation, LSN and commit-time ranges.
  Written into the Delta commit (`userMetadata`) and into a facts table, because
  Delta checkpoints do not preserve `commitInfo`, which is lost with log cleanup.
* **Verdict** per table: `finalized_until = truncate(end.commit_ts, granularity)`.
  Every period strictly before it is complete. Stored in a small control table,
  never in `TBLPROPERTIES`: on Databricks a table-property change is a metadata
  commit that can fail concurrent writers and streaming readers.
* **Ordering over atomicity**: data first, verdict after, verdict monotonic
  (`GREATEST`). If the job dies in between, the verdict lags; it cannot run ahead.

## Guards

* **Retention**: CDC cleanup deletes by time, even when consumers are behind. If
  the next `from_lsn` is below `sys.fn_cdc_get_min_lsn(ci)`, the source raises
  (`failOnDataLoss=true`) and a re-snapshot is needed. The reader reads the change
  table directly (ADR 0009), which returns a purged range as empty rather than failing,
  so every task checks `min_lsn` again after its read: cleanup moves the watermark
  before it deletes, so a purge during the read cannot go unnoticed.
* **Empty ranges**: `partitions()` plans nothing when `end <= start`.
* **Idempotency**: Delta `txnAppId`/`txnVersion` keyed by batch id, so a replayed
  micro-batch is skipped.

## What this is not

* Not a snapshot tool: the initial load is out of scope for v0.1 (record
  `max_lsn`, snapshot, then start the stream at that LSN; an idempotent MERGE
  downstream absorbs the overlap).
* Not business-time completeness: a late *business* event (a back-dated
  `order_date`) is a modelling problem, not an ingestion one.

## Future: Spark 4.2 `CHANGES`

Spark 4.2 added a DSv2 changelog API (`TableCatalog.loadChangelog` + `Changelog`)
behind `SELECT ... CHANGES FROM VERSION ...`. It is only available to JVM
connectors. SQL Server CDC maps naturally: `__$operation` -> `_change_type`,
LSN hex -> `_commit_version`, `tran_end_time` -> `_commit_timestamp`, and the
requirement that "all rows of a single commit must appear in the same micro-batch"
is exactly how this source cuts batches. A Scala implementation is a candidate v0.2.
