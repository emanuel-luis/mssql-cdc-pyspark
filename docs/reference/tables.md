# Tables

The Delta tables the library writes. You name each one; a name such as `ops.ingestion_facts`
is a catalog table, and anything with a `/` or a `:` is a path.

| Table | Grain | Written by | You pass it as |
|---|---|---|---|
| [Bronze](#bronze) | one row per change | `to_delta` (`delta_sink`, snapshots) | `target` |
| [Facts](#facts) | one row per micro-batch, snapshot and source change | `to_delta` (`delta_sink`) | `facts_table` |
| [Control](#control) | one row per table | `finalization.advance`, `apply_changes` | `control_table` |
| [Silver](#silver) | one row per source key | `apply_changes` | its `target` |

Each is created on first use with the `DeltaTable` builder: explicit types, a comment on the
table and on every column the library defines, which `DESCRIBE TABLE` shows
([ADR 0012](../decisions/0012-delta-tables-through-the-deltatable-api.md)). The comments
below are the ones the tables carry, copied from the code (`mssql_cdc.sink`,
`mssql_cdc.finalization`, `mssql_cdc.silver` and `mssql_cdc.migrations`), and
`tests/test_docs_tables.py` fails when they drift. How the pieces fit: [Architecture](../ARCHITECTURE.md#tables-written-by-the-sink-finalization-and-silver).

## Bronze

The change log, append-only.

> Append-only change rows from SQL Server CDC, written by mssql-cdc-pyspark's delta_sink. One row per change: an update is two rows (operation 3, the row before; 4, the row after). Order changes by (_start_lsn, _command_id, _seqval, _operation). Rows with operation 0 are a snapshot of the source table, all at one _start_lsn that precedes the changes read after it. A column the source table gained through a newer capture instance is added when the stream first reads it (older rows read NULL); a column it lost stays, NULL from then on.

| Column | Type | Comment |
|---|---|---|
| `_capture_instance` | STRING | CDC capture instance the change came from, e.g. dbo_orders: after the stream switched to a newer capture instance of the table (from that instance's start LSN on), the newer one. On snapshot rows, the instance the snapshot was taken for. |
| `_start_lsn` | STRING | Commit LSN of the source transaction (__$start_lsn) as 0x + 20 uppercase hex. All changes of one transaction share it; string order is commit order. On snapshot rows, the LSN recorded before the table was read: the row is at least that recent. |
| `_seqval` | STRING | Position of the change in the transaction log (__$seqval), 0x + 20 hex. Tie-breaker only: order by _command_id first. NULL on snapshot rows. |
| `_operation` | INT | What happened to the row: 1 = delete, 2 = insert, 3 = update (row before), 4 = update (row after), 0 = snapshot (the row as read from the source table). |
| `_command_id` | INT | Order of the statement within its transaction (__$command_id). Numbered per capture instance: another instance of the table numbers the same change differently, so it orders rows only within one _start_lsn (all rows of a commit come from one instance); (_start_lsn, _seqval, _operation) identifies a change across instances. NULL on snapshot rows. |
| `_commit_ts` | TIMESTAMP_NTZ | Commit time of the source transaction, UTC (from cdc.lsn_time_mapping); on snapshot rows, the commit time of their _start_lsn. |
| captured columns | see [Output schema](output-schema.md#captured-columns) | none |
| `_batch_id` | INT | Micro-batch that wrote the row; with the sink's app_id, the key of its row in the ingestion facts table. NULL on snapshot rows. |

`_command_id` is absent with `includeCommandId=false`. A column a newer capture instance
captures is added after the existing ones.

How it is written:

* One append per micro-batch that read rows, with `txnAppId` = `app_id` and `txnVersion` =
  the batch id, so a replayed batch is skipped. Its `userMetadata` holds the batch's facts as
  JSON: `rows`, `min_lsn`, `max_lsn`, `min_commit_ts`, `max_commit_ts`, `deletes`,
  `inserts`, `updates`, `batch_id` and `app_id`. A batch that read no rows writes no commit.
* A snapshot is one append whose `userMetadata` is `{"snapshot": <capture instance>, "lsn":
  ..., "commit_ts": ...}`.
* Every append uses `mergeSchema`. A changed column type fails it with `SchemaChangedError`
  unless the table has `delta.enableTypeWidening` and the change widens
  ([Schema changes](../guides/schema-changes.md#changing-a-column-type)).

```sql
DESCRIBE HISTORY bronze.orders;  -- userMetadata of each append
```

## Facts

What each micro-batch did, plus one row per snapshot and per source change. Optional, but
monitoring, the data-loss recovery and the schema-change events all need it.

> One row per micro-batch written by mssql-cdc-pyspark's delta_sink, including batches that read no change rows (rows = 0), so a current stream on a quiet table keeps writing rows: what was written (counts, LSN and commit-time ranges), how far the stream had read (end_lsn, end_commit_ts) and how long it took. The same facts are in each target commit's userMetadata (batches with rows only), which Delta log cleanup eventually drops. Each snapshot stream().to_delta takes (bootstrap or re-snapshot), each schema change on the source and each switch to a newer capture instance adds one row, with event set (see its comment).

| Column | Type | Comment |
|---|---|---|
| `app_id` | STRING | Identity of the sink that wrote the batch (Delta txnAppId of the target write). Stable for the life of one streaming checkpoint; a new checkpoint needs a new app_id. |
| `batch_id` | BIGINT | Structured Streaming micro-batch id. With app_id, the idempotency key: a replayed batch is skipped, so its row (event NULL) never appears twice. Its 'schema_change' and 'capture_instance_switched' rows carry it too; snapshot event rows have none. |
| `rows` | BIGINT | Change rows written to the target in this batch, all operations. 0 when the batch read none: its end offset moved only past idle entries or other tables' commits (or, on a new checkpoint's first batch, not at all), so it wrote nothing to the target, just this row (LSN and commit-time ranges NULL, counts 0). On 'bootstrap' and 'resnapshot' rows, the rows of the snapshot; 0 on other event rows. |
| `min_lsn` | STRING | Smallest source commit LSN (__$start_lsn, 0x + 20 hex) in the batch. |
| `max_lsn` | STRING | Largest source commit LSN in the batch; hex strings sort in LSN order. |
| `min_commit_ts` | TIMESTAMP_NTZ | Earliest source commit time in the batch, UTC. |
| `max_commit_ts` | TIMESTAMP_NTZ | Latest source commit time in the batch, UTC. written_at minus this is the batch's ingestion latency. |
| `deletes` | BIGINT | Rows with operation 1 (delete). |
| `inserts` | BIGINT | Rows with operation 2 (insert). |
| `updates` | BIGINT | Updated rows, counted once: operation 4 (the row after). Each has an operation 3 row (the row before) that is not counted here. |
| `started_at` | TIMESTAMP_NTZ | When the sink started processing the batch, UTC. |
| `duration_ms` | BIGINT | Milliseconds from started_at to the end of the target write: the read from SQL Server, these facts and the append. Offset planning and the checkpoint commit are not included. |
| `source_rtt_ms` | DOUBLE | Network latency to SQL Server during the batch: the median, over its partitions, of one round trip (SELECT 1) each made on its own connection just before reading, in milliseconds. NULL unless the source option metricsPath and delta_sink(metrics_path=...) are set. |
| `read_seconds` | DOUBLE | Seconds the batch's partitions spent reading from SQL Server, summed over partitions (task-seconds: with parallel partitions it can exceed wall time), including Spark taking the rows as they arrive. NULL unless the source option metricsPath and delta_sink(metrics_path=...) are set. |
| `read_mb` | DOUBLE | Megabytes of Arrow data the partitions read, after the cast to the Spark schema, summed. NULL under the same condition as read_seconds. |
| `network_wait_ms` | BIGINT | Milliseconds SQL Server waited for the client to take the rows (ASYNC_NETWORK_IO of each partition's session), summed. Close to read_seconds * 1000 means the network, not the server, set the pace. NULL under the same condition, or where the server does not expose sys.dm_exec_session_wait_stats. |
| `retention_watermark_ts` | TIMESTAMP_NTZ | How far CDC cleanup had deleted when the batch was read: the commit time (UTC) of sys.fn_cdc_get_min_lsn for the capture instance, the latest seen by the batch's partitions after reading. Changes committed before it are gone from the source. NULL unless the source option metricsPath and delta_sink(metrics_path=...) are set. |
| `retention_headroom_hours` | DOUBLE | Hours between retention_watermark_ts and end_commit_ts: how far the stream's position (the batch's end offset) is ahead of what cleanup has deleted. A current stream sits near the retention period (3 days by default), on a quiet table too, since a batch that read no rows also writes its row; it shrinks as the stream falls behind, and at 0 the next changes to read are being purged. Cleanup moves the watermark in steps (the default job runs daily), so alert with more margin than that interval, and also when facts stop arriving: the stream or CDC capture has stopped, and the real headroom keeps shrinking from the last value. Rows written before end_commit_ts existed measured from max_commit_ts, the batch's last change, which on a quiet table made a current stream look behind. NULL under the same condition as retention_watermark_ts. |
| `event` | STRING | What the row records: NULL for a micro-batch. Snapshots, with no batch_id: 'bootstrap' for the initial snapshot of the target; 'resnapshot' for a snapshot taken because CDC cleanup purged changes before the stream read them; min_lsn = max_lsn is the LSN the snapshot is stamped with, and the only trace of a snapshot of an empty table (rows = 0), which writes no target rows. Changes to the source (ADR 0023), with the batch_id of the batch that read past them and rows = 0: 'schema_change' for DDL on the source table, 'capture_instance_switched' when the stream first read a newer capture instance of the table (the older one can be dropped once the same app_id has a row with a larger batch_id: Spark commits the batch after this row); min_lsn = max_lsn is the change's LSN, detail says what changed. Downstream rebuilds only from 'bootstrap' and 'resnapshot' rows. |
| `lost_from_ts` | TIMESTAMP_NTZ | On 'resnapshot' rows, the UTC commit time of the last offset the stream had processed. Changes committed after it and before lost_to_ts were purged unread: the target has the rows as of the snapshot, but those changes are missing from its change history. NULL on other rows. |
| `lost_to_ts` | TIMESTAMP_NTZ | On 'resnapshot' rows, the UTC commit time of the CDC retention watermark (sys.fn_cdc_get_min_lsn) when the loss was detected: where the gap in the change history ends. NULL on other rows. |
| `source_max_commit_ts` | TIMESTAMP_NTZ | How far CDC capture had got when the batch was read: the commit time (UTC) of sys.fn_cdc_get_max_lsn, the latest seen by the batch's partitions after reading. The stream can read nothing newer than this. NULL unless the source option metricsPath and delta_sink(metrics_path=...) are set. |
| `capture_lag_seconds` | DOUBLE | How stale CDC capture was: seconds from source_max_commit_ts to the moment a partition read it (its own clock, UTC), the largest over the batch's partitions. Seconds on a busy database; up to about 5 minutes on a quiet one, where capture writes an idle entry that often, unless the heartbeat job runs. Growing beyond that means capture is slow (a large log backlog), whatever the stream does. A stopped capture (capture job or SQL Server Agent down) does not show here: max_lsn freezes, no batch runs and the last value stays small; only facts stop arriving. For that, use now minus latestOffset.commit_ts in the query progress. Clock skew between the Spark nodes and SQL Server shifts it. NULL under the same condition as source_max_commit_ts. |
| `ingestion_lag_seconds` | DOUBLE | Seconds between end_commit_ts and source_max_commit_ts: how far the stream's position (the batch's end offset) is behind what CDC capture had processed. Near 0 for a current stream, on a quiet table too, since a batch that read no rows also writes its row. Growing means the stream is falling behind, and as it grows retention_headroom_hours shrinks. Only moves while the stream runs: also alert when facts stop arriving (the stream or CDC capture has stopped). Rows written before end_commit_ts existed measured from max_commit_ts, which also counted the time from the table's last change to the database's newest commit. NULL under the same condition as source_max_commit_ts. |
| `end_lsn` | STRING | The batch's end offset: the commit LSN (0x + 20 hex) the stream had processed up to after this batch, the largest to_lsn of its partitions. At or after max_lsn: offsets follow CDC capture (sys.fn_cdc_get_max_lsn), which moves with idle entries and with other tables' commits, so on a quiet table it keeps moving while its batches read no rows. On event rows, the snapshot's or the change's LSN. NULL unless the source option metricsPath and delta_sink(metrics_path=...) are set, and on a batch that planned no range to read (a new checkpoint's first batch when nothing is new). |
| `end_commit_ts` | TIMESTAMP_NTZ | Commit time (UTC) of end_lsn: how far through the source's commit history the stream had read after this batch, whether the batch had rows or not. retention_headroom_hours and ingestion_lag_seconds are measured from it. Later than max_commit_ts, the batch's last change, when the table changed less recently than the database. On event rows, the snapshot's or the change's commit time. NULL under the same condition as end_lsn. |
| `detail` | STRING | On 'schema_change' rows, the DDL statement; on 'capture_instance_switched' rows, 'old -> new' capture instance, plus the columns the query reads that the new one does not capture (NULL from then on). NULL on other rows. |
| `target` | STRING | Table name or path the batch was written to. |
| `written_at` | TIMESTAMP_NTZ | When this facts row was written, after the target commit, UTC. |

The kinds of row, and the Delta `txnAppId` and `txnVersion` that make each write idempotent:

| Row | `event` | `batch_id` | `rows` | `app_id` | Idempotency key |
|---|---|---|---|---|---|
| Micro-batch | NULL | the batch's | change rows, 0 when none | the sink's | `<app_id>#facts`, the batch id |
| Initial snapshot | `bootstrap` | NULL | snapshot rows | as passed to `to_delta` | `<app_id>#events`, 0 |
| Re-snapshot after data loss | `resnapshot` | NULL | snapshot rows | the new generation's, `<app_id>.g<n>` | `<app_id>#events`, `n` |
| DDL on the source | `schema_change` | the batch's | 0 | the sink's | in the batch's own commit |
| Switch to a newer capture instance | `capture_instance_switched` | the batch's | 0 | the sink's | in the batch's own commit |

The sink's `app_id` is the one passed to `to_delta`, or `<app_id>.g<n>` in generation `n`.
Statistics over micro-batches filter `event IS NULL`; the snapshots downstream rebuilds from
are `event IN ('bootstrap', 'resnapshot')`:

```sql
SELECT app_id, batch_id, rows, end_commit_ts, retention_headroom_hours, ingestion_lag_seconds
FROM ops.ingestion_facts
WHERE target = 'bronze.orders' AND event IS NULL
ORDER BY written_at DESC
LIMIT 20;
```

What to alert on: [Monitoring](../guides/monitoring.md). The metric columns and `end_lsn`
are NULL without [metricsPath](options.md#metricspath).

## Control

The completeness verdict, `finalized_until`, one row per table, and the position of each
silver table.

> Completeness verdict per CDC-fed table, kept by mssql-cdc-pyspark: one row per table. Gate downstream work on finalized_until, e.g. with finalization.is_final().

| Column | Type | Comment |
|---|---|---|
| `table_name` | STRING | The table this verdict is about: the name passed to finalization.advance(), usually the target table. One row per table. |
| `finalized_until` | TIMESTAMP_NTZ | The verdict, UTC. Every period that ends at or before this instant is complete in the table: no source commit at or before it can still arrive. It only moves forward. A consumer of the period [start, end) waits for finalized_until >= end. |
| `end_lsn` | STRING | Source commit LSN (0x + 20 hex) of the batch end that last moved the verdict: how far the source had been read and committed to the table. |
| `end_commit_ts` | TIMESTAMP_NTZ | Commit time of end_lsn, UTC. finalized_until is this instant truncated to the period (an hour by default), because transactions sharing this exact commit time may still be arriving. |
| `updated_at` | TIMESTAMP_NTZ | When the verdict last moved, UTC. |
| `applied_lsn` | STRING | Tables built by mssql_cdc.apply_changes: the highest source commit LSN (0x + 20 hex) of the bronze changes applied to the table; the next call reads the changes after it. NULL for other tables. |
| `snapshot_lsn` | STRING | Tables built by mssql_cdc.apply_changes: the LSN of the bronze snapshot the table was last rebuilt from; a newer snapshot rebuilds it. NULL for other tables and before the first snapshot. |

`finalization.advance` MERGEs on `table_name` and updates the row only when the new verdict
is later, so it never moves back. `apply_changes` records `applied_lsn` and `snapshot_lsn`
for its silver table, then advances that table's verdict. See
[Finalization](../guides/finalization.md) and [Silver](../guides/silver.md).

```sql
SELECT table_name, finalized_until, end_lsn, applied_lsn, updated_at
FROM ops.table_finalization;
```

## Silver

The current state of the source table, built by `apply_changes`.

> Current state of a SQL Server table, one row per key, built from the bronze change log by mssql-cdc-pyspark's apply_changes. Deleted rows are removed. How far it is applied is applied_lsn in the control table; finalized_until there says which periods are complete.

| Column | Type | Comment |
|---|---|---|
| captured columns | as in bronze | none |
| `_start_lsn` | STRING | Commit LSN (0x + 20 hex) of the source transaction that wrote this row's current image; on a row unchanged since a snapshot, the snapshot's LSN. |
| `_commit_ts` | TIMESTAMP_NTZ | Commit time of _start_lsn, UTC. |

A column bronze gains is added to silver (older rows read NULL); a column's type never
changes ([ADR 0019](../decisions/0019-silver-helper-applies-the-change-log.md)).

## Schema versions

Each table carries the table property `mssql_cdc.schema_version`: the number of migrations
its kind has had. A new table is created at the latest version. An older one gets the
missing migrations, in order, the next time a stream, `finalization.advance` or
`apply_changes` opens it ([ADR 0013](../decisions/0013-schema-migrations-per-table-kind.md)).
The versions, from `mssql_cdc.migrations.<kind>.MIGRATIONS`:

| Kind | Version | Migrations |
|---|---|---|
| bronze | 1 | 1 capture instance comments |
| facts | 6 | 1 network and read metrics, 2 retention headroom, 3 snapshot events, 4 lag metrics, 5 end offset, 6 source change events |
| control | 1 | 1 add `applied_lsn` and `snapshot_lsn` |
| silver | 0 | none |

A migration that adds columns appends them, so in a table created by an older release they
come after `written_at` (facts) or `updated_at` (control): select columns by name. The
library sets no other table property; type widening is yours to enable.

```sql
SHOW TBLPROPERTIES ops.ingestion_facts ('mssql_cdc.schema_version');
```
