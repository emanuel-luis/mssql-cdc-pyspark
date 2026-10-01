# Monitoring

The facts table answers the questions an on-call engineer asks about a CDC stream: is it
running, how far behind is it, how close is it to losing data, and why was that batch
slow. It holds one row per micro-batch, batches that read nothing included, plus one row
per snapshot and per change to the source table.

## Turn it on

Pass `facts_table` to `to_delta`:

```python
from mssql_cdc import stream

query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/data/checkpoints/orders",
    facts_table="ops.ingestion_facts",
)
```

The counts and ranges are always there. The read, retention and lag columns come from
files each partition leaves in a metrics directory (option `metricsPath`), which the sink
folds into the batch's row. `to_delta` puts that directory under the checkpoint when the
checkpoint is a path Python can write on every node (local, or a FUSE mount). With a URI
checkpoint (`s3a://`, `abfss://`...), set `metricsPath` yourself to such a path;
the files then go to `<metricsPath>/<app_id>`, so streams may share one
([Options](../reference/options.md),
[ADR 0014](../decisions/0014-network-and-read-metrics-in-facts.md)).
Without it those columns are NULL. The sink never connects to SQL Server itself.

## What a row holds

A micro-batch row (`event` NULL):

| Group | Columns | Filled |
|---|---|---|
| Identity | `app_id`, `batch_id`, `target`, `written_at` | always |
| What was written | `rows`, `inserts`, `updates`, `deletes`, `min_lsn`, `max_lsn`, `min_commit_ts`, `max_commit_ts` | always; ranges NULL when `rows = 0` |
| Timing | `started_at`, `duration_ms` | always |
| Where the stream is | `end_lsn`, `end_commit_ts` | with metrics |
| Read and network | `source_rtt_ms`, `read_seconds`, `read_mb`, `network_wait_ms` | with metrics |
| Retention | `retention_watermark_ts`, `retention_headroom_hours` | with metrics |
| Lag | `source_max_commit_ts`, `capture_lag_seconds`, `ingestion_lag_seconds` | with metrics |

Event rows (`event` set) are different: `min_lsn`, `max_lsn` and `end_lsn` all hold the
event's LSN, and the commit-time columns its commit time, whatever `rows` says. Their
`started_at` and `duration_ms` are NULL, except on a snapshot row whose snapshot that run
took (a reused one also has `rows` NULL). `detail` is set on source changes,
`lost_from_ts` and `lost_to_ts` on re-snapshots.

Every column's comment is in [Tables](../reference/tables.md). Times are UTC, as
`TIMESTAMP_NTZ`. The `event` column tells the kinds of rows apart:

- NULL: a micro-batch. One whose end offset moved only past idle entries or other tables'
  commits has `rows = 0` and wrote nothing to the target.
- `bootstrap` and `resnapshot`: a snapshot, with no `batch_id`
  ([Bootstrap](bootstrap.md), [Data loss](data-loss.md)).
- `schema_change` and `capture_instance_switched`: DDL on the source table or a move to a
  newer capture instance, with the `batch_id` of the batch that read past it, `rows = 0` and
  the change in `detail` ([Schema changes](schema-changes.md)).

Statistics over micro-batches filter `event IS NULL`.

## Health of every stream

One query covers four alerts, on the latest micro-batch row of each target.
`convert_timezone('UTC', localtimestamp())` is the current UTC time as `TIMESTAMP_NTZ`,
whatever the session time zone:

```sql
WITH last AS (
  SELECT
    target,
    timestampdiff(SECOND, max(written_at), convert_timezone('UTC', localtimestamp())) / 3600.0
      AS hours_since_last,
    max_by(retention_headroom_hours, written_at) AS headroom_hours,
    max_by(ingestion_lag_seconds, written_at) AS ingestion_lag_seconds,
    max_by(capture_lag_seconds, written_at) AS capture_lag_seconds
  FROM ops.ingestion_facts
  WHERE event IS NULL
  GROUP BY target
)
SELECT *
FROM last
WHERE hours_since_last > 0.25                 -- no facts for 15 minutes
   OR headroom_hours - hours_since_last < 24  -- less than a day before cleanup catches up
   OR ingestion_lag_seconds > 3600            -- the stream is an hour behind capture
   OR capture_lag_seconds > 600;              -- capture is behind the database
```

Drop the `WHERE` to see every stream; the thresholds are examples.

### Facts stopped arriving

A running stream writes a row on every trigger, so silence means
the stream stopped, or CDC capture did (`max_lsn` freezes and no batch runs). For a stream
that keeps running (a `processingTime` trigger or the default), expect a row at least every
5 minutes (the idle entries of a quiet
database; about 10 seconds with the
[heartbeat](../decisions/0010-heartbeat-for-quiet-databases.md)) plus the trigger interval.
A job run with `availableNow` on a schedule writes only while it runs: use the schedule
interval. To tell the stream from capture, see [live capture lag](#live-capture-lag) below.

### Retention headroom falls

`retention_headroom_hours` is how far the stream's position is
ahead of what CDC cleanup has deleted. A current stream sits near the retention period (3
days by default) and the value falls by the hours the stream lags; at 0 the next changes
are being purged and the stream stops with `DataLossError` (or re-snapshots, with
`on_data_loss="resnapshot"`). Cleanup moves the watermark in
steps (the default job runs daily), so keep a margin larger than that, and subtract the
hours since the last row: a stopped stream keeps its last value while the real headroom
shrinks ([ADR 0017](../decisions/0017-retention-headroom-in-facts.md)).

### Ingestion lag grows

`ingestion_lag_seconds` is how far the stream is behind what CDC
capture has processed: slow batches, a sparse trigger, a stopped job. It is near 0 for a
current stream, on a quiet table too. What it gains, the headroom loses.

### Capture lag grows

`capture_lag_seconds` is how stale CDC capture itself was when the
batch read: a log backlog, for the DBA rather than for the stream. On a quiet database
without the heartbeat it sits up to about 5 minutes, so alert above that
([ADR 0020](../decisions/0020-capture-and-ingestion-lag-in-facts.md)).

## Why a batch was slow

```sql
SELECT
  batch_id, rows, duration_ms, read_seconds, read_mb, source_rtt_ms, network_wait_ms,
  round(try_divide(network_wait_ms, read_seconds * 1000), 2) AS network_share
FROM ops.ingestion_facts
WHERE target = 'bronze.orders' AND event IS NULL AND rows > 0
ORDER BY written_at DESC
LIMIT 20;
```

`network_wait_ms` is the time SQL Server waited for the client to take the rows
(`ASYNC_NETWORK_IO`). A `network_share` close to 1 with a high `source_rtt_ms` means the
link set the pace; a low share with a long read means the server did. `read_seconds` sums
the partitions (task-seconds), so with parallel partitions it can exceed `duration_ms`.

## Snapshots and source changes

```sql
SELECT written_at, target, event, detail, min_lsn, rows, lost_from_ts, lost_to_ts
FROM ops.ingestion_facts
WHERE event IS NOT NULL
ORDER BY written_at DESC;
```

A `resnapshot` row is worth an alert of its own: the changes committed between
`lost_from_ts` and `lost_to_ts` were purged before the stream read them.

## Live capture lag

The facts cannot show a stopped capture: no batch runs, so the last `capture_lag_seconds`
stays small. The query progress can. On every trigger, idle ones included, the source
reports `max_lsn` and its commit time as `latestOffset`:

```python
import json
from datetime import datetime, timezone

latest = json.loads(query.lastProgress.json)["sources"][0]["latestOffset"]
now = datetime.now(timezone.utc).replace(tzinfo=None)
capture_lag = now - datetime.fromisoformat(latest["commit_ts"])
```

If facts stopped arriving while the query still runs and this lag grows, capture has
stopped (capture job or SQL Server Agent down).

## Pitfalls

- Every micro-batch is a facts commit. A quiet stream with the heartbeat and the default
  trigger runs a batch about every 10 seconds, some 8,600 rows and small files a day. Bound
  the rate with a `processingTime` trigger, and compact the facts table (auto compaction,
  or a scheduled `OPTIMIZE` and `VACUUM`).
- Metrics are NULL without `metricsPath` (a URI checkpoint without one), and on the first
  batch of a new checkpoint that had nothing to read.
- When you wire the sink by hand, give `delta_sink(metrics_path=...)` the same directory as the
  `metricsPath` option and use it for one stream only: the sink folds and removes every file
  in it.
- `capture_lag_seconds` compares the Spark node's clock with SQL Server's
  commit times; skew shifts it and can make a small lag negative.
- Rows written before `end_commit_ts` existed measured the headroom and the
  ingestion lag from `max_commit_ts`, which made a current stream on a quiet table look
  behind.
- The facts table is the durable record. Bronze commits carry the same facts in
  `userMetadata` (batches with rows only), but Delta log cleanup drops them.

## See also

- [Tables](../reference/tables.md): every facts column with its comment.
- [Finalization](finalization.md): the verdict the facts back up.
- [Data loss](data-loss.md): what happens when the headroom reaches 0.
- [ADR 0014](../decisions/0014-network-and-read-metrics-in-facts.md),
  [ADR 0017](../decisions/0017-retention-headroom-in-facts.md),
  [ADR 0020](../decisions/0020-capture-and-ingestion-lag-in-facts.md): how each metric is
  measured.
