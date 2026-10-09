# Streaming into Delta

`stream(spark, options).to_delta(...)` runs the CDC source into a Delta bronze table through
an idempotent sink, with one facts row per micro-batch. It is the common pipeline, declared
once; the [by-hand form](#the-same-pipeline-by-hand) at the end is for another sink or more
control.

## The smallest stream

```python
from mssql_cdc import stream

options = {
    "connectionString": "Server=sqlhost,1433;Database=sales;UID=cdc_reader;PWD=...;Encrypt=yes",
    "captureInstance": "dbo_orders",
}
query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/checkpoints/orders",
    facts_table="ops.ingestion_facts",
    trigger={"availableNow": True},
)
query.awaitTermination()
```

This reads every change CDC still holds for `dbo_orders`, appends it to `bronze.orders`,
records each batch in `ops.ingestion_facts` and stops once it has caught up. Run it again
and it resumes from the checkpoint. To load the rows that changed before CDC retention's
window too, add `bootstrap=True` ([Bootstrap](bootstrap.md)).

## How it works

`stream()` registers the data sources on the session and keeps the options. `to_delta`
starts `spark.readStream.format("mssql_cdc")` with them, writes every micro-batch through
`foreachBatch` with `delta_sink`, and returns the `StreamingQuery` without waiting for it.
The query is named after the sink's `app_id` (`<app_id>.g<n>` in a re-snapshot's generation
`n`) unless `query_name` says otherwise, so the Spark UI, progress logs and listener events
match the facts rows. The bootstrap, snapshots and `backfill()` waves log their start and
end at INFO, and a decision to re-snapshot logs a WARNING with the lost range (loggers under
`mssql_cdc`).

Each micro-batch reads the commits after the last processed offset, up to an end that never
passes `sys.fn_cdc_get_max_lsn()`, the last commit CDC capture has processed. So every batch
is a prefix of the source's commit history and ends on a commit boundary. The sink then:

- appends the change rows to the target, keyed by `app_id` and the batch id so that a
  replayed batch is skipped; the table is created by the first batch with rows, with a
  comment on every metadata column;
- writes the batch's facts row, also for a batch that read no rows (`rows = 0`), so a
  current stream on a quiet table keeps writing facts.

The design is in [Architecture](../ARCHITECTURE.md); the columns written are in
[Output schema](../reference/output-schema.md) and [Tables](../reference/tables.md).

Every parameter of `to_delta`, with its default, is in
[Options](../reference/options.md#to_delta-parameters).

## Options

`options` are the data source options, the same keys `spark.readStream.option()` takes,
matched ignoring case. Only `connectionString` and `captureInstance` are required; the
captured columns and their types are read from the capture instance. The ones most streams
touch:

- `maxCommitsPerBatch`: the most commits one micro-batch reads (counted in
  `cdc.lsn_time_mapping`). Unlimited by default, so the first run after a long stop reads
  everything up to `max_lsn` in one batch; set it to keep batches small.
- `numPartitions`: at most how many LSN ranges, each read on its own connection, one batch is
  split into. `auto` takes the session's cores; set a number to cap the load on SQL Server.
  A range holds about 50,000 change rows or more, so small batches are read in one.
- `startingLsn`: where a new checkpoint starts. `earliest` (the default, the oldest change
  CDC still holds), `latest`, or an LSN such as `0x0000002A000001F40003`, taken as already
  processed. A checkpoint that has offsets ignores it.
- `sourceTimeZone`: the server clock's Windows zone name, when `auto` cannot tell (see
  [Installation](../getting-started/installation.md#prepare-sql-server)).

Every option, with its default, is in [Options](../reference/options.md).

## Triggers

`trigger` is passed to Spark's `DataStreamWriter.trigger` as keyword arguments.

| `trigger=` | Behaviour | Fits |
|---|---|---|
| `{"availableNow": True}` | Records `max_lsn` when the query starts, reads up to it in batches of `maxCommitsPerBatch` commits, then stops | Scheduled jobs: wait for the query, then advance `finalized_until` |
| `{"processingTime": "1 minute"}` | One batch per interval, until stopped | An always-on stream, with a bounded load on SQL Server and a bounded number of facts commits |
| none | Batches back to back, until stopped; while the source is idle Spark polls it about every 10 ms, a `max_lsn` query per poll | The lowest latency, at the cost of a steady stream of queries on an idle source |

Prefer a `processingTime` trigger for an always-on stream, all the more with
[start_many](many-tables.md), which multiplies the polling by the number of tables.

Every trigger writes one facts commit, with rows or without. With the
[heartbeat job](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/sql/heartbeat.sql)
on a quiet database and no trigger, that is a batch about every 10 seconds, some 8,600 small
commits a day per stream: use a `processingTime` trigger to bound it, and keep the tables
small:

- compact the facts table and bronze (optimized writes or auto compaction, or a scheduled
  `OPTIMIZE` and `VACUUM`); bronze gets a file per range each batch with rows reads (at
  most `numPartitions` per capture instance). `OPTIMIZE ops.ingestion_facts
  ZORDER BY (target, app_id)` keeps each stream's rows together for the library's own reads;
- delete old micro-batch rows from the facts table, never its event rows, which the
  snapshots, `apply_changes` and `reconcile` read back:

```sql
DELETE FROM ops.ingestion_facts
WHERE event IS NULL
  AND written_at < convert_timezone('UTC', localtimestamp()) - INTERVAL 90 DAYS;
```

Keep the window longer than any stream may stay stopped: the newest micro-batch rows of an
`app_id` are what the [checkpoint checks](#app_id) and `backfill()`'s headroom pause read.

After an `availableNow` run, advance the verdict from the query's last progress:

```python
from mssql_cdc import finalization

query.awaitTermination()
end = finalization.end_offset_from_progress(query.lastProgress)
finalization.advance(spark, "ops.table_finalization", "bronze.orders", end)
```

For a query that keeps running, `finalization.track` advances it after every batch
([Continuous mode](finalization.md#continuous-mode)).

## Checkpoints

The checkpoint is Spark's: it holds the offsets, each the last processed commit LSN and its
commit time in UTC, `{"lsn": "0x...", "commit_ts": "..."}`. A rerun resumes after the last
committed batch. `to_delta` keeps two more things under the same path: the default metrics
directory, `_mssql_cdc_metrics`, and after a re-snapshot the generation state and the new
generations' checkpoints ([Generations](../ARCHITECTURE.md#generations-to_delta)).

The kind of path matters:

| Checkpoint | Stream | Default metrics | `on_data_loss="resnapshot"` |
|---|---|---|---|
| Local path, or a FUSE path such as `/Volumes/...` | yes | yes | yes |
| URI (`dbfs:/`, `abfss://`, `s3://`...) | yes | no: set `metricsPath` | refused |
| `/dbfs/...` | yes | yes | refused: Spark and Python resolve it to different directories |

On a cluster, a local path is local to each node: use a path every node shares, such as a
Volume.

## app_id

`app_id` names the sink. Delta records it with each append (its `txnAppId`, with the batch
id as `txnVersion`), so a batch replayed after a crash is skipped instead of written twice.
The facts table uses `<app_id>#facts` the same way, and snapshot events `<app_id>#events`.

- Keep `app_id` for the life of the checkpoint, and give each stream its own.
- **A new checkpoint needs a new `app_id`.** Batch ids restart at 0, and Delta skips every
  batch whose id is not above the last one it recorded for that `app_id`. With a
  `facts_table`, a checkpoint deleted or rewound under the same `app_id` fails its first
  batch with an error that says so; without one, its batches are dropped without an error
  while the query and `finalization.track` carry on.
- After an automatic re-snapshot, `to_delta` derives `<app_id>.g<n>` for the new generation
  by itself; keep passing the original ([Data loss](data-loss.md#generations)).

## Metrics and `metricsPath`

With a facts table, every partition of a batch leaves a small file with its round trip to
SQL Server, read time, megabytes and network wait; the batch's last partition adds where the
stream is: the retention watermark, the capture lag and the commit time of the batch's end.
The sink folds them into the batch's facts row and deletes them. Without them, the facts still have counts and LSN ranges, but `end_lsn`,
`retention_headroom_hours`, the lags and the network columns are NULL, and schema change
events do not reach the facts ([Monitoring](monitoring.md)).

- `to_delta` sets `metricsPath` to `<checkpoint>/_mssql_cdc_metrics` on its own for a local
  or FUSE checkpoint.
- With a URI checkpoint, set the `metricsPath` option to a directory every node can write
  and the driver can read: a Volume, a local path on a single node, or a URI `pyarrow.fs`
  opens with credentials every node has (`s3://`, `gs://`, `abfss://`, `hdfs://`; not
  `dbfs:/`, a `ValueError` when the query starts). `to_delta` puts each stream's files under
  `<metricsPath>/<app_id>`, so streams may share one
  ([metricsPath](../reference/options.md#metricspath)).

## The same pipeline by hand

`register(spark)` adds `format("mssql_cdc")` (and `format("mssql_cdc_snapshot")`) to the
session; `delta_sink` is the `foreachBatch` function `to_delta` uses.

```python
from mssql_cdc import register
from mssql_cdc.sink import delta_sink

register(spark)

query = (
    spark.readStream.format("mssql_cdc")
    .options(**options)
    .option("metricsPath", "/Volumes/main/ops/metrics/orders")
    .load()
    .writeStream.foreachBatch(
        delta_sink(
            "bronze.orders",
            app_id="orders-v1",
            facts_table="ops.ingestion_facts",
            metrics_path="/Volumes/main/ops/metrics/orders",
        )
    )
    .option("checkpointLocation", "/checkpoints/orders")
    .trigger(availableNow=True)
    .start()
)
```

By hand, `metricsPath` and `metrics_path` must be the same directory, used by this stream
alone: the sink folds and deletes every file in it. Bootstrap, re-snapshots and generations
are `to_delta`'s; by hand they are yours. Any other `writeStream` sink works too, with
idempotency and facts then up to it.

## Pitfalls

- Run one job per stream. Spark refuses two queries on one checkpoint, and the re-snapshot
  pre-flight assumes nothing else uses it.
- `bootstrap=True` sets `startingLsn` itself; passing both is a `ValueError`.
- Each partition opens a connection to SQL Server. Many streams with `numPartitions=auto` on
  a large cluster add up: cap it on a busy source.
- On a Spark older than 4.2 without the admission control backport, `maxCommitsPerBatch`
  has no effect ([Installation](../getting-started/installation.md)).
- A query that stops with `DataLossError` or `SchemaChangedError` needs a decision, not
  a blind retry: see [Data loss](data-loss.md) and [Schema changes](schema-changes.md).
- A broken connection, a failover or a deadlock on the driver (offsets and planning) is
  retried on a new connection up to three times, within about 14 seconds, with a WARNING
  each time; a longer outage stops the query, for the job's own retry. A task's read is
  retried by Spark, as any failed task. A snapshot read waits for writers' locks unless
  [lockTimeoutMs](../reference/options.md#locktimeoutms) is set
  ([ADR 0029](../decisions/0029-driver-retries-and-lock-timeout.md)).

## See also

- [Bootstrap](bootstrap.md): the initial load.
- [Monitoring](monitoring.md): what the facts say and what to alert on.
- [Running on Databricks](../DATABRICKS.md): libraries, Volumes and notebooks.
- [`stream`](../reference/api.md#mssql_cdc.stream) and
  [`CdcStream.to_delta`](../reference/api.md#mssql_cdc.pipeline.CdcStream.to_delta) in the
  API reference.
