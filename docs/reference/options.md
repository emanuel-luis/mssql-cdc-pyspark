# Options

Everything a stream takes: the source options, given once to
[`stream()`](api.md#mssql_cdc.stream) or one by one to `spark.readStream.format("mssql_cdc")`,
and the parameters of [`to_delta()`](api.md#mssql_cdc.pipeline.CdcStream.to_delta),
[`snapshot()`](api.md#mssql_cdc.pipeline.CdcStream.snapshot),
[`backfill()`](api.md#mssql_cdc.pipeline.CdcStream.backfill) and
[`seed()`](api.md#mssql_cdc.pipeline.CdcStream.seed).

```python
from mssql_cdc import stream

options = {
    "connectionString": "Server=host,1433;Database=db;UID=u;PWD=p;Encrypt=yes",
    "captureInstance": "dbo_orders",
    "maxCommitsPerBatch": "500",
}
query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/Volumes/cat/sch/vol/ckpt/orders",
    facts_table="ops.ingestion_facts",
)
```

Option names are case-insensitive (`startinglsn` works). Pass values as strings, as Spark
does. A boolean option is true for `true`, `1`, `yes` or `y`, in any case, and false for
anything else.

## Source options

| Option | Default | Read by |
|---|---|---|
| [captureInstance](#captureinstance) | required | stream, snapshot |
| [connectionString](#connectionstring) | required | stream, snapshot |
| [backend](#backend) | `mssql-python` | stream, snapshot |
| [connectTimeout](#connecttimeout) | `30` | stream, snapshot |
| [sourceTimeZone](#sourcetimezone) | `auto` | stream, snapshot |
| [fakePath](#fakepath) | none | `backend=fake` only |
| [columns](#columns) | inferred | stream, snapshot |
| [includeCommandId](#includecommandid) | `true` | stream, snapshot |
| [startingLsn](#startinglsn) | `earliest` | stream |
| [maxCommitsPerBatch](#maxcommitsperbatch) | unlimited | stream |
| [numPartitions](#numpartitions) | `auto` | stream, snapshot |
| [arrowBatchSize](#arrowbatchsize) | `10000` | stream, snapshot |
| [failOnDataLoss](#failondataloss) | `true` | stream |
| [schemaChangePolicy](#schemachangepolicy) | `classify` | stream |
| [metricsPath](#metricspath) | none (see below) | stream |
| [snapshotLsn](#snapshotlsn) | `max_lsn` before the read | snapshot |
| [snapshotChunks](#snapshotchunks) | none: the whole table | snapshot |
| [snapshotKeys](#snapshotkeys) | the unique index's columns | snapshot |
| [isolationLevel](#isolationlevel) | `readCommitted` | snapshot |

"Snapshot" is `spark.read.format("mssql_cdc_snapshot")`, which `snapshot()`,
`to_delta(bootstrap=True)`, the re-snapshots, `backfill()` and `reconcile()` use.

### captureInstance

The CDC capture instance to read, such as `dbo_orders` (SQL Server's default name is
`<schema>_<table>`). Matched ignoring case, as SQL Server does; an exact match wins, and two
instances whose names differ only in case fail with a `ValueError` asking for the exact one.

When the table gets a newer capture instance the stream moves to it on its own, and a
configured name that was disabled still resolves when it is the table's default name. See
[Schema changes](../guides/schema-changes.md#keep-the-capture-instance-name).

### connectionString

Connection string for `mssql-python` or ODBC Driver 18, for example
`Server=host,1433;Database=db;UID=u;PWD=p;Encrypt=yes`; `backend=arrow-odbc` adds
`Driver={ODBC Driver 18 for SQL Server}` when it names no driver. Not needed with `backend=fake`.
The driver plans with it and every executor task opens its own connection with it, so the
server must be reachable from every worker. The login needs the grants in
[Permissions](../guides/permissions.md).

### backend

| Value | What it is |
|---|---|
| `mssql-python` | Microsoft's driver, installed with the package; fetches straight into Arrow |
| `arrow-odbc` | needs unixODBC and msodbcsql18 on every worker, and `pip install "mssql-cdc-pyspark[arrow-odbc]"` ([Installation](../getting-started/installation.md#the-driver)); the same connection string. A value over 64 KiB in a `(max)`, `text`, `xml` or `image` column fails the read |
| `fake` | the file-backed CDC simulator in `mssql_cdc.fake`, for the library's own tests; reads [fakePath](#fakepath). Internal: it may change in any release |

Anything else fails with `ValueError: Unknown backend`. Why `mssql-python` is the default:
[Connectors](../CONNECTORS.md) and [ADR 0003](../decisions/0003-mssql-python-default-backend.md).

### connectTimeout

Login timeout in seconds, passed to `mssql_python.connect`, or to `arrow_odbc.connect` with
`backend=arrow-odbc`.

### sourceTimeZone

Time zone of the SQL Server clock. `cdc.lsn_time_mapping` stores commit times without a
zone; this converts them to UTC for `_commit_ts`, the offsets and the facts. It does not
touch captured `datetime` columns.

* `auto`: `CURRENT_TIMEZONE_ID()` on SQL Server 2022+ and Azure SQL, which applies the
  daylight-saving rules in force at each commit. Older versions lack the function; then the
  server's current UTC offset (`SYSDATETIMEOFFSET()`) is applied to every commit, which is
  exact only for zones without daylight saving.
* A Windows time zone name, such as `E. South America Standard Time`, or `UTC`: set it on a
  server older than 2022 in a zone with daylight saving.

[ADR 0008](../decisions/0008-detect-source-time-zone.md).

### fakePath

With `backend=fake`, the directory that holds the simulator's state (`mssql_cdc.fake`).
Required there, ignored otherwise. Internal, like the fake itself.

### columns

Spark DDL of the captured columns to read, for example
`order_id INT, status STRING, amount DECIMAL(18,2)`. When omitted, the driver infers them at
`load()` from `sys.sp_cdc_get_captured_columns`, the union of every capture instance of the
table ([Output schema](output-schema.md#captured-columns)).

Pass it to read a subset of the columns, to choose other types (the read casts to them), or
when the backend has no column metadata (the fake without captured columns). A listed column
that no capture instance of the table captures fails the first planning and the snapshot
with a `ValueError`. A type change on the source made while the query runs is caught when the
batch is planned; one made while it is stopped fails the cast at read time: update
`columns` ([Schema changes](../guides/schema-changes.md#with-the-columns-option)).

### includeCommandId

Read `__$command_id` into `_command_id`, the order of a statement within its transaction.
With `false` the column is left out of the output. Set it only when the change tables lack
`__$command_id`, which can happen on SQL Server 2016
([ADR 0023](../decisions/0023-schema-changes-and-capture-instance-switching.md)).

### startingLsn

Where a new checkpoint starts. A checkpoint that already has offsets ignores it.

* `earliest`: the oldest change CDC still holds (`sys.fn_cdc_get_min_lsn` of the table's
  oldest capture instance).
* `latest`: `sys.fn_cdc_get_max_lsn()` when the query starts; only later commits are read.
* An LSN, such as `0x0000002A000001F00003`: treated as already processed, so the first batch
  reads the commits after it. The `0x` is optional, and fewer than 20 hex digits are padded
  on the left. An LSN that CDC cleanup has already passed fails the first batch with
  `DataLossError`.

`to_delta(bootstrap=True)` sets it to the snapshot's LSN, so passing both is a `ValueError`.
After a re-snapshot (generation 1 and later), `to_delta` replaces it with that snapshot's LSN.

### maxCommitsPerBatch

At most this many commits per micro-batch, counted in `cdc.lsn_time_mapping`. That table is
database-wide: idle entries and other tables' commits count too. A batch always ends on a
commit boundary. Without it every batch reads up to `max_lsn`.

It needs Spark 4.2+, or a runtime with its admission control backported, such as Databricks
Runtime 18.2+ ([Databricks](../DATABRICKS.md)). On older Spark it is ignored. With
`Trigger.AvailableNow` the run reads up to the `max_lsn` it saw when it started, in batches of
at most this many commits.

### numPartitions

How many partitions, each with its own connection to SQL Server, a micro-batch is read in.
The stream cuts a batch into commit-aligned LSN ranges holding about the same number of
change rows ([ADR 0015](../decisions/0015-split-batches-by-change-rows.md)). A snapshot cuts
a single integer key into uniform ranges between its MIN and MAX, any other key into `NTILE`
tiles of the rows, and reads a table without a unique index in one partition.

* `auto`: the cores of the session that called `register()` (`defaultParallelism`;
  `stream()` calls it), else the CPU count of the node that plans (Spark Connect, or without
  `register()`). [ADR 0011](../decisions/0011-num-partitions-from-cores.md).
* A number: set it to cap the load on the source.

### arrowBatchSize

Rows per Arrow record batch fetched from the driver, in the stream and the snapshot. Each
batch is cast to the output schema and handed to Spark as it arrives.

### failOnDataLoss

When the changes the next batch needs are gone (purged by CDC cleanup, or held only by an
older capture instance that was disabled before the stream read them), raise
`DataLossError`. It is checked on the driver before a batch is planned and in every task
after its read, since cleanup can run in between.

`false` skips ahead to what CDC still holds and loses those changes without a trace.
Prefer `to_delta(on_data_loss="resnapshot")`, which recovers with a snapshot and records the
gap: see [Data loss](../guides/data-loss.md).

### schemaChangePolicy

What DDL on the source table inside a batch does.

* `classify`: a captured column whose new type the query's type no longer holds fails the
  batch before it reads anything (`SchemaChangedError`; restart to infer the new type). Other
  DDL goes on, with a warning and a `schema_change` facts row.
* `fail`: any DDL fails the batch. The replayed batch holds the same DDL, so restart once with
  `classify` to go past it.

Anything else is a `ValueError`. See [Schema changes](../guides/schema-changes.md).

### metricsPath

A directory where each partition leaves a JSON file with its metrics (round trip, read time,
MB, network wait, retention watermark, capture lag, the commit time of its last LSN), and
where the reader leaves an event file for each schema change and capture instance switch.
The sink folds them into the batch's facts row and removes them. Without it the metric
columns of the facts table and `end_lsn` stay NULL, and events only reach the driver log.

It must be a path Python can write on every node: local, or a FUSE mount such as a
Databricks Volume, not an object-store URI. A file that cannot be written is skipped: metrics
never fail a read.

* Through `to_delta`, an explicit `metricsPath` holds each stream's files under
  `<metricsPath>/<app_id>`, so streams may share it. Without one, `to_delta` uses
  `<checkpoint>/_mssql_cdc_metrics` (of the live generation) when it has a `facts_table` or
  `snapshot_on_switch=True` and the checkpoint is not a URI. A URI checkpoint (`abfss://`,
  `dbfs:/`) gets no default: set it.
* By hand, pass the same directory to `delta_sink(metrics_path=...)` and use it for one
  stream only: the sink folds every file in it, and nothing else removes them.

What the metrics become: [Monitoring](../guides/monitoring.md) and
[Tables](tables.md#facts).

### snapshotLsn

For `mssql_cdc_snapshot` only: the LSN stamped on the snapshot's rows. By default,
`sys.fn_cdc_get_max_lsn()` recorded before the table is read (or, when capture has not
reached a just-enabled instance yet, the LSN just before its start). `snapshot()` and
`to_delta` set it themselves. Set it only for a snapshot you read yourself, with an LSN
recorded before the read: a row in the snapshot may be newer than the LSN, never older
([ADR 0016](../decisions/0016-bootstrap-snapshot-at-a-recorded-lsn.md)). A chunked snapshot's
waves are stamped this way by `backfill()`, never below the snapshot's own LSN.

### snapshotChunks

For `mssql_cdc_snapshot` only: read only these chunks of the table, one partition each, as
`backfill()` and `reconcile()` do. A JSON list of `[chunk, lo, hi]`: the chunk's number,
its first key (inclusive) and the key it ends before (exclusive). A key is a value for a
one-column key and a list for a composite one, `null` for an open end; a value JSON has no
type for is the text `CAST` reads back (a datetime in ISO 8601, binary as `0x` hex), as
`client.plan_chunks` plans them. With it, the rows gain a `_chunk INT` column with the
chunk's number, and with [metricsPath](#metricspath) each chunk leaves
`chunk-<chunk>.json` (rows, bytes, seconds, `max_lsn` after the read). Key bounds on a table
whose key cannot be read in ranges (no unique index, a type no bound can be bound as) are a
`ValueError`. `backfill()` sets it; set it yourself only to read part of a table.

### snapshotKeys

For `mssql_cdc_snapshot` only: the columns that key bounds ([snapshotChunks](#snapshotchunks),
and the ranges of [numPartitions](#numpartitions)) apply to, as a JSON list such as
`["id"]`. By default, the capture instance's unique index. `reconcile()` sets it to the key
it compares, which can be another column, or the only key of a table without a unique index;
on a column no index leads, each range scans the table.

### isolationLevel

For `mssql_cdc_snapshot` only: `readCommitted` (the default) or `snapshot`, which reads
under SNAPSHOT isolation and needs the database's `ALLOW_SNAPSHOT_ISOLATION`; SQL Server
refuses it otherwise. A SNAPSHOT read does not wait for writers' locks and holds versions
in tempdb while it runs. Anything else is a `ValueError`: a snapshot never reads
uncommitted rows (`NOLOCK`). `backfill(isolation="snapshot")` sets it.

## to_delta parameters

```python
stream(spark, options).to_delta(
    target,
    app_id,
    checkpoint,
    facts_table=None,
    trigger=None,
    query_name=None,
    bootstrap=False,
    on_data_loss="fail",
    resnapshot_interval_days=7.0,
    snapshot_on_switch=False,
    snapshot="full",
)
```

It starts the query and returns its `StreamingQuery`. `stream(spark, options)` registers the
data source on the session first. A guide to all of it: [Streaming](../guides/streaming.md).

### target

The bronze Delta table: a table name such as `bronze.orders`, or a path (anything with a `/`
or a `:`). Created on the first batch with rows ([Tables](tables.md#bronze)).

### app_id

The sink's identity: the Delta `txnAppId` of every append to `target`, with the batch id as
`txnVersion`, so a replayed batch is skipped. Keep it stable for the life of a checkpoint. A
new checkpoint needs a new `app_id`: its batch ids restart at 0 and would be skipped as
already written. Facts rows use `<app_id>#facts` and snapshot events `<app_id>#events`;
generation `n` of a re-snapshot writes as `<app_id>.g<n>`.

### checkpoint

The Spark checkpoint location; generation `n` uses `<checkpoint>/_generations/<n>`. A local
path or a Volume lets `to_delta` place the metrics there and is required by
`on_data_loss="resnapshot"`. A URI (`abfss://`, `dbfs:/`) works for everything else, with
`metricsPath` set by hand. Not `/dbfs/...` with `on_data_loss="resnapshot"`: Spark and Python
resolve it to two different directories.

### facts_table

The facts table, a name or a path: one row per micro-batch, snapshot and source change
([Tables](tables.md#facts)). Streams may share one. `None` writes no facts. Required by
`on_data_loss="resnapshot"`.

### trigger

Keyword arguments for `DataStreamWriter.trigger`, such as `{"availableNow": True}` or
`{"processingTime": "1 minute"}`. `None` is Spark's default: the next batch as soon as the
previous one ends and the source has moved. Every batch writes a facts row, so a
`processingTime` trigger bounds how many a quiet stream writes
([Monitoring](../guides/monitoring.md)).

### query_name

Passed to `DataStreamWriter.queryName`.

### bootstrap

`True` appends a snapshot of the source table to `target` before the first batch (once:
later runs find it, under any capture instance of the table) and starts the checkpoint at
its LSN, so the target holds the whole table, not only what CDC retention still has. With a
`facts_table` it also writes a `bootstrap` facts row. Not with `startingLsn` (`ValueError`).
On a table too big to snapshot within the retention, with `snapshot="chunked"`, or after
[seed](#seed-parameters): it then finds the seed and reads nothing
([Bootstrap](../guides/bootstrap.md#tables-too-big-to-snapshot)).

### on_data_loss

* `"fail"`: when CDC cleanup purged changes the stream has not read, the query stops with
  `DataLossError`.
* `"resnapshot"`: before the query starts, check whether that has happened; if so, snapshot
  the table into `target` again, write a `resnapshot` facts row with the gap, and continue
  in a new generation. Requires `facts_table` and a local or Volume `checkpoint`; run one job
  per stream. A purge while the query runs still fails it, and the next run recovers.

Anything else is a `ValueError`. See [Data loss](../guides/data-loss.md).

### resnapshot_interval_days

With `on_data_loss="resnapshot"`: at most one automatic re-snapshot, or failed attempt, per
this many days. A second loss inside the interval raises `DataLossError` instead, since the
stream does not keep up with the retention. Keep it above the CDC retention (3 days by
default); `0` allows a retry right away.

### snapshot_on_switch

`True` appends a snapshot of the table after the batch that first reads a newer capture
instance, so that rows unchanged since the switch carry the columns only the newer instance
captures instead of NULL. It reads the whole table. With a URI checkpoint it needs
`metricsPath` (`ValueError` otherwise). See
[Schema changes](../guides/schema-changes.md#rows-unchanged-since-the-switch).

### snapshot

How `bootstrap=True` and `on_data_loss="resnapshot"` take a snapshot.

* `"full"`: read the whole table before the query starts, as above.
* `"chunked"`: only open one: record its LSN S and the key's extent in a `snapshot_open`
  facts row, then start the stream (the new generation, after a loss) at S at once.
  [backfill()](#backfill-parameters), run in a task of its own, plans the chunks, reads the
  table in chunks next to the stream and writes the `bootstrap` or `resnapshot` row at the
  end. Requires `facts_table` (`ValueError`). A rerun finds the snapshot it opened and opens
  no second one.

Anything else is a `ValueError`. The mode holds for the whole run, its bootstrap and its
re-snapshot, and may change between runs. With a `facts_table`, both modes record the
snapshot as open before reading, and a run in one mode raises `ValueError` while a snapshot
of the other mode is still open, saying how to finish it; a full one stops counting once
CDC cleanup passes its LSN, as it can then never complete
([One mode per run](../guides/bootstrap.md#one-mode-per-run)). See
[Bootstrap](../guides/bootstrap.md#chunked-snapshots) and
[ADR 0028](../decisions/0028-chunked-snapshot-next-to-the-stream.md).

## backfill parameters

```python
status = stream(spark, options).backfill(
    "bronze.orders",
    app_id="orders-v1",
    facts_table="ops.ingestion_facts",
)
# {"snapshot": "0x...", "chunks_done": 8, "chunks_total": 40, "done": False,
#  "paused": False, "reason": None}
```

`backfill(target, *, app_id, facts_table, chunk_rows=None, max_waves=None,
max_seconds=None, min_headroom_hours=None, isolation=None)` reads the newest chunked
snapshot that `to_delta(..., snapshot="chunked")` opened for `target`, in waves of
[numPartitions](#numpartitions) chunks, and returns how far it got. Call it again until
`done`.

* `target`, `app_id`, `facts_table`: as passed to `to_delta`; the stream's generations
  (`<app_id>.g<n>`) are found from `app_id`. With a full snapshot of the stream still
  open it raises `ValueError`, before anything else
  ([One mode per run](../guides/bootstrap.md#one-mode-per-run)). Otherwise, without an open
  chunked snapshot it returns at once, `paused` with a `reason`.
* `chunk_rows`: the most rows a chunk holds when planned (at least 1; `None` is
  1,000,000). It counts on the first call only, which plans every chunk and records the plan
  in a `snapshot_plan` facts row; later calls keep the plan's value and log a warning when
  given another. One integer key is counted per slice on the server and the slices packed
  into chunks of at most `chunk_rows` rows, each but the last short of it by less than one
  slice (about a sixteenth of `chunk_rows` on an even key); other keys are cut every
  `chunk_rows` keys. The last chunk ends just above the MAX recorded at the open (for other
  keys at the first key after it, looked for again before each wave while there is none):
  rows inserted above it come from the stream
  ([How chunks are sized](../guides/bootstrap.md#how-chunks-are-sized)).
* `max_waves`, `max_seconds`: stop after that many waves, or before a wave once that many
  seconds have passed. `None`: until the snapshot is done.
* `min_headroom_hours`: before each wave, pause while the stream's newest facts row has
  less retention headroom (less that row's age), or there is none. `None`: never pause.
* `isolation`: `"snapshot"` sets [isolationLevel](#isolationlevel); `None` reads READ
  COMMITTED.

The result: `snapshot` (its LSN S), `chunks_done`, `chunks_total` (the plan's count, `None`
until a call has planned it), `done`, and `paused` with its `reason`.
Each wave is one commit to `target` and one `snapshot_chunk` facts row per chunk
([Tables](tables.md#facts)).

## snapshot parameters

```python
offset = stream(spark, options).snapshot("bronze.orders")
# {"lsn": "0x...", "commit_ts": "2026-09-28T14:03:12.117"}
```

`snapshot(target, resnapshot=False, *, app_id=None, facts_table=None)` appends the source
table's current rows to `target` as operation 0 and returns the offset they are stamped
with, to start a stream from ([startingLsn](#startinglsn)). When `target` already holds a
snapshot of the table, it returns that one's offset and reads nothing. `resnapshot=True`
always takes a new one. With `facts_table` (and the stream's `app_id`, else `ValueError`), a
chunked snapshot of that stream still open in `target` raises `ValueError`: this one is
full ([One mode per run](../guides/bootstrap.md#one-mode-per-run)).
`to_delta(bootstrap=True)` and `on_data_loss="resnapshot"` call it for you.

## seed parameters

```python
offset = stream(spark, options).seed(
    "bronze.orders", copy_df, as_of, app_id="orders-v1", facts_table="ops.ingestion_facts"
)
```

`seed(target, df, as_of, *, app_id=None, facts_table=None, allow_missing_columns=False,
reseed=False)` appends `df`, a copy of the table you already have, to `target` as its
snapshot, for a table too big to snapshot within the CDC retention; `to_delta(bootstrap=True)`
then starts from it.

* `as_of`: when the copy started being read, as a UTC `datetime` (an aware one is converted),
  or an LSN recorded before that. Every commit at or before it must be in the copy.
* `app_id`, `facts_table`: with a facts table, the stream's `app_id`; the seed writes the
  `bootstrap` facts row `to_delta` would, and raises `ValueError` while a snapshot of that
  stream, full or chunked, is open in `target`.
* `allow_missing_columns`: a captured column `df` lacks reads NULL instead of raising
  `ValueError`.
* `reseed`: append a copy newer than the snapshot already in `target`, after a
  `DataLossError`; then start a new checkpoint and `app_id`.

A rerun with the same `as_of` returns the seed already there. See
[Bootstrap](../guides/bootstrap.md#tables-too-big-to-snapshot) and
[`CdcStream.seed`](api.md#mssql_cdc.pipeline.CdcStream.seed).
