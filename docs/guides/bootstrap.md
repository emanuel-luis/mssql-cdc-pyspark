# Bootstrap

A stream that starts from `earliest` reads what CDC retention still holds, three days by
default. Rows written before CDC was enabled, or not changed within that window, never
arrive. A bootstrap loads the whole table once, with a snapshot that meets the stream at an
exact LSN: no gap, no overlap that a MERGE cannot absorb, and no lock on the source.

## The smallest bootstrap

```python
from mssql_cdc import stream

query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/checkpoints/orders",
    facts_table="ops.ingestion_facts",
    trigger={"availableNow": True},
    bootstrap=True,
)
query.awaitTermination()
```

Keep `bootstrap=True` on every run. The first run takes the snapshot; later runs find it in
the target and read nothing again.

## How it behaves

1. Before reading anything, it records the LSN `L`: `sys.fn_cdc_get_max_lsn()`, or the
   capture instance's first LSN minus one when capture has not reached a new instance yet.
   Every commit up to `L` is already in the table when the read starts.
2. It reads the table's current rows in the stream's schema, READ COMMITTED and never with
   `NOLOCK`. Each row has `_operation = 0`, `_start_lsn = L` and `_commit_ts` the commit
   time of `L`; `_seqval`, `_command_id` and `_batch_id` are NULL.
3. It appends them to the target in one Delta commit and, with a facts table, writes a
   facts row with `event = 'bootstrap'`, the snapshot's row count and how long it took.
4. The stream starts a new checkpoint at `L` and reads every commit after it.

A commit that lands while the table is read can appear twice: in the snapshot, and as a
change after `L`. Keeping the latest image per key, ordered by
`(_start_lsn, _command_id, _seqval, _operation)`, absorbs that, because every snapshot row
sorts before the changes read after it. [`apply_changes`](silver.md) does exactly this;
other consumers of bronze must deduplicate the same way, as they already must for updates.

On a rerun, the snapshot is found by the target's operation-0 rows under any capture
instance of the table, the name matched ignoring case. Its LSN is returned and nothing is
read, so a rerun can never skip the changes between the snapshot and a newer LSN. A
checkpoint that already has offsets resumes from them. The `bootstrap` facts row is written
once. The reasoning is in [ADR 0016](../decisions/0016-bootstrap-snapshot-at-a-recorded-lsn.md).

The read is split across `numPartitions` connections:

- a single integer key column: uniform ranges between its minimum and maximum (sparse or
  skewed keys give uneven ranges);
- a composite or non-integer key: tiles of the rows computed on the server with `NTILE`,
  which reads the whole key once;
- no unique index, or a key the stream cannot split on: one partition.

The snapshot needs no permission beyond the stream's ([Permissions](permissions.md)).

## A snapshot without the stream

`snapshot(target)` takes the snapshot alone, or finds the one already in the target, and
returns its offset. Start a new checkpoint from it:

```python
from mssql_cdc import stream

offset = stream(spark, options).snapshot("bronze.orders")
# {"lsn": "0x...", "commit_ts": "2026-10-01T12:00:00.123"}

query = stream(spark, {**options, "startingLsn": offset["lsn"]}).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/checkpoints/orders",
    facts_table="ops.ingestion_facts",
)
```

Done this way there is no `bootstrap` facts row: only `to_delta` writes it.
`snapshot(target, resnapshot=True)` takes a new snapshot even when the target has one, for
a manual recovery ([Data loss](data-loss.md#recovering-by-hand)).

To write the snapshot somewhere else, read it as a batch source:

```python
from mssql_cdc import register

register(spark)
rows = spark.read.format("mssql_cdc_snapshot").options(**options).load()
rows.write.format("parquet").save("/data/snapshots/orders")
```

Every action on `rows` reads the table again, stamped with the LSN recorded for that read:
write it once and take the LSN from the rows' `_start_lsn`. The `snapshotLsn` option stamps
an LSN you recorded yourself instead, before the read started.

## Tables too big to snapshot

A snapshot has to finish within the CDC retention, or the changes after its LSN are purged
before the stream reads them and the stream stops with `DataLossError`. At the 4,000 to
11,000 rows per second measured against a production source, the default three days hold
roughly 1 to 3 billion rows: a table of billions of rows cannot be snapshotted in time.

Seed the target from a copy you already have instead, an existing lake table for instance,
and let the stream start where the copy ends:

```python
from datetime import datetime, timezone

from mssql_cdc import stream

orders = stream(spark, options)
orders.seed(
    "bronze.orders",
    spark.table("lake.orders"),
    as_of=datetime(2026, 9, 30, 2, 0, tzinfo=timezone.utc),  # when the copy started
    app_id="orders-v1",
    facts_table="ops.ingestion_facts",
)

query = orders.to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/checkpoints/orders",
    facts_table="ops.ingestion_facts",
    bootstrap=True,  # finds the seed: never reads the table
)
```

`as_of` says where the copy ends: every commit at or before it must be in the copy. Later
commits may be in it too; the stream replays them, and keeping the latest image per key
absorbs the overlap, as after a snapshot.

- A `datetime` is the time the copy started being read, in UTC (an aware one is
  converted). It maps to the last commit at or before it in `cdc.lsn_time_mapping`, on
  SQL Server's clock (`sourceTimeZone`), to the second. Taken on another machine, subtract
  the clock difference: earlier is always safe, it only replays more; later loses the
  commits in between.
- An LSN recorded on SQL Server before the copy started is exact:
  `SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)`.

The copy is appended to the target in one Delta commit as snapshot rows: `_operation = 0`,
`_start_lsn` the LSN of `as_of`, `_commit_ts` its commit time, `_capture_instance` the
configured one. Its columns match the captured columns by name, ignoring case; other
columns are dropped and each value is cast to the stream's type. A captured column the copy
lacks is a `ValueError`; `allow_missing_columns=True` writes it as NULL instead. With a
facts table and the stream's `app_id`, the seed writes the `bootstrap` facts row, with the
copy's row count, and `to_delta(bootstrap=True)` writes no second one.

Nothing is written when:

- CDC no longer holds the changes right after `as_of` (cleanup passed it, or no commit is
  that old): `DataLossError`, the copy is older than the retention. Seed a newer copy.
- the target already holds a snapshot of the table at another LSN: `ValueError`. A rerun
  with the same `as_of` returns the seed already there, so the call can stay in the job.

Never use `on_data_loss="resnapshot"` on such a table: it snapshots it. After a
`DataLossError`, seed a newer copy with `reseed=True`, then start a new checkpoint and
`app_id` from it ([Data loss](data-loss.md#recovering-by-hand)). The reasoning is in
[ADR 0025](../decisions/0025-seed-from-an-existing-copy.md).

## Pitfalls

- `bootstrap=True` sets `startingLsn` itself: passing both is a `ValueError`.
- Do not delete the snapshot rows (`_operation = 0`) from bronze. The next run with
  `bootstrap=True` would find none and read the whole table again.
- The snapshot reads the source table, not CDC: it puts a full scan's load on SQL Server.
  Cap `numPartitions` on a busy server.

## See also

- [Data loss and re-snapshots](data-loss.md): snapshots taken after CDC cleanup purged
  unread changes.
- [Silver tables](silver.md): the current state from the snapshot and the changes.
- [`CdcStream.snapshot`](../reference/api.md#mssql_cdc.pipeline.CdcStream.snapshot) and
  [`CdcStream.seed`](../reference/api.md#mssql_cdc.pipeline.CdcStream.seed) in the API
  reference.
