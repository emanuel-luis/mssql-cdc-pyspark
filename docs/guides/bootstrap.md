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

Seed the target from an existing copy of the table instead, and start the stream at the LSN
that copy is consistent with. Record it on SQL Server before the copy starts, as the
snapshot does:

```sql
SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1) AS lsn;  -- 0x0000002A000001F40003
```

```python
query = stream(spark, {**options, "startingLsn": "0x0000002A000001F40003"}).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/checkpoints/orders",
    facts_table="ops.ingestion_facts",
)
```

If cleanup has already passed that LSN, the stream's retention guard raises
`DataLossError` rather than skipping the gap. Never use `bootstrap=True` or
`on_data_loss="resnapshot"` on such a table: both snapshot it.

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
- [`CdcStream.snapshot`](../reference/api.md#mssql_cdc.pipeline.CdcStream.snapshot) in the
  API reference.
