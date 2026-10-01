# Data loss and re-snapshots

CDC cleanup deletes change rows by age, whether anyone has read them or not. The retention
is three days by default. A stream that is stopped, or behind, for longer than that finds
its next changes gone. This page covers how the stream detects that, how to see it coming,
and how to recover, by hand or on its own.

## See it coming

With a facts table, every micro-batch records `retention_headroom_hours`: how far the
stream's position is ahead of what cleanup has already deleted. A current stream sits near
the retention period; the value falls as the stream falls behind, and at 0 the next changes
are being purged. Alert on it well before 0, and on facts that stop arriving, since a
stopped stream keeps its last value while the real headroom shrinks. The queries are in
[Monitoring](monitoring.md); the reasoning in
[ADR 0017](../decisions/0017-retention-headroom-in-facts.md).

## What the stream does

The retention guard runs twice for every range a batch reads:

- on the driver, while planning: if the first LSN to read is already below
  `sys.fn_cdc_get_min_lsn()` of the capture instance;
- on the executor, after reading: if `min_lsn` moved past the range's start while it was
  read. The change table returns whatever is left without an error, so this check is what
  catches a cleanup that ran mid-read.

Either way it raises `DataLossError`:

```text
dbo_orders: change data from 0x0000002A000001F40003 is gone (min_lsn is now
0x0000003100000A280001): purged by CDC cleanup. A re-snapshot is required.
Set failOnDataLoss=false to skip ahead (loses changes).
```

The query stops, and `awaitTermination()` raises Spark's streaming query exception with
that message. A rerun fails the same way: the checkpoint still points before the gap. When
an older capture instance of the table was disabled before the stream had read its changes,
the message names it as the other possible cause ([Schema changes](schema-changes.md)).

The option `failOnDataLoss=false` skips ahead to what cleanup left instead.
**Nothing records the skipped changes**: no error, no facts row. Use it only where losing
them is acceptable and a snapshot is not.

## Recovering by hand

Take a new snapshot, start a new checkpoint with a new `app_id` from its LSN, and rebuild
downstream from that snapshot:

```python
from mssql_cdc import stream

offset = stream(spark, options).snapshot("bronze.orders", resnapshot=True)

query = stream(spark, {**options, "startingLsn": offset["lsn"]}).to_delta(
    "bronze.orders",
    app_id="orders-v2",
    checkpoint="/checkpoints/orders-v2",
    facts_table="ops.ingestion_facts",
)
```

The new `app_id` matters: batch ids restart at 0 with a new checkpoint, and Delta would skip
them under the old one ([Streaming](streaming.md#app_id)).

For a table too big to snapshot, seed a newer copy instead,
`stream(spark, options).seed("bronze.orders", copy, as_of, reseed=True)`, and start from the
offset it returns the same way ([Bootstrap](bootstrap.md#tables-too-big-to-snapshot)).

## Recovering automatically

`on_data_loss="resnapshot"` does all of that before the query starts:

```python
query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/Volumes/main/ops/checkpoints/orders",
    facts_table="ops.ingestion_facts",
    trigger={"availableNow": True},
    bootstrap=True,
    on_data_loss="resnapshot",
    resnapshot_interval_days=7,
)
query.awaitTermination()
```

On every call, `to_delta` reads the last offset the checkpoint processed. When CDC has
captured anything after it and the next changes are purged, it:

1. records where the stream was, so that a crash mid-recovery resumes from the same point;
2. takes a new snapshot of the table into the target, stamped with the LSN recorded before
   the read ([Bootstrap](bootstrap.md#how-it-behaves));
3. writes a facts row with `event = 'resnapshot'` and the gap;
4. moves the stream to a new [generation](#generations) that starts at the snapshot's LSN.

Then it starts the query and returns it, as usual. When nothing is purged it only starts the
query. The design, with the alternatives considered, is in
[ADR 0018](../decisions/0018-automatic-resnapshot-after-data-loss.md).

It needs:

- a `facts_table`, where the event rows tell downstream to rebuild (a `ValueError`
  without one);
- a checkpoint path that Python and Spark resolve to the same directory: local, or a Volume.
  A URI (`dbfs:/`, `abfss://`) or a `/dbfs/...` path is a `ValueError`;
- one job per stream: there is no lock, and two recoveries at once waste a snapshot.

The check runs only before the query starts. A purge while the query runs still fails it
with `DataLossError`, and the next run recovers: give the job a retry, or let the next
scheduled run do it.

## Generations

A generation is a checkpoint and an `app_id` that belong together. Generation 0 is the pair
you pass; after the n-th re-snapshot the stream runs as generation n, all under the
checkpoint you passed:

```text
<checkpoint>/
  offsets/ commits/ ...          generation 0 (Spark's files)
  _mssql_cdc_metrics/            generation 0 metrics
  _mssql_cdc_generation.json     which generation is live, and its snapshot LSN
  _generations/<n>/              generation n: Spark checkpoint and metrics
```

Generation n writes with the `app_id` `<app_id>.g<n>`, so its batch ids, which restart at 0,
are not mistaken for the old ones. Keep passing the same `checkpoint` and `app_id` on every
run: `to_delta` reads the state file on every call, whatever `on_data_loss` says, and picks
the live generation. In generation n it starts at the snapshot and ignores `startingLsn`.
Earlier generations' files stay in place, unread. More in
[Architecture](../ARCHITECTURE.md#generations-to_delta).

## The events in the facts

The bootstrap and every re-snapshot leave one facts row with `event` set and no `batch_id`:
`'bootstrap'` for the first, `'resnapshot'` for each recovery. On those rows `min_lsn`,
`max_lsn` and `end_lsn` are the snapshot's LSN and `rows` is the snapshot's row count (0
for an empty table, whose snapshot writes nothing else; NULL when an interrupted recovery's
snapshot was reused). A `'resnapshot'` row also carries the gap:

- `lost_from_ts`: commit time of the last offset the stream had processed, NULL when it had
  committed nothing since an explicit `startingLsn`;
- `lost_to_ts`: commit time of the retention watermark when the loss was found, where the
  gap ends.

```sql
SELECT written_at, app_id, rows, lost_from_ts, lost_to_ts
FROM ops.ingestion_facts
WHERE target = 'bronze.orders' AND event = 'resnapshot'
ORDER BY written_at DESC
```

Statistics over micro-batches filter on `event IS NULL`. The other events,
`'schema_change'` and `'capture_instance_switched'`, are not snapshots.

## Downstream after a re-snapshot

The changes committed between `lost_from_ts` and `lost_to_ts` are gone for good. The
snapshot restores the current rows, not the versions in between, and a row deleted during
the gap has no delete row. Downstream rebuilds from the newest snapshot, then applies the
changes after it. The newest snapshot's LSN is the higher of the target's operation-0 rows
and the facts' snapshot events, because a snapshot of an empty table writes no rows:

```sql
SELECT greatest(
  (SELECT max(_start_lsn) FROM bronze.orders WHERE _operation = 0),
  (SELECT max(max_lsn) FROM ops.ingestion_facts
   WHERE target = 'bronze.orders' AND event IN ('bootstrap', 'resnapshot'))
) AS snapshot_lsn
```

[`apply_changes`](silver.md) does this on its own when given the facts table.
`finalized_until` stays monotonic across generations; over the gap it still means that
nothing more will arrive, not that the gap's changes are in the target.

## The interval between re-snapshots

`resnapshot_interval_days` (default 7) allows one automatic re-snapshot per interval. A
second loss within it raises `DataLossError` instead: the stream does not keep up with the
retention, or keeps stopping for longer than it, and a person has to decide. Keep the
interval above the CDC retention, which the reader cannot see. To re-snapshot now anyway,
fix the cause and rerun with `resnapshot_interval_days=0`.

A re-snapshot whose own LSN is purged before the read ends (the table takes longer to read
than the retention) raises `DataLossError` too, and counts as a failed attempt for the
interval. Lengthen the retention (`sys.sp_cdc_change_job` with `@job_type = N'cleanup'`) or
speed up the read (`numPartitions`), then rerun with `resnapshot_interval_days=0`.

## Pitfalls

- A table too big to read within the retention can never be recovered this way: see
  [Tables too big to snapshot](bootstrap.md#tables-too-big-to-snapshot), and leave
  `on_data_loss` at `"fail"` for it.
- Disabling an old capture instance before the stream has read past the new one's start
  loses the changes only the old one held. Follow the switch procedure in
  [Schema changes](schema-changes.md).
- Re-snapshots hide a stream that cannot keep up. Watch the `'resnapshot'` rows as well as
  the headroom.

## See also

- [Monitoring](monitoring.md): headroom and liveness alerts.
- [Bootstrap](bootstrap.md): what a snapshot reads and writes.
- [Silver tables](silver.md): rebuilding the current state after a re-snapshot.
- [`DataLossError`](../reference/api.md#mssql_cdc.DataLossError) in the API reference.
