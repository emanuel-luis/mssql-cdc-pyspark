# Silver tables

Bronze is a change log: one row per change, an update as two rows, a snapshot that overlaps
the stream. Most consumers want the source table as it is now, one row per key. Write it
with `apply_changes`: it keeps a silver Delta table equal to the source table, from the
bronze rows, with deletes applied and its own completeness verdict.

## Smallest example

Run it after the stream, in the same job or a separate one:

```python
from mssql_cdc import apply_changes, finalization, stream

query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/data/checkpoints/orders",
    facts_table="ops.ingestion_facts",
    trigger={"availableNow": True},
    bootstrap=True,
)
query.awaitTermination()
end = finalization.end_offset_from_progress(query.lastProgress)
finalization.advance(spark, "ops.table_finalization", "bronze.orders", end)

result = apply_changes(
    spark,
    "bronze.orders",  # the same string passed to to_delta and advance
    "silver.orders",
    "dbo_orders",
    ["order_id"],
    control_table="ops.table_finalization",
    facts_table="ops.ingestion_facts",
)
# {"rebuilt": False, "applied_lsn": "0x...", "finalized_until": datetime(...),
#  "bronze_found": True}
```

The result says whether this call rebuilt silver from a snapshot (`rebuilt`), how far
bronze is now applied (`applied_lsn`), silver's verdict (`finalized_until`) and whether
bronze exists (`bronze_found`). The first call that finds bronze counts as a rebuild.

To read the key from the capture instance's unique index instead of naming it, leave out
`keys` and pass the stream's options:

```python
apply_changes(
    spark,
    "bronze.orders",
    "silver.orders",
    "dbo_orders",
    control_table="ops.table_finalization",
    facts_table="ops.ingestion_facts",
    options=options,
)
```

A capture instance without a unique index fails with `ValueError` and asks for `keys`.

## How it behaves

Each call reads the capture instance's bronze rows beyond the last call and applies them
with one Delta MERGE ([ADR 0019](../decisions/0019-silver-helper-applies-the-change-log.md)):

- Per key, the latest image by `(_start_lsn, _command_id, _seqval, _operation)`, or
  `(_start_lsn, _seqval, _operation)` when bronze has no `_command_id`
  ([includeCommandId=false](../reference/options.md#includecommandid)): the order within a
  transaction then follows `__$seqval`, which Microsoft's documentation says not to order
  by. Operation 1
  deletes the key, and 0 (snapshot), 2 and 4 upsert it. Operation 3 (the row before an
  update) deletes its own key: the 4 of the same update outranks it, so it is the latest row
  only when the update moved the row to another key. Keys match with null-safe equality,
  since a unique index admits one NULL.
- Silver has the captured columns plus `_start_lsn` and `_commit_ts` of each row's current
  image. Deletes remove the row; bronze keeps the history. The column comments are in
  [Tables](../reference/tables.md).
- The position is `applied_lsn` in the control table, written after the MERGE. A rerun, or
  a call after a crash between the two, applies nothing twice and brings back no deleted
  row: a row only takes an image newer than its own `_start_lsn`.
- When bronze holds a snapshot newer than the one silver was last rebuilt from
  (`snapshot_lsn` in the control table), silver is rebuilt from it: keys in neither the
  snapshot nor the changes after it are deleted. That happens after `bootstrap=True` on an
  existing stream and after a re-snapshot that followed data loss, where the deletes of the
  purged gap never reached bronze ([Data loss](data-loss.md)).
- Silver's `finalized_until` is the bronze verdict as it stood before the call read bronze,
  truncated with `granularity` (`"hour"` by default). Bronze commits its rows before its
  verdict, so silver never claims more than it has applied. Without `facts_table` it is
  never advanced, and a warning says so (once per silver table in a process): a chunked
  snapshot shows in the facts alone until its first wave lands. Gate silver consumers on
  the silver name, as in [Finalization](finalization.md).
- Until the stream has created bronze, a call does nothing and returns the previous
  position and verdict, with `bronze_found` false and a warning naming the table. A result
  that keeps saying so is a wrong name, or arguments swapped, not a slow stream.

Check how far bronze and silver are with one query on the control table:

```sql
SELECT table_name, finalized_until, end_lsn, applied_lsn, snapshot_lsn
FROM ops.table_finalization
WHERE table_name IN ('bronze.orders', 'silver.orders');
```

### Chunked snapshots

A [chunked snapshot](bootstrap.md#chunked-snapshots) arrives in waves over days, so pass
`facts_table`: a call that finds chunk rows without it raises `ValueError`.

- While a chunked snapshot is open, a bootstrap or a re-snapshot, each call applies the
  waves that arrived since the last one, tracked by `open_snapshot_lsn` and `snapshot_wave`
  in the control table. A chunk row is ranked with every later change of its key in bronze,
  so it never brings back a key the stream deleted after the chunk's stamp.
- Each wave also removes the keys its chunks prove gone (range deletes, below).
- At the snapshot's `bootstrap` or `resnapshot` row, silver is rebuilt from its rows and
  the changes after S, and every key absent from both is deleted, whatever the key type.
- Silver's `finalized_until` stays where it was while a snapshot is open: silver lacks
  keys, or still holds deleted ones, until the rebuild.

#### Range deletes

A silver table built before a chunked re-snapshot holds keys that were deleted in the purged
gap, and no delete row for them ever reaches bronze. Each wave removes the ones in its
chunks' ranges: a chunk stamped L saw every commit up to L, so a silver key inside its range
`[lo, hi)` that the chunk does not hold, and whose image is older than L, was gone by L. The
call deletes it as a change at L, ranked with the rest, so a later change of the key in
bronze still wins. The gap's deleted keys leave silver wave by wave instead of at the
completion, which on a large table can be weeks later.

- Which keys: silver's key must be the snapshot's own (the key its chunks are cut on, the
  table's unique index) and a single column of an integer type, `date`, or a timestamp
  (`datetime2`, `datetime`, `smalldatetime`), which Spark orders as SQL Server does. A
  string key (SQL Server orders it by its collation, Spark by bytes), a composite key or
  another type gets no range deletes: its stale keys go at the completion's rebuild.
- Integer bounds are compared as `BIGINT`; the last chunk's end, MAX + 1, counts as open
  when it does not fit (a `BIGINT` key at its maximum).
- The microsecond rule: SQL Server keeps a `datetime2(7)` value to 100 ns, while Spark,
  through the drivers, holds the key to the microsecond, truncated. The plan writes its
  bounds to the microsecond, so they fall between two microseconds and every key lands on
  the same side of them in both. A bound with a seventh digit would not: Spark could not
  tell on which side the keys of that microsecond fall in SQL Server, so no chunk deletes
  them (a lower bound moves up to the next microsecond, an upper one is truncated), and
  those keys, if stale, go at the rebuild.

The same applies during a chunked bootstrap, where a silver table that is new at the open
has no stale keys to remove.

### After a switch to a new capture instance

When the stream moves to a newer capture instance of the table, bronze holds rows of both
([Schema changes](schema-changes.md)). Pass `options` so `apply_changes` asks SQL Server
which instances belong to the table. Without it, a bronze row of another instance fails the
call with `ValueError` instead of being skipped. A column that bronze gained is added to
silver, and rows that have not changed since read NULL for it.

## Pitfalls

- **Use one name for bronze everywhere.** `apply_changes` finds the bronze verdict and the
  snapshot events by the exact string passed as `bronze`: advance bronze's verdict under
  that name, and write the stream to that same target, so the facts `target` matches.
  `bronze.orders` and the table's storage path are two different keys.
- One bronze table per source table. The snapshot events carry no capture instance, so a
  bronze table shared by two source tables would rebuild one from the other's snapshot.
- Pass `facts_table`, the stream's; a stream without one still needs a name here, where
  no table will appear. A re-snapshot of a table that was empty writes no bronze rows, only
  its facts event, and a chunked snapshot opens in the facts alone: without them, silver
  would keep the rows that the table lost, or claim periods the snapshot has not reached.
  So silver's verdict only advances with `facts_table`.
- One call per silver table at a time, as with one job per stream.
- `apply_changes` adds columns but never changes a column's type: its MERGE runs without
  schema evolution, so `delta.enableTypeWidening` alone leaves silver at the old type. After
  a type widening on the source, widen silver's column by hand
  ([Schema changes](schema-changes.md#changing-a-column-type)). No test covers silver
  through a widening yet, so compare silver's column types with bronze's after one.
- After the old capture instance is dropped and `captureInstance` is renamed to the new
  one, SQL Server no longer lists the old name, and a silver table built from scratch fails
  on the old rows. Keep the old (default) name configured: the stream follows the table's
  newest instance anyway.
- The MERGE joins against the whole silver table, and each call scans bronze for snapshot
  rows. Delta file statistics skip most of that scan, but it grows with bronze.

## See also

- [Bootstrap](bootstrap.md): the snapshot silver is first built from.
- [Finalization](finalization.md): gating consumers on silver's verdict.
- [Validation](validation.md): compare silver with the source table.
- [API reference](../reference/api.md#mssql_cdc.apply_changes) for every parameter.
- [ADR 0019](../decisions/0019-silver-helper-applies-the-change-log.md): why a batch call,
  hard deletes and a position in the control table.
