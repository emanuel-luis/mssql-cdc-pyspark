# Bootstrap

A stream that starts from `earliest` reads what CDC retention still holds, three days by
default. Rows written before CDC was enabled, or not changed within that window, never
arrive. A bootstrap loads the whole table once, with a snapshot that meets the stream at an
exact LSN: no gap, no overlap that a MERGE cannot absorb, and no lock on the source.

## Which snapshot

| | Full (the default) | [Chunked](#chunked-snapshots) | [Seed](#tables-too-big-to-snapshot) |
|---|---|---|---|
| What reads the table | `to_delta`, before the stream starts | `backfill()`, in waves, next to the running stream | nothing: you have a copy |
| Has to finish within | the CDC retention | the time the stream runs without a gap | (the copy's own load) |
| Progress | one Delta commit at the end | one commit and facts rows per wave | one commit |
| Needs | nothing more | a `facts_table`, a second task | a copy and when it started |
| For | tables read in hours | tables the link needs days or weeks to read | very large tables with a copy |

Size a snapshot by its data, not its rows: what limits the read is the link to SQL Server.
Runs against a production source moved 1.8 to 5.9 MB/s over four connections, which was
18,000 to 105,000 rows per second depending on the row width. Divide the table's size by
that and compare with the retention.

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
   Every commit up to `L` is already in the table when the read starts. With a facts table
   it first writes a `snapshot_open` facts row in mode `full`, which keeps a chunked run
   from opening another snapshot while this one is read ([One mode per run](#one-mode-per-run)).
2. It reads the table's current rows in the stream's schema, READ COMMITTED and never with
   `NOLOCK`. Each row has `_operation = 0`, `_start_lsn = L` and `_commit_ts` the commit
   time of `L`; `_seqval`, `_command_id` and `_batch_id` are NULL.
3. It appends them to the target in one Delta commit and, with a facts table, writes a
   facts row with `event = 'bootstrap'`, the snapshot's row count and how long it took.
4. The stream starts a new checkpoint at `L` and reads every commit after it. Its first
   batch holds everything committed while the table was read, up to `max_lsn`, cached whole
   by the sink: on a snapshot that takes hours, set
   [maxCommitsPerBatch](../reference/options.md#maxcommitsperbatch) to keep it bounded.

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
returns its offset (an [`Offset`](../reference/api.md#mssql_cdc.types.Offset)). Start a new
checkpoint from it:

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

## Chunked snapshots

A full snapshot has to finish within the CDC retention, or the changes after its LSN are
purged before the stream reads them. A chunked snapshot starts the stream first, at an LSN
S, and reads the table in chunks next to it, so it only needs the stream to keep running.
Run the stream as usual, with `snapshot="chunked"`:

```python
from mssql_cdc import stream

orders = stream(spark, options)
query = orders.to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/Volumes/main/ops/checkpoints/orders",
    facts_table="ops.ingestion_facts",  # required: its rows hold the snapshot's state
    trigger={"processingTime": "1 minute"},
    bootstrap=True,
    snapshot="chunked",
)
```

and, in a task of its own, call `backfill()` until it is done:

```python
import time

grace = time.monotonic() + 3600  # the stream's first run may not have opened it yet
while True:
    status = orders.backfill(
        "bronze.orders",
        app_id="orders-v1",
        facts_table="ops.ingestion_facts",
        chunk_rows=1_000_000,
        max_seconds=3600,  # one call's budget; the next call goes on
        min_headroom_hours=24,
    )
    state = status["state"]
    if state == "done":
        break
    if state in ("waiting_headroom", "waiting_metrics"):
        time.sleep(600)  # status["reason"] says why
    elif state == "no_snapshot":
        if time.monotonic() > grace:  # a wrong target or app_id, or snapshot="full"
            raise RuntimeError(status["reason"])
        time.sleep(60)
    # "running": the call's budget ran out; go on
```

`state` is one of `done`, `running`, `waiting_headroom`, `waiting_metrics` and
`no_snapshot` ([backfill parameters](../reference/options.md#backfill-parameters)); the
result is a [`BackfillStatus`](../reference/api.md#mssql_cdc.types.BackfillStatus).

How it behaves ([ADR 0028](../decisions/0028-chunked-snapshot-next-to-the-stream.md)):

1. The first `to_delta` records S, `sys.fn_cdc_get_max_lsn()` as for a full snapshot, then
   the key's MIN and MAX, and writes them in a `snapshot_open` facts row (mode `chunked`). It
   reads nothing else from the table and starts the stream at S at once. Later runs find
   that row and start from S again until the checkpoint has offsets.
2. The first `backfill()` call plans every chunk ([below](#how-chunks-are-sized)) and records
   the plan in a `snapshot_plan` facts row. Each call then reads the next chunks of the plan
   in waves, [numPartitions](../reference/options.md#numpartitions) chunks at a time, one
   connection each. Before a wave is read, its stamp L is recorded, `max_lsn` again, at or
   after S. The wave is appended to the target in one Delta commit: operation 0,
   `_start_lsn` = L, `_snapshot` = S and the chunk in `_chunk`. Then one `snapshot_chunk`
   facts row per chunk: its rows, L and its key range. The next wave is read meanwhile,
   while a background thread writes that commit and those rows.
3. After the last chunk, which ends just above the MAX recorded at the open, it writes the
   snapshot's `bootstrap` facts row, with min = max = S and the rows of every chunk, and
   returns `done`. Downstream rebuilds from S: its rows and every change after S.

A commit that lands between a chunk's stamp and its read can show in the chunk and again as
a change after S; the latest image per key absorbs it, as with a full snapshot. A key that
moves between chunks while they are read is a change after S, which the stream carries, and
so is every row inserted above the MAX: no chunk reads those.

- A crash between a wave's append and its facts rows reruns that wave: Delta skips the
  append and the facts rows are rebuilt from the commit that holds the wave or, once Delta's
  log cleanup has dropped that commit, from the wave's rows in the target (without its read
  time and size). Nothing is appended twice.
- `min_headroom_hours` pauses before a wave while the stream's newest facts row shows less
  [retention headroom](monitoring.md), or the stream has written none: the chunks share the
  link with the stream, and a stream that falls behind the retention loses the snapshot too.
- `isolation="snapshot"`, or the stream's `isolationLevel=snapshot` option when `isolation`
  is left out, reads under SNAPSHOT isolation, where the DBA has set
  `ALLOW_SNAPSHOT_ISOLATION`: neither a chunk nor the planning then waits for writers'
  locks, at the cost of the version store. By default it reads READ COMMITTED, where a chunk
  or the plan waits for a transaction holding locks in its range; never `NOLOCK`.
- `max_waves` and `max_seconds` bound one call; the result also has `chunks_done`,
  `chunks_total` (the plan's, from the first call on) and the snapshot's LSN.
- A wave takes whole rounds of `numPartitions` chunks, each connection reading its share one
  chunk after the other: as many rounds as `target_wave_seconds` (5 minutes by default)
  holds at the pace of the last wave, and no more than the rest of `max_seconds`. Each wave
  pays a Spark job, a commit and its facts rows, and waits for its slowest connection, so
  fewer, longer waves cost less; its rows stay cached until its commit, so a longer target
  holds more of them. `target_wave_seconds=0` reads one round per wave.

[`apply_changes`](silver.md#chunked-snapshots) applies the waves as they arrive, when given
the facts table. [`reconcile`](validation.md) checks the chunks against the facts and the
result against the table.

### How chunks are sized

The first `backfill()` call plans every chunk of the snapshot, after S, and the plan never
changes while the snapshot is open: a later call with another `chunk_rows` logs a warning
and keeps the plan's. Each chunk is a fixed key range `[lo, hi)`; the first is open below,
each starts where the previous ends, and the last ends just above the MAX.

- One integer key: the rows are counted per slice of the key, about 16 slices per
  `chunk_rows`, in one `GROUP BY` on the server. A slice holding more than `chunk_rows` rows
  is counted again in finer slices. Consecutive slices are then joined into chunks of at
  most `chunk_rows` rows. So the sizes follow where the rows are, not the key's spread: a
  dense cluster of ids is split, and a sparse region or a sentinel far above the ids joins
  its neighbours instead of making a chunk of its own. When planned, every chunk but the
  last holds at most `chunk_rows` rows and loses less than one slice to the next; rows
  written while the snapshot is read change that a little.
- Any other key (composite, string, date): each chunk ends `chunk_rows` keys after the
  previous one, found by seeking the key on the server. The last ends at the first key after
  the MAX; when there is none yet at planning, `backfill()` looks for it again before each
  wave, so rows inserted above the MAX while a long backfill runs (a creation time, a
  sequential `uniqueidentifier`) stay the stream's instead of growing the last chunk.

Planning reads the whole key once before the first wave, under READ COMMITTED: on a table of
billions of rows that is a full scan of the narrowest index on the key, at the start of the
first call. It needs only the stream's grants ([Permissions](permissions.md)), and reads no
row over the network but the counts and the bounds.

### One mode per run

A run, one `to_delta` call with its bootstrap and its re-snapshot, takes its snapshots in
one `snapshot` mode. The next run may use the other mode, but not while a snapshot of the
other mode is still open. Both modes write a `snapshot_open` facts row before they read the
table (a chunked one once per generation, a full one at each run), and the snapshot's
`bootstrap` or `resnapshot` row closes it. With a facts table, `to_delta` (with
`bootstrap=True` or `on_data_loss="resnapshot"`), `snapshot(app_id=..., facts_table=...)`
and `backfill()` raise `ValueError` when the stream has an open snapshot of the other mode,
and `seed(app_id=..., facts_table=...)` when it has one of either mode.

A full snapshot is open only while a run may still be reading it. Once CDC cleanup has
passed its LSN it can never complete, so it no longer holds the mode, and a chunked run takes
its generation over.

To finish an open snapshot and free the mode:

- a chunked one: call `backfill()` until it returns `done` (or rerun `to_delta` with
  `snapshot="chunked"` and keep calling it);
- a full one, left open by a run that stopped while reading: rerun `to_delta` with
  `snapshot="full"`, which reads the table again and writes its `bootstrap` or `resnapshot`
  row.

The error names the snapshot's LSN and generation. The library cannot tell a full snapshot
whose run died from one still being read: after a killed full run, either rerun it or wait
until CDC cleanup passes its LSN (the retention at most) before switching to chunked. A full
re-snapshot that failed because it outlived the retention is past that already: rerun with
`snapshot="chunked"` (and `resnapshot_interval_days=0`, as after any failed attempt).

## Tables too big to snapshot

A snapshot has to finish within the CDC retention, or the changes after its LSN are purged
before the stream reads them and the stream stops with `DataLossError`. When the table's
size over the link's MB/s does not fit in the retention, take a
[chunked snapshot](#chunked-snapshots), or, when a copy already exists, seed the target from
it: hours instead of the weeks a chunked snapshot of billions of rows takes.

Seed the target from a copy you already have, an existing lake table for instance, and let
the stream start where the copy ends:

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
  SQL Server's clock (`sourceTimeZone`), to the second; in the hour before a daylight-saving
  fall-back, an hour earlier, since that hour's local times repeat. Taken on another
  machine, subtract the clock difference: earlier is always safe, it only replays more;
  later loses the commits in between.
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
  with the same `as_of` returns the seed already there, so the call can stay in the job:
  also under a snapshot appended later, and once cleanup has passed a time `as_of`, when
  the newest snapshot of the table at or before it is taken as the seed.

Never use `on_data_loss="resnapshot"` with a full snapshot on such a table: it reads it
whole. With `snapshot="chunked"` the re-snapshot is chunked too
([Data loss](data-loss.md#chunked-re-snapshots)). After a
`DataLossError`, seed a newer copy with `reseed=True`, then start a new checkpoint and
`app_id` from it ([Data loss](data-loss.md#recovering-by-hand)). The reasoning is in
[ADR 0025](../decisions/0025-seed-from-an-existing-copy.md).

## Pitfalls

- `bootstrap=True` sets `startingLsn` itself: passing both is a `ValueError`.
- Do not delete the snapshot rows (`_operation = 0`) from bronze. The next run with
  `bootstrap=True` would find none and read the whole table again.
- The snapshot reads the source table, not CDC: it puts a full scan's load on SQL Server.
  Cap `numPartitions` on a busy server. A chunked snapshot spreads the same load over its
  waves.
- A chunked snapshot needs the stream running while it is read: a gap after S (data loss,
  `failOnDataLoss=false`) abandons it. Leave `snapshotLsn` unset: a stamp below S breaks it.
- A chunked snapshot copies source key values out of bronze: its facts rows
  (`snapshot_open`, `snapshot_plan`, `snapshot_chunk`) and bronze's commit `userMetadata`
  hold the chunk bounds: the key's MAX at the open and, for a key that is not a single
  integer, a real key every `chunk_rows` rows. Column masks and row filters on bronze do not reach them, so give the
  facts table and bronze's history (`DESCRIBE HISTORY`) bronze's access policy. Where keys
  are natural or personal identifiers (a document number, an e-mail address), use one
  `facts_table` per access domain ([Many tables](many-tables.md#what-each-stream-has)).
- `to_delta(bootstrap=True)` with either `snapshot` mode, and `snapshot()`, return the S of
  a chunked snapshot of the target, open or complete, instead of reading the table again
  (given the facts table, a full run raises while it is open:
  [One mode per run](#one-mode-per-run)); `seed()` refuses a target that holds one. To take another kind, start a new target, or a
  new checkpoint and `app_id`. Before the first wave lands only the facts show it, so
  `snapshot()` and `seed()`, which do not read them, would not see it then.

## See also

- [Data loss and re-snapshots](data-loss.md): snapshots taken after CDC cleanup purged
  unread changes.
- [Silver tables](silver.md): the current state from the snapshot and the changes.
- [Validation](validation.md): compare silver with the source table after a bootstrap.
- [`CdcStream.snapshot`](../reference/api.md#mssql_cdc.pipeline.CdcStream.snapshot),
  [`CdcStream.backfill`](../reference/api.md#mssql_cdc.pipeline.CdcStream.backfill) and
  [`CdcStream.seed`](../reference/api.md#mssql_cdc.pipeline.CdcStream.seed) in the API
  reference.
