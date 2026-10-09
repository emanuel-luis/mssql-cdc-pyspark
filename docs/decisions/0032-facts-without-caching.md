# 0032: Facts without caching where the platform refuses it

**Status:** accepted  
**Date:** 2026-10-09T11:39:56-03:00  
**Amended:** 2026-10-09T19:26:50-03:00, a run's first batch takes the batch before it from the facts table; `reconcile()` asks a one-row range whether checkpoints are refused; a write's own commit found by version; metrics a Spark Connect write did not return counted again; the uncached path tested through Spark Connect; not run on serverless yet

## Context
The sink persists each micro-batch, counts it (its facts, and whether it has rows to append)
and appends it from the cache, so the source is read once. `backfill()` does the same with a
wave: cached and counted per chunk, then appended with the counts in its commit's
userMetadata, while the next wave is read ([ADR 0028](0028-chunked-snapshot-next-to-the-stream.md)).
`reconcile()` reads the rows it compares into a local checkpoint, once, before bronze is
pinned. Databricks serverless, a Spark Connect platform, refuses every DataFrame cache API
(`persist`, `cache`, `unpersist`, `localCheckpoint`) with an exception, so the sink fails on
its first batch and `backfill()` on its first wave. Counting the batch with one read and
appending it with another doubles the load on the source and its link, and was ruled out
when this was planned.

Measured on 2026-10-09: on serverless `mssql-python` imports and loads without an init
script, but serverless cannot reach the private lab SQL Server, so this path is to be checked
there with the `fake` backend; it has not run on serverless yet. An `observe()`d DataFrame
appended to Delta reports its metrics on classic PySpark 4.2 with delta-spark 4.4
(`tests/test_delta_sink.py`), and on a local Spark Connect 4.2 server from inside
`foreachBatch` (`tests/test_connect.py`). An empty append with `txnAppId` commits, and writes
an empty file.

## Decision
* Where the platform caches, nothing changes. The sink and `backfill()` try `persist()`;
  Spark Connect sends it at once, so a refusal raises there, before anything is read. The
  platform is told by that behaviour, never by its name; a sink, or a `backfill()` call,
  that was refused once does not ask again.
* Where it refuses, the sink reads the batch through its append, through `observe()` with
  the aggregates the cached path computes (`rows`, the LSN and commit-time ranges, the counts
  per operation): the facts are those of the rows the append wrote, from the same job.
  Whether that append committed or Delta skipped it as a replay is told by its userMetadata
  among the commits after the version read before it, selected by version, since others
  (a backfill wave, auto compaction) may commit meanwhile; a skipped one read nothing, so its
  facts are counted with one read of the batch.
* A Spark Connect server can answer a write before its observed metrics are in, and the
  client's `Observation` then holds none (seen in the connect suite: `seen.get` was `{}`
  after one append). The sink then counts the batch with one more read, as a replay's;
  `backfill()` counts the wave's chunks in bronze, as a rerun after a crash does.
* An empty append with `txnAppId` still commits, with an empty file, so after a batch
  without rows the sink first asks `isEmpty()`: it reads the batch up to its first row, and
  an empty batch whole, which is then its only read (its partitions write their metrics
  files) and writes no commit. After a batch with rows it does not ask, so a batch without
  rows that follows one writes an empty commit. Each run is a new sink (on serverless, each
  scheduled `availableNow` job): for a run's first batch, the batch before it is the facts
  table's last batch row, which the sink reads anyway to check the batch id; without a facts
  table, or before its first row, the sink does not ask. A quiet table pays the question only
  on the batch that ends a quiet spell, a busy one only on the empty batch that starts one.
* That commit's userMetadata is fixed before the batch is read: the same keys, `rows` to
  `updates` null, with `batch_id` and `app_id`. The facts table holds the values.
* `backfill()` appends an uncached wave as it reads it, with `observe()` counting each
  chunk's rows. Its userMetadata has `rows`, `high_lsn`, `read_seconds` and `read_mb` null;
  its 'snapshot_chunk' rows get them from the observation, the chunks' metrics files and
  `max_lsn` after the append. A rerun that finds such a commit after a crash before its facts
  rows counts each chunk's rows in bronze, as it already did once log cleanup dropped the
  commit. Nothing is read ahead, since the read is the append: a wave is planned once the one
  before is recorded, and sized by how long its append took.
* `reconcile()` asks for a lazy local checkpoint of a one-row range first: even a lazy one of
  the source's read plans its scan, which asks SQL Server, so a failure there (a timeout, a
  failover) would be taken for a refusal and the source's plan made twice. The read's own
  checkpoint then raises as it fails. Refused, it compares the counts (Tier 1) only, every
  bucket `hashed` false, and logs a warning: without a checkpoint the rows would be read
  after bronze is pinned, and once for each use.
* `silver.apply_changes` uses no cache API.

## Consequences
* The facts table gets the same values either way: a test runs one timeline (batches with
  rows, two without, an update and a delete, each batch replayed) cached and uncached and
  compares bronze and the facts, and another runs `backfill()` uncached through a crash
  between a wave's append and its facts rows. Through Spark Connect, a test refuses the cache
  API in the client and in the local server's `foreachBatch` worker (a `sitecustomize` on
  its Python path) and runs a chunked bootstrap, `availableNow` runs with batches with and
  without rows and a replayed batch, `backfill()`, `apply_changes` and `reconcile()`: the
  facts' counts, bronze's commits and silver are as expected, and `reconcile()` compares
  the counts.
* Uncached, a batch with rows that `isEmpty()` asked about (the first after a batch without
  rows) is read a second time, its first partition with rows up to its first row: one more
  connection and query, on a range ordered by `__$command_id`, which SQL Server may sort
  before it returns a row, and a Python worker stopped mid-read (it logs a
  `ConnectionResetError`). A batch without rows after one with rows (or first in a run
  without a facts table) leaves an empty commit and file in bronze, which compaction
  (`OPTIMIZE`, or predictive optimization of a Unity Catalog managed table) removes. These,
  and reading ahead in `backfill()`, are why caching stays where the platform allows it.
* An uncached wave whose chunks are all empty writes a commit with an empty file, where a
  cached one writes none; a wave is rarely empty.
* State: the payloads of [ADR 0021](0021-compatibility-policy-for-0x.md) keep their keys; a
  wave's `rows`, `high_lsn`, `read_seconds` and `read_mb` may now be null, which a reader
  takes as not known (ADR 0021 amendment 8). A release before this one that finds such a
  commit after a crash writes NULL counts for its chunks.
* Measured with a script outside the suite, on the `fake` backend (local Spark 4.2 and
  delta-spark 4.4, `local[2]`, `numPartitions` 2, two runs each, the refusal simulated by
  making the cache API raise): a
  batch of 20,000 rows took 3.6 to 4.4 s cached and 4.6 to 5.1 s uncached (the observation
  and the history read back), the run's first one 6.6 to 8.4 s and 10.4 s (the question);
  a batch without rows 1.9 to 2.4 s cached, 1.6 to 1.9 s uncached after one without rows
  and 4.0 to 4.6 s after one with rows (the empty commit). `backfill()` of 8 chunks of
  20,000 rows in waves of 2: 23.7 and 26.1 s cached, 24.0 and 26.4 s uncached. The fake
  reads local files, so it shows neither the second read's cost on a remote source nor what
  reading ahead saves there (0.5.0's benchmark against a production source).
