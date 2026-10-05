# 0014: Network and read metrics in the ingestion facts

**Status:** accepted  
**Date:** 2026-09-29T10:12:04-03:00  
**Amended:** 2026-09-29T10:48:30-03:00, the round trip moved to the partitions; `stream()` declares the options once  
**Amended:** 2026-09-29T22:15:55-03:00, a partition that read no rows leaves no file (see Amendment 2)  
**Amended:** 2026-09-30T15:16:41-03:00, every partition leaves a file again, with the commit time of its last LSN; the sink folds every file present (see Amendment 3)  
**Amended:** 2026-09-30T17:47:32-03:00, one directory per stream, emptied before each batch is read (see Amendment 4)  
**Amended:** 2026-10-05T13:53:56-03:00, only the batch's last partition measures where the stream is (see Amendment 5)

## Context
On a production source the reader was network-bound: round trips of 180–950 ms, and the
server spending most of each read in `ASYNC_NETWORK_IO` (waiting for the client), not on
disk or CPU (lab t8 and a private benchmark). Throughput moved several-fold between runs
with the link, and nothing in the facts said why a batch was slow.

The measurements happen where the facts cannot see them: partitions read in Python
workers on executors, planning runs in a worker on the driver, and the Python data source
API has no custom metrics or accumulators. The offset cannot carry them (invariant 1) and
the bronze schema should not.

## Decision
* `CdcClient.ping(samples)` (round trips of `SELECT 1`) and `network_wait_ms()` (the
  session's `ASYNC_NETWORK_IO` from `sys.dm_exec_session_wait_stats`, which a session may
  read for itself without `VIEW SERVER STATE`).
* Source option `metricsPath`: each partition writes one JSON file there (LSN range,
  rows, bytes, seconds, the change in `ASYNC_NETWORK_IO` over the read; since Amendment 3,
  also the commit time of its last LSN).
* `delta_sink(..., source_options=..., metrics_path=...)`: the sink pings SQL Server once
  per batch over one connection kept for the query run (`source_rtt_ms`), sums the files of
  the batch's partitions (`read_seconds`, `read_mb`, `network_wait_ms`), writes them with
  the other facts and removes the files.
* The four columns arrive as facts migration 1 (ADR 0013); their definitions live in
  `migrations/facts.py` and the creation schema reuses them.
* Metrics are best effort: any error leaves them NULL and never fails a batch.

## Consequences
* A slow batch can be read as network (`network_wait_ms` close to `read_seconds * 1000`,
  high `source_rtt_ms`) or server (low wait, long read) from the facts alone.
* `source_rtt_ms` costs one round trip per partition read (see the Amendment; it was
  first a driver-side ping by the sink).
* `metricsPath` must be a directory every node can write and the driver can read: a local
  path on a single node, or a FUSE path such as a Unity Catalog Volume.
* `read_seconds` sums partitions (task-seconds) and includes Spark taking the rows.

## Amendment: no source options in the sink, one declaration
Measuring the round trip in the sink meant passing the stream's options (with the
credentials) to `delta_sink` as well and holding a second connection per query. Each
partition now makes one `SELECT 1` on its own connection just before reading and records
it with its other metrics; `source_rtt_ms` is the median over the batch's partitions, and
`delta_sink` lost `source_options`: the sink never connects to SQL Server. Facts migration 1
was revised in place for the new meaning of `source_rtt_ms`, the same day it was written
and before any release.

`mssql_cdc.stream(spark, options).to_delta(target, app_id, checkpoint, facts_table)` wires
source and sink from one set of options. Knowing the checkpoint, it defaults `metricsPath`
to `<checkpoint>/_mssql_cdc_metrics` when that is a path Python can write on every node
(no URI scheme); otherwise metrics stay off unless `metricsPath` is set.

## Amendment 2: only partitions that read rows leave a file
The sink skips a batch without rows, so it never folded or removed the files of an empty
batch's partitions, and they piled up in `metricsPath` on a quiet source. A partition that
read no rows now writes no file. The cost: in a batch with rows, a partition that read none
(with `numPartitions` > 1, for example a trailing range of idle mapping entries) still
pings and reads the wait counter, but is left out of `read_seconds`, `network_wait_ms` and
the `source_rtt_ms` median. Such a partition returns no data, so its share is small.

## Amendment 3: every partition leaves a file again
Amendment 2 existed because the sink skipped a batch without rows. It no longer does: such a
batch writes its facts row too (rows = 0), so that the headroom and the ingestion lag are
measured from where the stream is, its end offset, on quiet tables as well (ADRs 0017 and
0020, amended the same day). So every partition writes its file again, rows or not, with one
more value: `to_commit_ts`, the commit time of its `to_lsn` (one more short query per
partition). The file with the largest `to_lsn` is the partition that ends at the batch's end
offset: its LSN and commit time become `end_lsn` and `end_commit_ts` (facts migration 5). On
a quiet table that partition often read nothing: with `numPartitions` > 1 it is the range
between the batch's last change and the end offset, when the offset is past that change.

That rules out picking files by the rows' LSNs, as the sink did: an empty batch has none, and
the trailing range lies past them. The sink now folds every file in `metricsPath` and removes
them all after the facts write, the unreadable ones too. Every file there is the current
batch's: micro-batches run one at a time, the partitions are read inside `foreachBatch`, and
a retried task or batch rewrites its file under the same name. The one exception is a retry
planned with a different split (a changed `numPartitions`), whose failed attempt's files
are folded once into the retry. The sink removes the files without a facts table too, and
`stream()` hands it the metrics path whenever `metricsPath` is set, so no file is left
behind. A batch that plans no range (a new checkpoint's first batch when nothing is new)
has no files: its row has `rows = 0` and NULL metrics. Amendment 2's cost is gone:
partitions without rows count in `read_seconds`, `network_wait_ms` and the `source_rtt_ms`
median again.

## Amendment 4: one directory per stream, emptied before each batch
Folding every file present is right only when the directory holds nothing but the current
batch's files. Two things broke that. Streams sharing one explicit `metricsPath` (a job
looping over tables with one options dict, the natural setup with a URI checkpoint) folded
and removed each other's files: one stream's `end_lsn`, headroom and lags came from
another's position, maybe another database's. And files left by an attempt that died after
some partitions wrote theirs were folded into a later batch: a new checkpoint's or
generation's first batch with an explicit `metricsPath`, or a replay planned with another
split, which `numPartitions=auto` produces without the user changing anything when the
cluster restarts with other cores. A stale `to_lsn` put `end_lsn` ahead of or behind the
real position, and the read metrics were counted twice.

Now:
* The sink empties the directory at the start of each batch, before the batch is read:
  its partitions run only when the sink reads the batch, so anything there is a dead
  attempt's. It removes the files after the facts write as before.
* `stream()` puts an explicit `metricsPath`'s files under `<metricsPath>/<sink app_id>`
  (`<app_id>.g<n>` in generation `n`), so its streams may share one `metricsPath`. The
  default under the checkpoint is per stream already.
* Wired by hand, `metricsPath` must belong to one stream, and `delta_sink` needs the same
  directory as `metrics_path`: nothing else removes the files, and every partition of every
  batch writes one, so without it they pile up.

## Amendment 5: the batch's position, measured once
Every partition measured the retention watermark (ADR 0017), how far capture had got and
its lag (ADR 0020), and the commit time of its own `to_lsn`. The first three describe the
whole batch, and the sink kept the latest or largest of each; of the fourth it used only the
partition with the largest `to_lsn`. They were four of each partition's nine queries.

Now the planner marks the batch's last range (`LsnRange.last`, the one that ends at the end
offset), and only it measures the four after its read; the other partitions leave their
read metrics (range, rows, bytes, seconds, round trip, network wait). A partition other
than the last runs 5 queries: the ping, the wait counter, the read, the retention guard
(invariant 4) and the wait counter again; the last runs those and `max_lsn` plus three
commit times (`tests/test_reader_units.py` pins both). Each runs one more, its range's UTC
offset (ADR 0008), where the server's clock is a named zone other than UTC. The sink is unchanged: its maximum
over the one value present is that value, and `end_lsn` is still the largest `to_lsn`. When
`failOnDataLoss=false` skipped everything up to the end offset in the batch's last capture
instance, the last range planned measures, as the largest `to_lsn` did before. The facts
columns keep their meaning; their comments, written for the per-partition values ("the
latest seen by the batch's partitions"), still hold for the one partition that reports.
With small tiles merged (ADR 0015, Amendment) a small batch is a single partition anyway;
this saves queries in large batches, four per range other than the last.
