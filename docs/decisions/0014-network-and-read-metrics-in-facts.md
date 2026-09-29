# 0014: Network and read metrics in the ingestion facts

**Status:** accepted  
**Date:** 2026-09-29T10:12:04-03:00
**Amended:** 2026-09-29T10:48:30-03:00, the round trip moved to the partitions; `stream()` declares the options once

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
* Source option `metricsPath`: each partition read writes one JSON file there (LSN range,
  rows, bytes, seconds, the change in `ASYNC_NETWORK_IO` over the read).
* `delta_sink(..., source_options=..., metrics_path=...)`: the sink pings SQL Server once
  per batch over one connection kept for the query run (`source_rtt_ms`), sums the files of
  the partitions that overlap the batch's LSNs (`read_seconds`, `read_mb`,
  `network_wait_ms`), writes them with the other facts and removes the files.
* The four columns arrive as facts migration 1 (ADR 0013); their definitions live in
  `migrations/facts.py` and the creation schema reuses them.
* Metrics are best effort: any error leaves them NULL and never fails a batch.

## Consequences
* A slow batch can be read as network (`network_wait_ms` close to `read_seconds * 1000`,
  high `source_rtt_ms`) or server (low wait, long read) from the facts alone.
* `source_rtt_ms` is the driver's round trip at write time, a proxy for the executors'
  (same network); it costs 3 round trips per batch.
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
