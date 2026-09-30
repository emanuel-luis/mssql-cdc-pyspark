# 0020: Capture and ingestion lag in the ingestion facts

**Status:** accepted  
**Date:** 2026-09-30T10:33:11-03:00  
**Amended:** 2026-09-30T15:16:41-03:00, ingestion lag measured from the batch's end offset; a batch without rows writes its row, so facts that stop arriving mean the stream or capture stopped (see the Amendment)

## Context
A stream can be stale for two different reasons, and they need different people. CDC
capture itself can be behind: the capture job or SQL Server Agent stopped, or a log backlog
it has not scanned yet. Nothing the stream does helps; the DBA has to. Or capture is current
and the stream is behind it: slow batches, a sparse trigger, a job that stopped. The facts
said neither. `reportLatestOffset` shows `max_lsn` and its commit time in the query
progress, but progress is not kept.

The reader can see capture's progress without new permissions: `sys.fn_cdc_get_max_lsn()`
needs none, and `sys.fn_cdc_map_lsn_to_time` is what the offsets already use (invariant 11).

## Decision
* Each partition (before the Amendment, each that read rows), after reading, records in its
  metrics file (ADR 0014) the commit time (UTC) of `max_lsn` and its capture lag: the
  partition's own UTC clock, taken right after that query, minus that commit time. Best
  effort like the other metrics.
* The sink folds them into three facts columns, as facts migration 4 (ADR 0013):
  `source_max_commit_ts` (the latest over the partitions), `capture_lag_seconds` (the
  largest) and `ingestion_lag_seconds` (`source_max_commit_ts - end_commit_ts`; it was
  `max_commit_ts` before the Amendment).
* Same condition as the other metrics: NULL without `metricsPath`; `stream()` sets it for
  local and FUSE checkpoints. Snapshot event rows leave them NULL: a snapshot reads no
  change range.

## Consequences
* Two alerts with separate owners: capture lag high means capture is slow (a log backlog;
  on a quiet database without the heartbeat, ADR 0010, it sits up to about 5 minutes, so
  alert above that); ingestion lag high means the stream is behind. Ingestion lag and
  `retention_headroom_hours` (ADR 0017) add up to `source_max_commit_ts -
  retention_watermark_ts`, which stays near the retention period: what the lag gains, the
  headroom loses, and at 0 headroom the stream's next changes are being purged.
* Ingestion lag was measured from the batch's last change, like the headroom. On a table
  that changes less often than the rest of the database it also counted the time from that
  change to the newest commit in the database, which the stream may already have passed.
  The Amendment measures both from the batch's end offset.
* The capture lag cannot show a stopped capture (capture job or SQL Server Agent down):
  `max_lsn` freezes, `latestOffset` returns the start once the stream reaches it, no batch
  runs and no facts row is written, so the last capture lag stays small. The only sign in
  the facts is rows no longer arriving, which before the Amendment a quiet table also
  caused. Capture lag rises in the facts only while capture is behind but still moving, or
  while a stream that was behind drains up to the frozen `max_lsn`. For a capture lag that
  updates on every trigger, idle ones included, use `now - latestOffset.commit_ts` from the
  source's `lastProgress` (`reportLatestOffset`, admission-control reader).
* Capture lag compares the Spark node's clock with the commit time from SQL Server's clock;
  skew between them shifts it, and can make a small lag negative.
* Facts only move while the stream runs; a stopped stream keeps its last lags. Alert on
  facts that stop arriving too (`now - max(written_at)`).
* Two more short queries per partition (`max_lsn` and its commit time).
* `tests/test_delta_sink.py` checks exact values against the fake's timeline;
  `tests/integration` checks they are filled on a real server, the capture lag allowing a
  few seconds of skew between the host and the container clocks.

## Amendment: measured from the end offset, a row for every batch
The ingestion lag was `source_max_commit_ts - max_commit_ts`, from the batch's last change,
and a batch that read no rows wrote no facts row. On a table that changes less often than
its database, a current stream showed the time since the table's last change as lag, and
the alert on facts that stop arriving fired while the stream ran. Now (as for the headroom,
ADR 0017, amended the same day):
* `ingestion_lag_seconds = source_max_commit_ts - end_commit_ts`, the commit time of the
  batch's end offset (facts migration 5, ADR 0014 Amendment 3); `max_commit_ts` only when
  `end_commit_ts` is unknown. A current stream's lag is near 0 on a quiet table too.
* Every partition writes its metrics file, rows or not, and a batch that read no rows writes
  its facts row (`rows = 0`, no target commit).
* So the facts can now show a stopped capture: they stop arriving only when the stream or
  CDC capture stops (a frozen `max_lsn` runs no batch). On a quiet database capture writes an
  idle entry about every 5 minutes (ADR 0010), so a stream with a continuous trigger writes
  a row at least that often, plus its trigger interval; `now - max(written_at)` beyond that
  means one of the two stopped. If the query is still running, it is capture:
  `now - latestOffset.commit_ts` in its progress grows.
* Migration 5 also gives existing tables the new column comment.
* `tests/test_delta_sink.py` checks the lag of an empty batch and of a batch whose end offset
  is past its last change against the fake's timeline; `tests/integration` checks `end_lsn`
  and `end_commit_ts` are filled on a real server.
