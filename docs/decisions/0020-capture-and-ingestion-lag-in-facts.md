# 0020: Capture and ingestion lag in the ingestion facts

**Status:** accepted  
**Date:** 2026-09-30T10:33:11-03:00

## Context
A stream can be stale for two different reasons, and they need different people. CDC
capture itself can be behind: the capture job or SQL Server Agent stopped, or a log backlog
it has not scanned yet. Nothing the stream does helps; the DBA has to. Or capture is current
and the stream is behind it: slow batches, a sparse trigger, a job that stopped. The facts
said neither. `reportLatestOffset` shows `max_lsn` in the query progress, but progress is
not kept, and it is an LSN, not a time.

The reader can see capture's progress without new permissions: `sys.fn_cdc_get_max_lsn()`
needs none, and `sys.fn_cdc_map_lsn_to_time` is what the offsets already use (invariant 11).

## Decision
* Each partition that read rows, after reading, records in its metrics file (ADR 0014) the
  commit time (UTC) of `max_lsn` and its capture lag: the partition's own UTC clock, taken
  right after that query, minus that commit time. Best effort like the other metrics.
* The sink folds them into three facts columns, as facts migration 4 (ADR 0013):
  `source_max_commit_ts` (the latest over the partitions), `capture_lag_seconds` (the
  largest) and `ingestion_lag_seconds` (`source_max_commit_ts - max_commit_ts`).
* Same condition as the other metrics: NULL without `metricsPath`; `stream()` sets it for
  local and FUSE checkpoints. Snapshot event rows leave them NULL: a snapshot reads no
  change range.

## Consequences
* Two alerts with separate owners: capture lag high means capture is stuck (on a quiet
  database without the heartbeat, ADR 0010, it sits up to about 5 minutes, so alert above
  that); ingestion lag high means the stream is behind. Ingestion lag and
  `retention_headroom_hours` (ADR 0017) add up to `source_max_commit_ts -
  retention_watermark_ts`, which stays near the retention period: what the lag gains, the
  headroom loses, and at 0 headroom the stream's next changes are being purged.
* Ingestion lag is measured from the batch's last change, like the headroom. On a table
  that changes less often than the rest of the database it also counts the time from that
  change to the newest commit in the database, which the stream may already have passed.
* Capture lag compares the Spark node's clock with the commit time from SQL Server's clock;
  skew between them shifts it, and can make a small lag negative.
* Facts only move while the stream runs; a stopped stream keeps its last lags. Alert on
  facts that stop arriving too (`now - max(written_at)`).
* Two more short queries per partition (`max_lsn` and its commit time).
* `tests/test_delta_sink.py` checks exact values against the fake's timeline;
  `tests/integration` checks they are filled and non-negative on a real server.
