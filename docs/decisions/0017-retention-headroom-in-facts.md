# 0017: Retention headroom in the ingestion facts

**Status:** accepted  
**Date:** 2026-09-29T14:58:53-03:00

## Context
CDC cleanup deletes change rows by age whether or not anyone has read them. A stream that
falls behind by more than the retention period (3 days by default) finds its next range
purged and stops with `DataLossError`; recovering needs a new snapshot (ADR 0016). Nothing
warned before that point.

The retention period itself lives in `msdb.dbo.cdc_jobs`, and `sys.sp_cdc_help_jobs` needs
`db_owner`: neither is available to a least-privilege reader (invariant 11). What the
reader can see is the effect of cleanup: `sys.fn_cdc_get_min_lsn(ci)`, which every
partition already reads after its range for the retention guard.

## Decision
* Each partition records the commit time of that `min_lsn` (`lsn_to_time`) in its
  metrics file (ADR 0014), best effort like the other metrics.
* The sink folds the latest one into two facts columns, as facts migration 2 (ADR 0013):
  `retention_watermark_ts` (how far cleanup had deleted) and `retention_headroom_hours`
  (`max_commit_ts - retention_watermark_ts`: how far the stream is ahead of it).
* Same condition as the network metrics: NULL without `metricsPath`; `stream()` sets it
  for local and FUSE checkpoints.

## Consequences
* A current stream shows headroom near the retention period; it falls by the hours the
  stream lags, and a falling series is the warning. The value needs no permission beyond
  the reader's.
* Cleanup moves the watermark in steps (the default job runs daily), so the headroom
  saw-tooths; alerts need more margin than the cleanup interval.
* The facts only move while the stream runs. A stopped stream keeps its last headroom while
  the real one shrinks, so alert on facts that stop arriving too (`now - max(written_at)`),
  e.g. `retention_headroom_hours - hours since the last written_at < 24`.
* One more short query per partition (the commit time of `min_lsn`).
* `tests/test_delta_sink.py` checks the values against the fake's cleanup;
  `tests/integration` checks they are filled on a real server.
