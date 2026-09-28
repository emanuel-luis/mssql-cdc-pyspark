# 0010: Idle lag of ~5 minutes; an optional Agent heartbeat for less

**Status:** accepted  
**Date:** 2026-09-28T19:03:23-03:00

## Context
`finalized_until` follows the commit time of the batch's end LSN, and the end LSN is at
most `sys.fn_cdc_get_max_lsn()`. That LSN only moves when the capture job writes to
`cdc.lsn_time_mapping`: for each captured commit, and, with no changes, an idle entry
whose frequency Microsoft does not document ("entries may also be logged for which
there are no change tables entries").

The lab measured it on SQL Server 2022 CU27 (lab t1, and a probe with each scenario for
3 minutes, capture job at its defaults, `pollinginterval` 5 s):

| Scenario | `max_lsn` advances | Worst lag of its commit time |
|---|---|---|
| idle | one every ~305 s | ~304 s |
| `UPDATE` every 10 s on a table without CDC | as idle | ~300 s |
| `CHECKPOINT` every 10 s | as idle | ~276 s |
| `UPDATE` every 10 s on a CDC-tracked table | every 10 s | 20 s (p50 10 s) |

So a quiet database stalls the verdict for up to ~5 minutes, and only captured commits
move it sooner. The first CI runs of t7's idle check failed for this reason: they
waited 4 minutes.

## Decision
* Document the ~5-minute idle lag. It is acceptable for the default hourly periods.
* Ship `sql/heartbeat.sql`, optional and run by a DBA: a one-row table `dbo.cdc_heartbeat`
  with CDC on, and a SQL Server Agent job that updates it every 10 seconds (the Agent's
  minimum interval). `max_lsn` then lags by about 10 seconds.
* The reader stays read-only. It does not write heartbeats itself: that would need write
  permission on the source and a writer on every stream, and it only works while a
  stream is running.
* Idle windows in the lab exceed the interval: t1 runs 11 minutes, t7's idle check 6.

## Consequences
* `tests/integration` runs the shipped script and checks that an idle stream's end offset
  stays within 30 seconds of real time.
* The heartbeat's change table grows by one row per 10 seconds (8,640 a day), removed by
  the normal CDC cleanup.
* `dbo_cdc_heartbeat` is a capture instance like any other; a pipeline that fans out over
  every capture instance should skip it.
* The idle interval is an observation on one build, not a contract: t1 keeps measuring
  it in CI.
