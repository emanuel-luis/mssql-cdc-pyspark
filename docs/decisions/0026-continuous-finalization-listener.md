# 0026: Continuous-mode finalization through a streaming query listener

**Status:** accepted  
**Date:** 2026-10-01T16:36:15-03:00

## Context
`finalized_until` advanced only when the caller ran `finalization.advance` with the end offset
of a committed batch, which fits `availableNow` runs: `awaitTermination()`, then
`advance(end_offset_from_progress(query.lastProgress))`. A query that keeps running (a
`processingTime` trigger, or the default) has no such point, so its verdict did not move
while it ran.

* Spark posts a `QueryProgressEvent` after the batch's data and its checkpoint commit: in
  Spark 4.2.0's `MicroBatchExecution`, `runBatch` calls the sink and then
  `markMicroBatchEnd`, which writes the commit log, and `executeOneBatch` calls
  `finishTrigger`, which posts the progress, after `runBatch` returns. The event's
  `sources[0].endOffset` is the batch's end offset. Advancing from it keeps the ordering of
  ADR 0005.
* Listener callbacks run on a thread shared by every streaming listener of the session (the
  JVM's listener bus, which calls a Python listener through Py4J; on Spark Connect, the
  client's `StreamingQueryListenerBus` thread). A MERGE takes seconds, so running `advance` in
  the callback would hold up every other listener's events.
* With a short trigger, a batch every few seconds would mean a MERGE on the control table per
  batch, while the verdict only moves once per period.

## Decision
* `finalization.track(spark, query, control_table, table_name, granularity="hour")` creates
  the control table, registers a `FinalizationListener` for the query's `runId` and returns
  it.
* The callbacks only store the newest progress of that run (one slot, so a newer progress
  replaces a waiting one) and wake one worker thread. The worker calls `advance` with the
  progress's end offset when its candidate is newer than the verdict the worker last wrote.
  An exception is logged as a warning and swallowed: the next progress retries, and an idle
  query reports one about every 10 seconds.
* `onQueryTerminated` of that run stops the worker after it applies what is left; the
  worker then removes the listener. `join(timeout)` waits for that. A progress posted before
  `track` registered the listener is covered by `query.lastProgress`, which `track` applies
  first; a query that terminated before `track` is applied once and stopped.
* `to_delta` takes no control table: `track` returns the handle `join` needs, and keeps
  `to_delta`'s signature as it is.

## Consequences
* A running stream's verdict moves one MERGE after the first batch that reaches a new
  period; the control table gets about one commit per period and table.
* If the driver dies between a batch's commit and the worker's MERGE, the verdict lags until
  the next run's first progress: data first, verdict after.
* Filtering by `runId`, not `id`: a restart keeps the query's `id`, and the old run's
  termination must not stop a listener tracking the new run. Every run needs its own `track`.
* On Spark Connect, PySpark 4.2.0 keeps Python listeners on the client, so `track` works
  with the client's session there by construction; not tested. Databricks classic,
  serverless and Databricks Connect are not tested either.
