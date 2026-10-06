# Finalization

With CDC, a period that has data is not necessarily complete: a commit can reach the lake
after its hour is over. `finalized_until` tells a downstream job when a period is safe to
read. It lives in a control table, one row per table:

> Every period that ends at or before `finalized_until` is complete in the table: no source
> commit at or before it can still arrive. A consumer of the period `[start, end)` waits for
> `finalized_until >= end`.

## Smallest example

The producer advances the verdict after the stream's data is committed. With
`trigger={"availableNow": True}` that is when `awaitTermination()` returns:

```python
from mssql_cdc import finalization, stream

query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/data/checkpoints/orders",
    facts_table="ops.ingestion_facts",
    trigger={"availableNow": True},
)
query.awaitTermination()

end = finalization.end_offset_from_progress(query.lastProgress)
finalization.advance(spark, "ops.table_finalization", "bronze.orders", end)
```

`advance` creates the control table on first use and returns the table's verdict, a naive
UTC `datetime`, or `None` before the first one.

The consumer asks whether its period is complete:

```python
from datetime import datetime

from mssql_cdc import finalization

period_end = datetime(2026, 10, 1, 14)  # UTC, no tzinfo
if finalization.is_final(spark, "ops.table_finalization", "bronze.orders", period_end):
    ...  # every commit before 14:00 UTC is in bronze.orders
```

Or, from any SQL engine or orchestrator sensor that reads Delta:

```sql
SELECT 1
FROM ops.table_finalization
WHERE table_name = 'bronze.orders'
  AND finalized_until >= TIMESTAMP_NTZ '2026-10-01 14:00:00';
```

[Running on Databricks](../DATABRICKS.md) shows the same gate in Lakeflow Jobs and Airflow.

## How it behaves

`finalized_until` is the commit time of the batch's end offset, truncated to the period:
`"hour"` by default, or `"minute"` or `"day"` through `advance(..., granularity=...)`. The
period that contains the end commit is left out, because other transactions with that exact
commit time may still be on their way.

The signal is sound because a batch never reads past `sys.fn_cdc_get_max_lsn()` and CDC
capture writes changes in commit order: once a batch ending at an LSN is in the table, every
commit up to that LSN's commit time is too ([Design notes](../DESIGN.md)).

- Data first, verdict after. `advance` runs after the data commit and never moves the
  verdict backwards. If the job dies between the two, the verdict lags and consumers wait a
  little longer; it can never run ahead of the data
  ([ADR 0005](../decisions/0005-ordering-over-atomicity.md)).
- A control table, not a table property, so the verdict never interferes with writers or
  streaming readers of the data table
  ([ADR 0004](../decisions/0004-verdict-in-control-table.md)). Its columns are
  `table_name`, `finalized_until`, `end_lsn`, `end_commit_ts` and `updated_at`, plus
  `applied_lsn` and `snapshot_lsn` for silver tables ([Tables](../reference/tables.md)).
- On a quiet database the end offset still moves: `max_lsn` advances on every captured
  commit and, when nothing is captured, on an idle entry about every 5 minutes. The end
  offset trails real time by up to about 5 minutes, so an hour becomes final up to about 5
  minutes after it ends (and `finalized_until`, truncated to the hour, trails real time by
  up to about 65). The optional
  [`sql/heartbeat.sql`](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/sql/heartbeat.sql)
  Agent job, run by a DBA, brings the 5 minutes down to about 10 seconds
  ([ADR 0010](../decisions/0010-heartbeat-for-quiet-databases.md)).
- `apply_changes` advances the silver table's own row, capped at the bronze verdict it read
  before applying ([Silver tables](silver.md)). Gate silver consumers on the silver name.

## Continuous mode

`end_offset_from_progress(query.lastProgress)` after `awaitTermination()` fits jobs that run
with `availableNow` on a schedule. A stream that keeps running (a `processingTime` trigger,
or the default) has no end to wait for: `track` advances the verdict after each batch,
while the query runs.

```python
from mssql_cdc import finalization, stream

query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/data/checkpoints/orders",
    facts_table="ops.ingestion_facts",
    trigger={"processingTime": "1 minute"},
)
tracker = finalization.track(spark, query, "ops.table_finalization", "bronze.orders")
query.awaitTermination()
tracker.join()  # the last batch's verdict is written
```

`track` registers a `StreamingQueryListener` for this run of the query
([ADR 0026](../decisions/0026-continuous-finalization-listener.md)):

- Spark posts a batch's progress event after the batch's data and its checkpoint commit, so
  the verdict still follows the data.
- The listener does no Spark work on the listener bus, which every listener of the session
  shares. It hands the newest progress to one worker thread that calls `advance`. Progress
  that arrives while an advance runs replaces the one still waiting, and an end offset that
  does not move the verdict is skipped, so the control table gets about one commit per
  period, not one per batch.
- A failure never touches the query. `advance` itself retries, for up to a minute with
  backoff, a MERGE that loses a write conflict to another commit on the control table (in
  open-source Delta two MERGEs conflict even on different rows), also after the query has
  stopped. A verdict that still fails waits for the next progress: an idle query still
  reports progress about every 10 seconds
  (`spark.sql.streaming.noDataProgressEventInterval`).
- `tracker.last_error` holds the error of the last attempt (`None` once one succeeds) and
  `tracker.failures` how many failed in a row. The first failure of a streak is logged at
  ERROR with its traceback (logger `mssql_cdc.finalization`), then one WARNING at most every
  10 minutes until one succeeds, so a permanent error (no `MODIFY` on the control table)
  shows once instead of flooding the log. Alert on the verdict's freshness too
  ([Monitoring](monitoring.md#freshness-of-the-verdict)).
- When the query terminates, with or without an error, the worker applies what is left,
  stops and removes the listener. `join(timeout)` waits for that and returns `False` if the
  timeout passes first; whether the last verdict failed is in `last_error`, not in `join`'s
  result. A restarted query is a new run: call `track` again.
- `track` creates the control table before it returns, so a wrong name fails there.
  Called on a query that already finished, it applies the query's last progress and stops.

`to_delta` does not take a control table: `track` is the one line that returns the handle
`join` needs. `track` also works with `availableNow`: `awaitTermination()`, then `join()`.

Where the listener runs:

- Classic PySpark (local, a cluster's driver): the JVM calls the Python listener through
  Py4J; tested with PySpark 4.2.0 in local mode.
- Spark Connect (a PySpark 4.x client): PySpark 4.2.0's client keeps Python listeners on the
  client and receives the events over the connection
  (`pyspark/sql/connect/streaming/query.py`, `StreamingQueryListenerBus`), so the worker runs
  on the client and runs `advance` through the client's session. There the listener stays
  registered after its run: removing the client's last listener while another query posts
  an event hangs PySpark 4.2.0's listener bus. It ignores every other run's events. Not
  tested.
- Databricks classic compute and Databricks Connect: not tested. Serverless is not supported
  yet ([Databricks](../DATABRICKS.md)).

A separate job that reads the checkpoint's committed offsets and calls `advance` remains an
option where a listener cannot run ([Extension points](../ARCHITECTURE.md#extension-points)).

## Pitfalls

- Advance only after the data is committed. An early verdict claims data the table does not
  have yet.
- The control row is keyed by the string you pass to `advance`. Use the same name or path
  for the stream's target, the verdict, `apply_changes` and the consumers: `bronze.orders`
  and the table's storage path are two different rows.
- `finalized_until` is a `TIMESTAMP_NTZ` in UTC, and `advance` and `finalized_until` return
  it as a naive `datetime`. Python's `.timestamp()` reads a naive `datetime` as the
  machine's local time: call `fu.replace(tzinfo=timezone.utc).timestamp()` for an epoch.
- Complete is not lossless. Over a recorded loss gap, a change-log table's verdict (bronze's)
  means that nothing more will arrive, not that the gap's changes are in it: after a
  re-snapshot that followed data loss, the changes between `lost_from_ts` and `lost_to_ts`
  of the facts' `resnapshot` row are missing from bronze's change history
  ([Data loss](data-loss.md)). Silver, rebuilt from the snapshot, is complete up to its own
  verdict.
- With `failOnDataLoss=false` the verdict moves past skipped changes. A skip leaves a
  `data_skipped` facts row with the gap (it needs the facts table and a
  [metricsPath](../reference/options.md#metricspath)); one a task finds after its read says
  the loss is possible, not certain (`certain` false in `detail`) ([Data loss](data-loss.md)).
- With a named zone that has daylight saving (`sourceTimeZone`, or `auto` on SQL Server
  2022), `granularity="minute"` is unsafe across a fall-back: commits in the second pass of
  the repeated hour get commit times an hour early, in minutes already declared final. Use
  `"hour"` or `"day"` there; those rows are still filed under the hour before.

## See also

- [Monitoring](monitoring.md): the per-batch facts behind the verdict.
- [API reference](../reference/api.md#mssql_cdc.finalization.advance): `advance`, `track`,
  `finalized_until`, `is_final`, `candidate`, `end_offset_from_progress`.
- [Design notes](../DESIGN.md): why SQL Server CDC can give a stronger signal than event
  times.
