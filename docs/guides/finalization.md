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

### A stream that keeps running

`end_offset_from_progress(query.lastProgress)` after `awaitTermination()` fits jobs that run
with `availableNow` on a schedule. For a stream that keeps running (a `processingTime`
trigger or the default), the library has no built-in hook yet ([Roadmap](../ROADMAP.md)):
call `advance` from a `StreamingQueryListener`,
or from a separate job that reads the checkpoint's committed offsets
([Extension points](../ARCHITECTURE.md#extension-points)).

## Pitfalls

- Advance only after the data is committed. An early verdict claims data the table does not
  have yet.
- The control row is keyed by the string you pass to `advance`. Use the same name or path
  for the stream's target, the verdict, `apply_changes` and the consumers: `bronze.orders`
  and the table's storage path are two different rows.
- `finalized_until` is a `TIMESTAMP_NTZ` in UTC, and `is_final` compares it with a naive
  `datetime`. Pass the period end in UTC without `tzinfo`; an aware `datetime` raises
  `TypeError`.
- Complete is not lossless. After a re-snapshot that followed data loss, the verdict still
  means that no more commits will arrive, but the changes between `lost_from_ts` and
  `lost_to_ts` are missing from bronze's change history ([Data loss](data-loss.md)).

## See also

- [Monitoring](monitoring.md): the per-batch facts behind the verdict.
- [API reference](../reference/api.md#mssql_cdc.finalization.advance): `advance`,
  `is_final`, `candidate`, `end_offset_from_progress`.
- [Design notes](../DESIGN.md): why SQL Server CDC can give a stronger signal than event
  times.
