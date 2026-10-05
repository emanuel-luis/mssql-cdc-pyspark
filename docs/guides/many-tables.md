# Many tables

A stream reads one capture instance. For a database with many tracked tables, run one
stream per table: `start_many` starts one [`to_delta`](streaming.md) stream per capture
instance in the same Spark session, each with its own checkpoint, `app_id` and bronze
table, all writing to one facts table
([ADR 0027](../decisions/0027-fan-out-one-stream-per-table.md)).

## Start them together

A scheduled job that catches every table up, then advances each table's verdict:

```python
from mssql_cdc import await_all, finalization, start_many

options = {
    "connectionString": "Server=sqlhost,1433;Database=sales;UID=cdc_reader;PWD=...;Encrypt=yes",
}
queries = start_many(
    spark,
    options,
    ["dbo_orders", "dbo_order_items", "dbo_customers"],
    target="bronze.{ci}",
    app_id="{ci}-v1",
    checkpoint="/Volumes/main/ops/checkpoints/{ci}",
    facts_table="ops.ingestion_facts",
    trigger={"availableNow": True},
)
failed = await_all(queries)

for ci, query in queries.items():
    if ci not in failed:
        end = finalization.end_offset_from_progress(query.lastProgress)
        finalization.advance(spark, "ops.table_finalization", f"bronze.{ci}", end)
if failed:
    raise RuntimeError(f"CDC streams failed: {sorted(failed)}")  # fail the run, alert
```

`target`, `app_id` and `checkpoint` are templates: `{ci}` becomes the capture instance.
Each must contain it, or `start_many` raises `ValueError` before starting anything. The
other keyword arguments (`facts_table`, `trigger`, `bootstrap`, `on_data_loss`...) go to
every `to_delta`, as in [Options](../reference/options.md#to_delta-parameters).

`await_all` waits for every query and returns the error of each one that failed, by
capture instance. It raises nothing, so the job above raises itself: without that, a run
with a failed table succeeds.

### Options per table

Pass a mapping instead of a list to give a stream options of its own. They override the
shared ones, matched ignoring case like every option:

```python
queries = start_many(
    spark,
    options,
    {
        "dbo_orders": {"numPartitions": "8", "maxCommitsPerBatch": "500"},
        "dbo_order_items": {"numPartitions": "8"},
        "dbo_customers": {"numPartitions": "1"},
    },
    target="bronze.{ci}",
    app_id="{ci}-v1",
    checkpoint="/Volumes/main/ops/checkpoints/{ci}",
    facts_table="ops.ingestion_facts",
)
```

## What each stream has

Each stream is exactly the one `stream(spark, options).to_delta(...)` starts for its table
alone; `start_many` adds no state of its own.

| | Per stream | Shared |
|---|---|---|
| Spark checkpoint, re-snapshot generations | `checkpoint` with `{ci}` | |
| Sink identity (`txnAppId`), query name | `app_id` with `{ci}` | |
| Bronze table | `target` with `{ci}` | |
| Metrics files | `<checkpoint>/_mssql_cdc_metrics`, or `<metricsPath>/<app_id>` | one `metricsPath` option may serve all |
| Facts rows | `app_id` and `target` columns | the facts table |
| `finalized_until` | one control row per target | the control table |

- The facts table is created, or brought up to date, once before the first stream starts.
  Writers that commit to a new Delta table at the same time conflict; afterwards every
  stream only appends its own rows. The [health query](monitoring.md#health-of-every-stream)
  groups by `target`, so it covers every table as is.
- A shared facts table shows every table's facts to whoever reads it, and a chunked
  snapshot's facts rows hold source key values (the chunk bounds,
  [Bootstrap](bootstrap.md#pitfalls)). Where keys are natural or personal identifiers, use
  one `facts_table` per access domain, a `start_many` call each, and give each the access
  policy of its bronze tables.
- Finalization stays per table: advance each target's verdict after its own query, as in
  the job above. A consumer that joins two tables waits until both are final for its
  period. Each stream is a prefix of the commit history of its own table, read at its own
  pace, so there is no single LSN across tables
  ([Finalization](finalization.md)).
- A new checkpoint template needs a new `app_id` template, as for one stream: bump the
  version in both (`{ci}-v2`, `.../checkpoints-v2/{ci}`)
  ([Streaming](streaming.md#app_id)).

## Keep them running

For an always-on job, use a `processingTime` trigger and bound the run with a timeout:

```python
from mssql_cdc import await_all, finalization, start_many, stop_all

tables = ["dbo_orders", "dbo_order_items", "dbo_customers"]
common = {
    "target": "bronze.{ci}",
    "app_id": "{ci}-v1",
    "checkpoint": "/Volumes/main/ops/checkpoints/{ci}",
    "facts_table": "ops.ingestion_facts",
    "trigger": {"processingTime": "1 minute"},
}
queries = start_many(spark, options, tables, **common)
trackers = [
    finalization.track(spark, q, "ops.table_finalization", f"bronze.{ci}")
    for ci, q in queries.items()
]
failed = await_all(queries, timeout=4 * 3600)  # every table runs for four hours
stop_all(queries)
for tracker in trackers:
    tracker.join()  # each table's last verdict is written, or its failure logged
if failed:
    raise RuntimeError(f"CDC streams failed: {sorted(failed)}")
```

A failed table stops alone while the others keep running; the run reports it when the
timeout ends, and the next run (the job's schedule or retry) starts every table again from
its checkpoint. In between, its facts stop arriving and the health query alerts on that
target. `await_all` with a timeout returns the failures so far; the queries still running
are not in it.

To stop everything as soon as one table fails instead, wait with Spark's own call:

```python
try:
    spark.streams.awaitAnyTermination()  # returns, or raises its error, when one query stops
finally:
    stop_all(queries)
```

`awaitAnyTermination` keeps returning at once after the first query stopped, until
`spark.streams.resetTerminated()`.

## Failure isolation

- **A query that fails stops alone.** Spark runs each query on its own thread with its own
  checkpoint; the others keep reading and writing (`tests/test_fanout.py`). Nothing in
  `start_many` stops them.
- **Restart one table** in the same session with the same arguments: it resumes from its
  checkpoint.

  ```python
  stopped = [ci for ci, q in queries.items() if not q.isActive]
  queries.update(start_many(spark, options, stopped, **common))
  ```

  Starting a table whose query still runs fails: Spark refuses two queries on one
  checkpoint. After `DataLossError` or `SchemaChangedError`, decide first
  ([Data loss](data-loss.md), [Schema changes](schema-changes.md)).
- **A start that raises stops the queries already started** and raises the error: a wrong
  option, a missing permission on one table, a bootstrap that failed, or the
  `on_data_loss="resnapshot"` pre-flight refusing a second re-snapshot. These need a
  person; take that table out of the list (or into a job of its own) to run the others
  meanwhile.
- **Starts are sequential.** `to_delta` runs a table's bootstrap or re-snapshot before it
  starts the query, so a large snapshot delays the tables after it in the list. For a first
  run over many large tables, put them last, or bootstrap them in a run of their own.

## Sizing the cluster

The streams share the cluster, and their micro-batches run at the same time.

- **Executor cores.** A batch is read in `numPartitions` ranges, each a task on one core
  with its own connection to SQL Server. `numPartitions=auto` gives every stream all the
  session's cores ([ADR 0011](../decisions/0011-num-partitions-from-cores.md)): with twenty
  tables on sixteen cores, every batch of every table is cut into sixteen ranges, mostly
  tiny ones. Set it per table instead: 1 or 2 for small or quiet tables, more for the busy
  ones, with a sum close to the cores.
- **Connections on SQL Server.** At most one per running task, so no more than the
  executor cores, plus one per stream on the driver, which plans its batches.
- **The driver** plans every stream and runs every `foreachBatch`, where the sink writes
  bronze and the facts. Each stream also keeps, for the life of its query, a Python worker
  process of its own on the driver, where Spark runs the source's planning, and that
  process's connection to SQL Server. Driver memory is therefore the first ceiling, before
  executor cores: past what one driver holds, split the tables into several jobs. No
  per-stream figure has been measured yet.
- **Facts commits.** Every trigger of every stream writes a facts commit. A
  `processingTime` trigger bounds them; compact the facts table
  ([Monitoring](monitoring.md#pitfalls)).
- **Scheduling.** Spark schedules the jobs of all queries first in, first out by default,
  so a large batch of one table can hold the cores while the others wait. Cap it with
  `maxCommitsPerBatch` on that table, or give the queries
  [fair scheduler pools](https://spark.apache.org/docs/latest/job-scheduling.html#fair-scheduler-pools).

## One job, or one job per table

| | One job with `start_many` | One job (or task) per table |
|---|---|---|
| Compute | one cluster, cores shared by every table | a cluster per job, or tasks on a shared job cluster |
| Driver | one, planning every stream, with a Python process per stream | one per job |
| A failing table | the others keep running; the run reports it at its end | its own run fails, retries and alerts |
| Restart | that table in the session, or the next run | that job |
| Deploy | one list | one definition per table, or a loop over the list |

Many small or quiet tables fit one job. Give a large or critical table a job of its own,
where its batches and its failures touch nothing else. Both can run side by side, as long as
no table is in two of them.

## On Databricks

`start_many` has not run on Databricks yet; what [Running on
Databricks](../DATABRICKS.md) says about runtime, access mode, install and network
applies to every stream.

- Put the checkpoints in a Volume (`/Volumes/<catalog>/<schema>/<volume>/checkpoints/{ci}`):
  the metrics then default under each checkpoint, and `on_data_loss="resnapshot"` works.
- In a notebook, end the cell that starts the streams with `await_all(queries,
  timeout=...)` and call `stop_all(queries)` in the next cell.
- In Lakeflow Jobs, run `start_many` in one task, or one stream per table with a
  [For each task](https://docs.databricks.com/aws/en/jobs/for-each) over the table list;
  that is the one-job-per-table column above, with a concurrency limit.

## Pitfalls

- Never run two jobs over the same list at once: each checkpoint takes one query at a time
  (Spark refuses a second one in the same session).
- The reader needs `SELECT` on the change table of each capture instance
  ([Permissions](permissions.md)).
- `start_many` names each query after its `app_id`: two queries with the same name cannot
  run in one session.

## See also

- [Streaming into Delta](streaming.md): one stream, which each of these is.
- [Monitoring](monitoring.md): the facts every stream writes.
- [`start_many`](../reference/api.md#mssql_cdc.start_many),
  [`await_all`](../reference/api.md#mssql_cdc.await_all) and
  [`stop_all`](../reference/api.md#mssql_cdc.stop_all) in the API reference.
