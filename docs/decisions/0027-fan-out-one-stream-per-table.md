# 0027: Many tables: one stream per capture instance, started by a fan-out helper

**Status:** accepted  
**Date:** 2026-10-01T16:30:09-03:00  
**Amended:** 2026-10-13T20:07:28-03:00, the driver's cost per stream measured (lab t11); each start waits for the source of the one before it (see the Amendment)

## Context
A source database tracks many tables, and a stream reads one capture instance. Every user
with more than a few tables wrote the same loop over `stream(...).to_delta(...)`, and had
to get the same details right: a checkpoint and an `app_id` per table (invariant 8), a
target per table, a query name per table, and the shared facts table. The roadmap asked
for either multiple capture instances per stream or a documented fan-out pattern.

## Decision
* One stream per capture instance, as today. `mssql_cdc.fanout` adds three functions,
  exported from the package:
  - `start_many(spark, options, capture_instances, *, target, app_id, checkpoint,
    facts_table=None, **to_delta_kwargs)` starts one `to_delta` per capture instance and
    returns the queries by capture instance. `target`, `app_id` and `checkpoint` are
    templates that must contain `{ci}`; `capture_instances` is a list, or a mapping of name
    to options that override the shared ones for that stream (ignoring case). Each query is
    named after its `app_id`.
  - `await_all(queries, timeout=None)` waits until every query stopped, or the timeout
    elapsed in all, and returns the failed ones' errors by capture instance, without
    raising: the caller advances the verdict of the others first, then fails the run.
  - `stop_all(queries)`.
* `start_many` creates or migrates the facts table once before the first start: two
  writers committing to a new Delta table at the same time conflict, and so would two
  streams migrating it. Afterwards every stream only appends its own rows, keyed by its
  `app_id`.
* Starts are sequential, each after its own bootstrap or re-snapshot. A start that raises
  stops the queries already started and re-raises: start errors are a wrong option, a
  missing permission or a refused re-snapshot, which need a person; the guide says how to
  run the other tables meanwhile.
* Nothing else: no threads, no scheduler, no restart loop. Spark already runs every query on
  its own thread with its own checkpoint, so a failed query stops alone; restarting is the
  orchestrator's retry, or `start_many` again for the stopped tables.
* "Run one job per stream" (ADR 0018) means one runner per checkpoint: a job running
  `start_many` owns the checkpoints of its tables, and no table is in two jobs.
* `docs/guides/many-tables.md` documents the pattern: cluster sizing (`numPartitions` per
  table against the cores), one job against one job per table, failure isolation and
  restarts, finalization and metrics per stream, Databricks.

## Considered: one stream over several capture instances
One query reading several capture instances of the same schema, or of different ones.
Rejected:
* The offset becomes a map of capture instance to LSN, or a single LSN whose batches span
  every table. Either changes the offset contract users' checkpoints hold (invariant 1).
* A purged range of one table fails the query of every table (invariant 4), and a
  re-snapshot generation (ADR 0018) would re-snapshot or restart them all.
* The output schema is one per query: different tables need a union schema or a variant
  column, and the sink a split into one target per table. Same-schema tables (shards) are
  rare enough to stay as separate streams.
* Finalization is per table: a shared end offset would hold every table's verdict back to
  the slowest one.
* What it would give: one planning loop and one driver connection instead of one per table,
  and a common LSN across the tables of one batch. The first is cheap at tens of tables; the
  second a consumer gets from each table's `finalized_until` (a period is complete for a
  join when it is final in every table).

## Consequences
* Each table costs a driver connection, a planning loop and a facts commit per trigger;
  the executor cores and SQL Server connections are shared, sized through `numPartitions`
  per table (ADR 0011's `auto` gives every stream all the cores).
* A large bootstrap delays the start of the tables after it in the list.
* No state of its own: the checkpoints, `app_id`s, generations and metrics directories are
  exactly those of `to_delta`, so moving a table between `start_many` and a job of its own
  keeps its progress as long as its checkpoint and `app_id` stay the same.
* Invariants unchanged. Tested in `tests/test_fanout.py` with the fake and Delta: two
  capture instances, each bronze gets only its rows, the facts carry both `app_id`s, and a
  stream that fails on a purged range leaves the other running.

## Amendment: the driver's cost per stream, measured

The guide had no figure for what a stream costs the driver, and the decision above called
one planning loop per table "cheap at tens of tables" without measuring it.

**Method.** Lab check t11 (`lab/checks/t11_fanout_cost.py`, no SQL Server): `start_many`
over 1, 5, 10 and 20 tables of the file-backed fake, each committing a row a second, with a
5-second `processingTime` trigger and one shared facts table. The executors run in JVMs of
their own (`local-cluster[2,4,2048]`), so the driver JVM and its child processes are the
driver alone. Processes, PSS and threads come from `/proc`; the heap is read after a full
GC; durations come from the queries' progress, over a 90-second window (60 on Spark Connect)
that starts once every query has run three batches. A run of one stream first warms the
JVM. Each size is measured against the session before its first start. Host: WSL2,
16 vCPUs, 12 GB, PySpark 4.2.0, delta-spark 4.4.0, the package imported from a Windows
mount (slow imports). Other test suites ran on the host meanwhile (load average 13 to 22),
so the timings are pessimistic and the resource counts are not.

**Results.** Classic (`t11_fanout_cost-20261013T224322Z.json`), per running stream:

| | 1 | 5 | 10 | 20 |
|---|---|---|---|---|
| planning processes (`python_streaming_source_runner`), PSS each | 1, 51 MB | 5, 49 MB | 10, 52 MB | 15 of 20, 51 MB |
| driver JVM heap after GC, per stream | 104 MB (caches the session fills once) | 10.8 MB | 8.0 MB | |
| driver JVM threads, per stream | 24 | 25.6 | 19.6 | |
| threads of the driver's Python process, per stream | 2 | 1.4 | 1.1 | |
| `latestOffset` per trigger, median (p95) | 3 (5) ms | 21.5 (53) ms | 56 (98) ms | 105 (180) ms |
| `queryPlanning` per trigger, median (p95) | 17 (20) ms | 24.5 (31) ms | 16.5 (48) ms | 17 (54) ms |
| `addBatch` (the sink) per trigger, median | 11.9 s | 19.5 s | 20.0 s | 24.6 s |
| `start_many` | 20 s | 68 s | 128 s | 287 s |

At every size the planning processes exited with their queries. The fake's `latestOffset`
reads one mapping file that every table's commits grow, which accounts for part of its
growth with the number of tables; against SQL Server it is a round trip instead. A one-row
Delta append took 2 to 3 seconds on this host, so `addBatch` measures the host more than
the sink. At 20 streams the per-stream rows are left empty: 5 queries failed (below).

Spark Connect, through a local server (`t11_fanout_cost-20261013T230703Z.json`, 1 and 5
streams): the same planning process (51 MB) plus a `foreach_batch_worker` per stream (96
to 101 MB), where the server runs the `foreachBatch` function; about 50 server threads per
stream; `latestOffset` 3 and 13 ms, `queryPlanning` 10 ms.

**Findings.**

* Each running stream keeps one Python process on the driver for its query's life, about
  50 MB, and a Connect server one more, about 100 MB. JVM heap (about 10 MB) and threads
  (about 20; about 50 on Connect) come next. Memory is not the first limit: 20 streams hold
  about 1 GB of Python processes on a classic driver.
* Spark gives each of those processes 10 seconds to start and connect back
  (`PythonWorkerFactory`, not configurable). Importing PySpark, PyArrow and this package
  took 4.5 to 5 seconds alone on this host. Started back to back, 9 of 20 queries failed at
  their start with "Python worker failed to connect back"
  (`t11_fanout_cost-20261013T220938Z.json`); with the change below, 5 of 20,
  those started while 13 streams kept the host busy. On Spark Connect the
  `foreachBatch` worker has the same limit, and missing it makes `writeStream.start()`
  raise, so `start_many` stops the queries it started and raises (seen once at 5 streams).
* Planning takes milliseconds per trigger. The sink's commits, run from the driver, take
  the trigger's time: when a trigger outlasts its interval, Spark logs "Current batch is
  falling behind", and the driver sets the pace.

**Decision.** `start_many` starts each stream once the previous query has started its
source (its status message no longer starts with "Initializing"), or stopped, or two
minutes passed. The planning processes then start one at a time instead of together; it
costs each start the time its source takes to start, which the query spends anyway.

**Guidance** (`docs/guides/many-tables.md`): about 10 always-on streams per driver as a
starting point. Past that, a longer `processingTime` interval (fewer plans and facts
commits; the per-stream memory stays), tables in groups with `availableNow` (each group's
processes exit before the next starts), or more jobs. The one-stream-per-table decision
stands: a stream over several capture instances would save the per-stream process, but
not the sink's work, which sets the pace.

Not measured: a Databricks driver, and planning against a real SQL Server.
