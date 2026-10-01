# 0027: Many tables: one stream per capture instance, started by a fan-out helper

**Status:** accepted  
**Date:** 2026-10-01T16:30:09-03:00

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
