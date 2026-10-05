# 0029: Driver-side retries, and an optional lock timeout

**Status:** accepted  
**Date:** 2026-10-05T12:23:04-03:00

## Context
The stream's reader opens one connection on the driver and keeps it for the query's life:
`initialOffset`, `latestOffset`, `reportLatestOffset`, `partitions` and
`prepareForTriggerAvailableNow` all go through it. Any error on it ended the query: an
availability group failover, an Azure SQL reconfiguration, a deadlock victim on a planning
read. Executors never needed more: each task opens its own connection and Spark retries a
failed task. With `start_many` (ADR 0027), one such error stopped that table until the next
run.

ODBC Driver 18 reconnects a connection that broke while idle, once by default
(`ConnectRetryCount=1`): a session killed between two polls comes back on its own. It does not
cover a statement in flight, nor an outage longer than its retry.

Only the login timeout was set, and no `LOCK_TIMEOUT`. A read of the source table under READ
COMMITTED waits as long as a writer holds a lock it needs: a snapshot chunk behind a long OLTP
transaction, a plan's count, `reconcile()`'s buckets. The task then hangs with no error, and
nothing retries a hang.

Neither driver exposes SQL Server's error number. `mssql-python` raises one DB-API class per
SQLSTATE and keeps only a text for it, not the SQLSTATE; `arrow-odbc` prints it in the message
(`State: 08S01, Native error: 10054, Message: ...`).

## Decision
* One decorator, `_retrying`, on those five driver-side methods. On a transient error it
  closes and drops the client, waits, and runs the method again on a new connection, up to 3
  times, 1-2, 2-4 and 4-8 seconds apart (exponential backoff with jitter, about 14 seconds
  at most), with a WARNING each time; then it raises the last error. A run re-reads from SQL
  Server everything it plans with, and what it leaves behind is idempotent (event files named
  by what a replan reproduces, the instance names seen), so a retry is safe.
* Transient means SQLSTATE 08xxx (a broken or refused connection), HYT00 or HYT01 (a
  timeout) or 40001 (a deadlock victim). For `mssql-python`, an `OperationalError` whose text
  is the one it gives those SQLSTATEs: it also raises `OperationalError` for 28000 (a login
  refused) and HY000 (most server errors), which a new connection does not fix. For
  `arrow-odbc`, the SQLSTATE in the message. Never `DataLossError`, `SchemaChangedError`,
  `ValueError` or `PermissionError`, which need a decision, not a retry.
* The count and the backoff are constants, not options, until a site needs to ride out a
  longer outage.
* `lockTimeoutMs`, a source option, off by default: `SET LOCK_TIMEOUT n;` in front of every
  read of the source table, through the prefix `_isolated` already builds for SNAPSHOT
  isolation (the snapshot's ranges, the plans' counts and seeks, `reconcile()`'s counts and
  tiles). A blocked read fails with error 1222 after `n` ms: Spark retries the task, and a
  planning read raises it to the caller. The change table reads do not take it.
* No query timeout yet: one would also kill a long, healthy snapshot fetch. It waits for a
  measurement of the longest legitimate read.

## Consequences
* An outage longer than the retries still stops the query, for the job's own retry, as
  before. Executors keep Spark's task retry only (`spark.task.maxFailures`: 4 on a cluster, 1
  in local mode).
* `tests/integration`: with `ConnectRetryCount=0`, a killed session fails with
  `OperationalError` "Communication link failure" (08S01), which counts as transient, and the
  reader answers on a new connection; with `lockTimeoutMs=500`, a count and a seek blocked by
  a writer fail with error 1222 within seconds.
* `mssql-python` 1.x loses the message of an error met after the first rows were fetched
  (`An error occurred with SQLSTATE code: ; DDBC Error: Unknown DDBC error`): a chunk read that
  times out after returning rows fails with it, and a connection broken in the middle of a
  fetch carries no SQLSTATE either, so the driver does not retry that one.
* Sent without parameters, `SET LOCK_TIMEOUT` can stay on the session, as SNAPSHOT isolation
  does (`tests/integration`), and bound the same client's later metadata reads too. The client
  sends it before every read of the source table either way.
