# 0033: Spark Connect, tested against a local Connect server

**Status:** accepted  
**Date:** 2026-10-09T11:39:43-03:00  
**Amended:** 2026-10-09T19:26:50-03:00, the server's Python workers load a `sitecustomize` through which a test refuses the cache API in its `foreachBatch` worker, as serverless does (ADR 0032)

## Context
Databricks serverless compute (the default for new Databricks jobs), Databricks Connect and
any Spark Connect server give user code a client session with no JVM: no `sparkContext`, no
`_jvm`; DataFrames, SQL, `DeltaTable` (through Delta Connect), Python data sources and
streaming query listeners (on a bus in the client) all work through the connection. A
`foreachBatch` function is pickled and runs in a Python process the server starts.

The library was written against classic sessions but already kept to what a Connect session
has: `spark` parameters take one (ADR 0031, amendment 1), `available_cores` falls back to 0
when `sparkContext` fails, a Delta write conflict is recognised by its error class in the
message, the finalization listener stays registered on Connect (ADR 0026, amendment), and the
sink's switch wrapper holds no session. None of it had run against a Connect server: the
finalization guide said "not tested", and the docs listed serverless as unsupported.

Run against a local Connect server (PySpark 4.2.0 with Delta Connect 4.4.0), with the fake
backend: `register`, `to_delta` with `availableNow` and `processingTime`, `track`,
`snapshot`, a chunked bootstrap with `backfill`, `apply_changes`, `reconcile`, and
`start_many` through a data loss and its re-snapshot all passed unchanged, and so did the
state every released wheel wrote (`tests/compat`, run once with its session swapped for a
Connect one). One public helper broke: `get_spark()` builds its session with `.master(...)`,
and with `SPARK_REMOTE` set PySpark refuses a master
(`CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`), so no lab check or example could run through a
Connect server. Past it, t5 and t7 read `numInputRows` from each progress, which a Connect
progress JSON leaves out (the sources' counts are there).

Databricks serverless adds limits of its own: no DataFrame cache API (the sink and `backfill()`
append uncached there, [ADR 0032](0032-facts-without-caching.md)), only `Trigger.AvailableNow`, and it reaches a SQL Server in a private
network only through what the workspace configures. Measured on 2026-10-09: on serverless,
`mssql-python` imports and loads without an init script, and the private SQL Server used for
the Databricks runs is out of its reach.

## Decision
* Spark Connect is a supported way to run the library. The core calls only APIs a Connect
  session has; where a classic session tells it more (the cores behind
  `numPartitions=auto`), it tries the call and falls back when it fails. It never asks
  which platform it runs on (invariant 10). `tests/test_package.py` fails on a
  `sparkContext`, `rdd`, `_jvm`, `_jsc`, `_jdf` or `SparkContext` in `src/` other than
  `available_cores`' guarded one, in every loop.
* `get_spark()` returns a Connect session when `SPARK_REMOTE` is set: the server holds its
  own configuration, so neither the master nor the Delta jars are passed. The active session
  still wins, as before. The lab checks count a batch's rows from its sources, so they run
  through a Connect server as they are: `t5` does in the connect suite.
* `tests/test_connect.py`, marked `connect`: the `connect_server` fixture starts a local
  Spark Connect server in its own JVM (`spark-submit --class
  org.apache.spark.sql.connect.service.SparkConnectServer` from the PySpark package, with
  `io.delta:delta-connect-server` and its relation and command plugins, and
  `protobuf-java` 3.25.1, because Delta Connect's classes need a newer runtime than the
  3.24.4 its POM declares); the test process holds only the client (`connect_spark`). The
  fake backend stands in for SQL Server: the reads run on the server's executors in the
  same code as on a classic cluster, and the client-side SQL Server calls are the same
  calls a classic driver makes. The server's Python workers have
  `tests/connect_site/sitecustomize.py` on their path: in the `foreachBatch` worker it makes
  the cache API raise while the `refuse_caching` fixture's flag file exists, so the sink's
  uncached path ([ADR 0032](0032-facts-without-caching.md)) runs where it runs on serverless.
* The connect tests are also marked `spark` and `delta`, so the loops that leave those out
  leave them out too, and the default run deselects `connect`. They run alone, in CI's
  `connect` job: a Connect session turns the whole Python process to Connect
  (`SPARK_CONNECT_MODE_ENABLED`; `delta.tables` then dispatches to `delta.connect`), so
  they cannot share a run with the classic suite.
* `MSSQL_CDC_TEST_SPARK=connect` hands `connect_spark` to every test that takes `spark`, so
  the SQL Server path runs through the Connect server too, on demand
  (`uv run --group connect pytest -m sqlserver -k ...`). It is not a CI job: a test that
  reaches into the JVM, reads the progress's top-level `numInputRows`, or collects into a
  list from its `foreachBatch` function (which runs on the server under Connect) fails
  under it for that reason alone.
* The Connect client is a dependency group, `connect` (`pyspark[connect]`, and pandas
  below 3, which PySpark 4.2 warns it does not fully support), not an extra: platforms ship
  their own client.
* The docs state the scope: tested against a local Connect server with the fake backend;
  serverless needs `trigger={"availableNow": True}`, which `to_delta` never sets on its
  own; the network path from serverless to SQL Server is the user's to provide.

## Consequences
* The calls that talk to SQL Server themselves (`to_delta`'s bootstrap and pre-flight,
  `snapshot`, `seed`, `backfill`, `reconcile`, `apply_changes(options=...)`) and the
  `track` listener run in the client process: it must reach SQL Server and see the
  checkpoint for the pre-flight, and verdicts stop moving when it exits. This was already
  so from Databricks Connect; the installation guide now says it in one place.
* The connect suite took about five minutes (the server's start included); with the test that
  refuses the cache API, 13 minutes in one local WSL run on 2026-10-09. It has a job of its
  own; its first run on a cold Ivy cache downloads the Delta Connect jars.
* The SQL Server path through the local Connect server, run on 2026-10-09 with
  `MSSQL_CDC_TEST_SPARK=connect` (SQL Server 2022 in Docker): 10 integration tests passed:
  the stream resumed from its checkpoint, the network metrics in the facts, a
  bootstrap with a least-privilege login, a purged range stopping the stream, a re-snapshot
  recovering from it, `seed`, a type change stopping the running query, a chunked bootstrap
  next to a running stream and a writer, silver on a composite key and `reconcile`.
* The rest of the suite through the same server, the same day: 114 of the 125 tests that
  take `spark` passed (the state of every released wheel in `tests/compat` among them). The
  11 others failed on the tests' own means alone: a JVM attribute (`_jsqm`, `sparkContext`,
  `df.rdd`), the top-level `numInputRows` a Connect progress leaves out, a list appended in
  a `foreachBatch` function, which runs on the server, and a `foreachBatch` function from a
  test module the server cannot import. None in the library.
* Not covered: Databricks serverless and Databricks Connect themselves, whose runs are
  recorded where they happen.
