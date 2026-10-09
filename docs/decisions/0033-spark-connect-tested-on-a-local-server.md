# 0032: Spark Connect, tested against a local Connect server

**Status:** accepted  
**Date:** 2026-10-09T11:39:43-03:00

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

Databricks serverless adds limits of its own: no DataFrame cache API (the sink's `persist()`
is a separate change), only `Trigger.AvailableNow`, and it reaches a SQL Server in a private
network only through what the workspace configures. Measured on 2026-10-09: on serverless,
`mssql-python` imports and loads without an init script, and the private SQL Server used for
the Databricks runs is out of its reach.

## Decision
* Spark Connect is a supported way to run the library. The core calls only APIs a Connect
  session has; where a classic session tells it more (the cores behind
  `numPartitions=auto`), it tries the call and falls back when it fails. It never asks
  which platform it runs on (invariant 10). `tests/test_package.py` fails on a
  `sparkContext`, `_jvm`, `_jsc`, `_jdf` or `SparkContext` in `src/` other than
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
  calls a classic driver makes.
* The connect tests are also marked `spark` and `delta`, so the loops that leave those out
  leave them out too, and the default run deselects `connect`. They run alone, in CI's
  `connect` job: a Connect session turns the whole Python process to Connect
  (`SPARK_CONNECT_MODE_ENABLED`; `delta.tables` then dispatches to `delta.connect`), so
  they cannot share a run with the classic suite.
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
* The connect suite takes about five minutes (the server's start included), in a job of its
  own; its first run on a cold Ivy cache downloads the Delta Connect jars.
* Not covered by the suite: Databricks serverless and Databricks Connect themselves, whose
  runs are recorded where they happen, and the SQL Server backend through a Connect server,
  which runs the same code on the executors as on a classic cluster.
