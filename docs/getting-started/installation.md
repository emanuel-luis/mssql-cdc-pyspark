# Installation

!!! note "These pages document `main`"
    The latest release on PyPI may be older, and `pip install mssql-cdc-pyspark` skips
    release candidates: a feature not released yet needs `pip install --pre
    mssql-cdc-pyspark` or the candidate's exact pin, such as `==0.2.0rc1`. What each release
    has is in the
    [changelog](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/CHANGELOG.md).

## Requirements

| What | Version | Notes |
|---|---|---|
| Python | 3.10–3.13 | CI runs 3.11 on every push and all four weekly |
| Spark | 4.1+ | CI runs 4.2.0, and the source alone on 4.1.1; 4.0 is untested. `maxCommitsPerBatch` needs 4.2+ or a runtime with the Python data source admission control backported, such as Databricks Runtime 18.2+ ([Databricks](../DATABRICKS.md)) |
| Delta Lake | delta-spark 4.4+ locally | for `to_delta`, the facts and control tables and `apply_changes`; Spark platforms ship it |
| Java | 17 | only for a local Spark; CI runs 17, 21 is untested |
| SQL Server | CI runs 2022 | CDC enabled on the database and the table, SQL Server Agent running (capture and cleanup are Agent jobs) |

Every Spark node that runs tasks must reach SQL Server: executors open their own
connections to read their part of each batch.

## On a Spark platform

Databricks, EMR, Dataproc, Fabric and similar platforms ship their own PySpark and Delta.
Install the package alone:

```bash
pip install mssql-cdc-pyspark
```

Leave out the `[spark]` extra there: PySpark from PyPI conflicts with the runtime's own
Spark. On Databricks, install it as a job library and add an init script for the driver's
system libraries; both are in [Running on Databricks](../DATABRICKS.md).

Of these platforms, only Databricks classic compute has run it, on a single node.
Databricks serverless is a Spark Connect platform (below), run with the fake backend (its
SQL Server path needs a network path from serverless, not tried). The metrics need a
[metricsPath](../reference/options.md#metricspath) every node sees: a local or FUSE path, or,
where there is none (EMR and Dataproc by default), an object store URI `pyarrow.fs` opens
with the credentials the nodes have (`s3://`, `gs://`).

## Spark Connect

Databricks serverless compute, Databricks Connect and any Spark Connect server give your code
a client session with no JVM; the queries run on the server. The library calls only APIs a
Connect session has (DataFrames, SQL, `DeltaTable`, Python data sources, streaming query
listeners). Where a classic session can tell it more, such as the cores behind
`numPartitions=auto`, it tries the call and falls back when it fails, without asking which
platform it runs on.

Tested with a PySpark 4.2 client against a local Spark Connect server (PySpark 4.2 with
Delta Connect 4.4), with the fake backend in place of SQL Server: `to_delta` with
`availableNow` and with `processingTime`, `track`, `snapshot`, a chunked bootstrap with
`backfill`, `apply_changes`, `reconcile`, and `start_many` through a data loss and its
re-snapshot, and the sink, `backfill` and `reconcile` with the cache API refused in the
client and in the server's `foreachBatch` worker, as Databricks serverless refuses it
(`tests/test_connect.py`, CI's `connect` job). The SQL Server path through the
same server is tested on demand: integration tests against SQL Server 2022 in Docker
(bootstrap, re-snapshot, chunked bootstrap, `seed`, silver, `reconcile`, a type change
stopping the query) pass with their session swapped for a Connect one
([Development](../DEVELOPMENT.md#tests)).

What runs where:

- On the server: the stream (the reader plans in a Python worker of the server's driver and
  reads on its executors) and the sink, a `foreachBatch` function that runs in a Python
  process the server starts. Install the library where the server's Python finds it.
- In your process: the calls that talk to SQL Server themselves (`to_delta`'s bootstrap and
  `on_data_loss="resnapshot"` pre-flight, `snapshot()`, `seed()`, `backfill()`,
  `reconcile()`, `apply_changes(options=...)`) and the listener `track()` adds. Your process
  must reach SQL Server, and see the checkpoint path for the pre-flight
  ([checkpoint](../reference/options.md#checkpoint)); `track()` advances the verdict only
  while it runs.
- `numPartitions=auto` is the CPU count of the node that plans: a Connect session has no
  `sparkContext` to count the cluster's cores.

A Spark Connect server of your own needs Delta Connect for the `DeltaTable` calls:
`io.delta:delta-connect-server_<spark version>_2.13` with its relation and command plugins,
as the `connect_spark` fixture in `tests/conftest.py` starts one. Databricks has its own.
For Databricks serverless, see [Databricks](../DATABRICKS.md).

## Locally

The `spark` extra brings PySpark and delta-spark:

```bash
pip install "mssql-cdc-pyspark[spark]"
```

`mssql_cdc.spark.get_spark()` then returns the active session, or builds a local one with
Delta configured and the session time zone set to UTC; with `SPARK_REMOTE` set, it returns a
session on that Spark Connect server instead. Native Windows also needs
`HADOOP_HOME` (winutils) and `PYSPARK_PYTHON`; see
[Development](../DEVELOPMENT.md#windows-native-powershell).

An unreleased commit installs from Git:

```bash
pip install "mssql-cdc-pyspark @ git+https://github.com/emanuel-luis/mssql-cdc-pyspark.git@<commit>"
```

## The driver

The default driver, `mssql-python`, is installed with the package and fetches rows
straight into Arrow batches. Its Windows wheels bundle what they need. On Linux it loads
system libraries that pip does not install:

```bash
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2
```

The connection string uses ODBC keywords, for example
`Server=host,1433;Database=db;UID=user;PWD=secret;Encrypt=yes`; add
`TrustServerCertificate=yes` only for a server with a self-signed certificate, such as a
local container. How to quote a password and authenticate without one:
[connectionString](../reference/options.md#connectionstring).

`mssql-python` depends on `mssql-python-odbc`, which holds Microsoft's ODBC Driver 18
binaries under Microsoft's license, not MIT: every default install brings them in, and
license scanners flag them. Where that matters, install without them and use
[arrow-odbc](#arrow-odbc) with a driver installed separately under its EULA:

```bash
pip install --no-deps mssql-cdc-pyspark
pip install pyarrow arrow-odbc
```

`mssql-python` is imported only when it opens a connection, so the other backends work
without it.

### arrow-odbc

`arrow-odbc` is the alternative where Microsoft's ODBC Driver 18 for SQL Server is installed
already, or can be. Every node that runs tasks needs unixODBC and the driver. On Ubuntu,
after adding Microsoft's package repository as its
[install guide](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
shows:

```bash
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc
pip install "mssql-cdc-pyspark[arrow-odbc]"
```

Then set the option `backend=arrow-odbc`; the connection string stays the same. CI runs the
integration tests that read through the backend (14 of them) on both; the heartbeat, facts
metrics, re-snapshot, silver, type changes and dropped columns run on mssql-python only.
Where they differ: a value longer than 64 KiB in a `(max)`, `text`, `ntext`, `xml` or
`image` column fails the read with arrow-odbc, which has to size its buffers, while
mssql-python reads any length
([ADR 0003](../decisions/0003-mssql-python-default-backend.md), Amendment 2). The survey
behind the choice is in [Drivers](../CONNECTORS.md).

## Check the install

```python
import mssql_cdc

print(mssql_cdc.__version__)
```

On Spark 4.2+ and on runtimes with the admission control backport, the
`maxCommitsPerBatch` option and `Trigger.AvailableNow` work. On an older Spark every batch
reads up to the newest change captured and `maxCommitsPerBatch` has no effect: CI runs that
path on PySpark 4.1.1, with the fake backend and without Delta.

Without PySpark the import fails with an `ImportError` that says how to get it.

## Prepare SQL Server

A database owner enables CDC on the database (this one needs sysadmin) and on each table
to stream:

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL;
```

The capture instance is then `dbo_orders`, the name the stream's `captureInstance` option
takes. The table needs a primary key or a unique index for [snapshots](../guides/bootstrap.md)
to split the read and for [silver tables](../guides/silver.md) to find the key on their
own.

- The login the stream uses needs a few grants; see [Permissions](../guides/permissions.md).
- On SQL Server 2022 and Azure SQL the stream reads the server's time zone itself. On older
  versions it applies the server's current UTC offset, which is exact only for zones without
  daylight saving: elsewhere set the `sourceTimeZone` option to the server's Windows zone
  name ([Options](../reference/options.md)).
- On a quiet database the stream's end offset trails real time by up to about 5 minutes, so
  an hour becomes final up to about 5 minutes after it ends; the optional
  [heartbeat job](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/sql/heartbeat.sql)
  brings that to about 10 seconds ([Finalization](../guides/finalization.md)).

## See also

- [Quickstart](quickstart.md): a first stream against SQL Server in Docker.
- [Development](../DEVELOPMENT.md): working on the library itself, per platform.
