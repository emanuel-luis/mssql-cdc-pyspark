# Installation

## Requirements

| What | Version | Notes |
|---|---|---|
| Python | 3.10+ | |
| Spark | 4.2+ | or a runtime with the Python data source admission control backported, such as Databricks Runtime 18.2+ ([Databricks](../DATABRICKS.md)) |
| Delta Lake | delta-spark 4.4+ locally | for `to_delta`, the facts and control tables and `apply_changes`; Spark platforms ship it |
| Java | 17 or 21 | only for a local Spark |
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

## Locally

The `spark` extra brings PySpark and delta-spark:

```bash
pip install "mssql-cdc-pyspark[spark]"
```

`mssql_cdc.spark.get_spark()` then returns the active session, or builds a local one with
Delta configured and the session time zone set to UTC. Native Windows also needs
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
local container.

`arrow-odbc` is the alternative where Microsoft's ODBC Driver 18 is already installed:
`pip install "mssql-cdc-pyspark[arrow-odbc]"` and the option `backend=arrow-odbc`. The test
suite does not exercise it yet ([ADR 0003](../decisions/0003-mssql-python-default-backend.md));
the survey behind the choice is in [Drivers](../CONNECTORS.md).

## Check the install

```python
import mssql_cdc

print(mssql_cdc.__version__)
```

On Spark 4.2+ and on runtimes with the admission control backport, the
`maxCommitsPerBatch` option and `Trigger.AvailableNow` work. On an older Spark every batch
reads up to the newest change captured, `maxCommitsPerBatch` has no effect, and that path
is not tested.

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
