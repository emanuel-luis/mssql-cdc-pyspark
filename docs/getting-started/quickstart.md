# Quickstart

Stream a CDC-tracked table from SQL Server 2022 in Docker into a local Delta table, with
facts and a completeness verdict. The last section does the same against a SQL Server of
your own.

## Before you start

You need Docker, [uv](https://docs.astral.sh/uv/), Java 17 or 21 and Git. On Linux and WSL,
the driver also needs system libraries that uv does not install:

```bash
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2
```

Native Windows needs `HADOOP_HOME` and `PYSPARK_PYTHON` set; see
[Development](../DEVELOPMENT.md#windows-native-powershell). The SQL Server image is
linux/amd64 only: on Apple Silicon, enable Rosetta in Docker Desktop.

## 1. Start SQL Server and create the lab tables

```bash
git clone https://github.com/emanuel-luis/mssql-cdc-pyspark.git
cd mssql-cdc-pyspark
cp .env.example .env                    # set MSSQL_SA_PASSWORD
docker compose up -d                    # SQL Server 2022 with SQL Server Agent
uv sync                                 # .venv with Spark, Delta and mssql-python

uv run python -m lab.workload setup     # database cdc_lab, dbo.customers, dbo.orders, CDC
uv run python -m lab.workload seed      # Faker rows in both tables
```

SQL Server Agent must run: CDC capture and cleanup are Agent jobs, and the compose file
enables it. Wait until `docker compose ps` shows the container healthy before `setup`.

## 2. Run the pipeline

```bash
uv run python examples/local_pipeline.py
```

<!-- fmt: off -->
```python title="examples/local_pipeline.py"
--8<-- "examples/local_pipeline.py"
```
<!-- fmt: on -->

The first run:

1. takes a snapshot of `dbo.orders` into the bronze table (`bootstrap=True`), stamped with
   the LSN recorded before the read;
2. streams the changes committed after that LSN, in batches of at most 500 commits, and
   stops when it has caught up (`availableNow`);
3. writes one facts row for the snapshot and one per micro-batch;
4. advances `finalized_until` for the table, only after the data is committed.

The first Spark run with Delta downloads its jars from Maven Central. Tables and the
checkpoint go under the directory in `MSSQL_CDC_WORK`, by default `mssql-cdc-work` in the
system temp directory.

## 3. Change the source and run again

```bash
uv run python -m lab.workload stream --duration 60   # inserts, updates, deletes for a minute
uv run python examples/local_pipeline.py
```

The second run resumes from the checkpoint: it finds the snapshot already in bronze, reads
only the commits made since, and moves `finalized_until` forward.

## 4. Look at the tables

```python
import os
import tempfile

from mssql_cdc.spark import get_spark

work = os.environ.get("MSSQL_CDC_WORK", os.path.join(tempfile.gettempdir(), "mssql-cdc-work"))
spark = get_spark()

# the change log: operation 0 is the snapshot, 1-4 are SQL Server's codes
bronze = spark.read.format("delta").load(f"{work}/bronze_orders")
bronze.orderBy("_start_lsn", "_command_id", "_seqval", "_operation").show(10)

# one row per micro-batch (event NULL) plus the bootstrap snapshot
facts = spark.read.format("delta").load(f"{work}/ingestion_facts")
facts.select("event", "batch_id", "rows", "end_lsn", "retention_headroom_hours").show()

# the verdict: every period ending at or before finalized_until is complete
spark.read.format("delta").load(f"{work}/table_finalization").show(truncate=False)
```

The columns are described in [Output schema](../reference/output-schema.md) and
[Tables](../reference/tables.md).

## Against your own SQL Server

Install the package ([Installation](installation.md)), enable CDC on the table, and give
the stream a connection string and the capture instance. Table arguments take a catalog
name (`bronze.orders`) or a path; a string with `/` or `:` is a path.

```python
from mssql_cdc import finalization, stream
from mssql_cdc.spark import get_spark

spark = get_spark()
options = {
    "connectionString": "Server=sqlhost,1433;Database=sales;UID=cdc_reader;PWD=...;Encrypt=yes",
    "captureInstance": "dbo_orders",
}
query = stream(spark, options).to_delta(
    "/data/bronze/orders",
    app_id="orders-v1",
    checkpoint="/data/checkpoints/orders",
    facts_table="/data/ops/ingestion_facts",
    trigger={"availableNow": True},
    bootstrap=True,
)
query.awaitTermination()

end = finalization.end_offset_from_progress(query.lastProgress)
print(finalization.advance(spark, "/data/ops/table_finalization", "/data/bronze/orders", end))
```

The columns and their types are read from the capture instance, and commit times are
converted to UTC from the server's time zone. Run the script on a schedule; each run picks
up where the last one stopped.

!!! note "Right after enabling CDC"
    Until the capture job has processed a new capture instance, reads can fail with
    "Capture instance ... not found, the login lacks permission to read it, or capture has
    not processed its creation yet". Retry after a few seconds; if it persists, check the
    login's grants ([Permissions](../guides/permissions.md)) and that SQL Server Agent runs.

## Next steps

- [Streaming into Delta](../guides/streaming.md): triggers, checkpoints, `app_id`, metrics.
- [Bootstrap](../guides/bootstrap.md): what the snapshot does, and tables too big for one.
- [Finalization](../guides/finalization.md): gating downstream jobs on `finalized_until`.
- [Silver tables](../guides/silver.md): a current-state table, one row per key.
