# Running on Databricks

Nothing in `mssql_cdc` imports Databricks APIs. What has run on Databricks is classic
compute, DBR 18.2, dedicated access mode, single node (items 1 and 3); other runtimes and
classic compute types are untested. Serverless compute is a Spark Connect platform the
library is built for, with `availableNow` only and no cache API (item 2); 0.6.0rc3
ran the whole library on Databricks serverless (environment 4) with the fake backend: stream, chunked bootstrap and backfill, silver, `reconcile` and finalization; its SQL Server path has not run there, since serverless needs a network path to your SQL Server. Keep the metrics in a Volume (item 5): a `metricsPath` URI goes
through `pyarrow.fs`, which has no `dbfs:/` and gets no Unity Catalog credentials.
Platform-specific concerns:

1. **Runtime.** `maxCommitsPerBatch` needs the Python data source streaming API with
   admission control and `Trigger.AvailableNow` (Spark 4.2, SPARK-55304); without it every
   batch reads up to `max_lsn` (CI runs the source on PySpark 4.1.1). DBR 18.2
   ships Spark 4.1.0 with that API backported: `t5_engine` passes there (dedicated,
   single node). Run `lab/checks/t5_engine.py` in a notebook to confirm on another
   runtime before anything else. `t5` needs a single-node cluster, because its
   file-backed fake needs a local path shared by driver and executors. Leave `--path`
   at its default (a fresh temp dir): a path without a scheme is local to Python but
   resolves to DBFS for the Spark checkpoint, so a fixed one survives the cluster and
   the next run fails with "does not support recovering from checkpoint location".
2. **Access mode.** On classic compute, use dedicated. Python streaming data sources on
   standard access mode are untested. Serverless compute and Databricks Connect are Spark
   Connect clients: the library calls only what any Spark Connect server has
   ([Spark Connect](getting-started/installation.md#spark-connect)), tested against a local
   one with the fake backend, also with the cache API refused as serverless refuses it.
   Neither has run it yet. Serverless allows no `processingTime` trigger, Spark's
   default included: pass `trigger={"availableNow": True}` to `to_delta` and
   `start_many`, run the job on a schedule, and advance the verdict after
   `awaitTermination()` (`advance`, or `track` then `join`).
3. **Install.** In a job, a `pypi` task library pinned to a release (checked with 0.1.0 on
   DBR 18.2, dedicated, single node: installed with `mssql-python`, bootstrap and stream
   ran). A release candidate installs only by its exact pin:

   ```json
   "libraries": [{"pypi": {"package": "mssql-cdc-pyspark==0.6.0"}}]
   ```

   For an unreleased commit, a `requirements` task library pointing to a
   `requirements.txt` in the workspace or a Volume that holds a git reference (the `pypi`
   library type takes only a name and version):

   ```text
   mssql-cdc-pyspark @ git+https://github.com/emanuel-luis/mssql-cdc-pyspark.git@<commit>
   ```

   ```json
   "libraries": [{"requirements": "/Workspace/Users/<you>/requirements.txt"}]
   ```

   In a notebook, `%pip install mssql-cdc-pyspark==0.6.0` (or the git line) works too. Leave out the `[spark]`
   extra: PyPI `pyspark` conflicts with the runtime's own Spark. `mssql-python`, installed
   with the package, loads `libltdl7` (and the Kerberos libraries) on every node that
   opens a connection; add a
   cluster init script that installs them if the image lacks them:

   ```bash
   #!/bin/bash
   set -euo pipefail
   if ! ldconfig -p | grep -q 'libltdl.so.7'; then
     apt-get update -qq
     DEBIAN_FRONTEND=noninteractive apt-get install -y -qq libltdl7 libkrb5-3 libgssapi-krb5-2
   fi
   ```

   Serverless compute takes no init scripts and needs none: `mssql-python` imports and loads
   there as it is (checked on 2026-10-09). List the package in the serverless job's
   environment dependencies, or `%pip install` it in the notebook.
4. **Network.** The SQL Server must be reachable from every worker (executors open
   their own connections in `read()`). `to_delta`'s bootstrap and
   `on_data_loss="resnapshot"` pre-flight, `seed()` and `backfill()` run in the Python
   process that calls them, which connects to SQL Server and, for the pre-flight, reads
   the checkpoint itself: from Databricks Connect, that is your machine, which must reach
   SQL Server and see the checkpoint path. Serverless compute reaches only the networks the
   workspace lets it reach: a SQL Server in a private network needs that path set up first
   (serverless network connectivity, or a public endpoint behind a firewall rule). That is
   yours to provide: the library cannot test it for you.
5. **Names and paths.** Unity Catalog managed tables for bronze, facts and control;
   checkpoints in a Volume, where `to_delta` keeps the metrics files too. With a checkpoint
   that is a URI, set `metricsPath` to a Volume path. A URI `metricsPath` is written through
   `pyarrow.fs` with the credentials each node has of its own: `dbfs:/` is a `ValueError`
   when the query starts, and `abfss://` or `s3://` work only where every node has
   credentials pyarrow finds on its own (environment variables, an instance profile):
   pyarrow does not use Unity Catalog's storage credentials. A path without a scheme (`/mnt/...`, `/tmp/...`) is DBFS for
   Spark but the driver's local disk for Python: with `on_data_loss="resnapshot"`, when the
   facts table holds batches of the stream but Python finds nothing of Spark's in the
   checkpoint, `to_delta` raises `ValueError` saying the two may see different directories.
6. **Tables too big to snapshot.** A table of billions of rows cannot be snapshotted within
   the CDC retention: seed it from a copy already in the lakehouse with `seed()`, then
   `to_delta(bootstrap=True)` starts from the seed without reading the table
   ([Bootstrap](guides/bootstrap.md#tables-too-big-to-snapshot)). Without a copy, take a
   chunked snapshot: the stream task runs `to_delta(..., snapshot="chunked")` and a second
   task of the same job calls `backfill()` until it is done
   ([Bootstrap](guides/bootstrap.md#chunked-snapshots)); not run on Databricks yet.
7. **Schema changes.** Type widening on a Unity Catalog bronze table is the same statement,
   `ALTER TABLE <catalog>.<schema>.<table> SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')`;
   `to_delta(snapshot_on_switch=True)` needs `metricsPath` when the checkpoint is a URI
   rather than a Volume, and so do the `schema_change` and `capture_instance_switched` facts
   rows the DBA waits for before disabling an old capture instance. Neither has run on
   Databricks yet ([Schema changes](guides/schema-changes.md)).
8. **Consumers.** Gate downstream work on `table_finalization`:
   * Lakeflow Jobs: a table update trigger on the control table, then a task that
     reads `finalized_until` and an If/else condition (compare epoch numbers).
   * Airflow: `DatabricksSqlSensor` with
     `SELECT 1 FROM lab.cdc.table_finalization WHERE table_name = 'lab.cdc.bronze_orders' AND finalized_until >= '{{ data_interval_end }}'`.

Checks to re-run on Databricks: `t5_engine`, `t6_delta_semantics --schema <catalog.schema>`,
`t7_end_to_end --schema <catalog.schema> --checkpoint /Volumes/...`.
