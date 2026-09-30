# Running on Databricks (classic compute)

Nothing in `mssql_cdc` imports Databricks APIs; the same package runs on any Spark
4.2+ runtime, and on DBR 18.2+. Platform-specific concerns:

1. **Runtime.** The source needs the Python data source streaming API with
   admission control and `Trigger.AvailableNow` (Spark 4.2, SPARK-55304). DBR 18.2
   ships Spark 4.1.0 with that API backported: `t5_engine` passes there (dedicated,
   single node). Run `lab/checks/t5_engine.py` in a notebook to confirm on another
   runtime before anything else. `t5` needs a single-node cluster, because its
   file-backed fake needs a local path shared by driver and executors. Leave `--path`
   at its default (a fresh temp dir): a path without a scheme is local to Python but
   resolves to DBFS for the Spark checkpoint, so a fixed one survives the cluster and
   the next run fails with "does not support recovering from checkpoint location".
2. **Access mode.** Use dedicated. Python streaming data sources on standard access
   mode are untested.
3. **Install.** In a job, a `requirements` task library pointing to a
   `requirements.txt` in the workspace or a Volume that holds a git reference (the
   `pypi` library type takes only a name and version):

   ```text
   mssql-cdc-pyspark @ git+https://github.com/emanuel-luis/mssql-cdc-pyspark.git@<commit>
   ```

   ```json
   "libraries": [{"requirements": "/Workspace/Users/<you>/requirements.txt"}]
   ```

   In a notebook, `%pip install` with the same line works too. Leave out the `[spark]`
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
4. **Network.** The SQL Server must be reachable from every worker (executors open
   their own connections in `read()`).
5. **Names and paths.** Unity Catalog managed tables for bronze, facts and control;
   checkpoints in a Volume.
6. **Tables too big to snapshot.** A snapshot must finish within the CDC retention (3 days by
   default); at the 4-11k rows/s measured against a production source, a table of billions
   of rows cannot. Seed the target from an existing copy of the table (for example one
   already in the lakehouse) and start the stream at the LSN that copy is consistent with
   (`startingLsn`). Never use `bootstrap=True` or `on_data_loss="resnapshot"` on it: both
   snapshot the table ([README](../README.md#tables-too-big-to-snapshot)).
7. **Consumers.** Gate downstream work on `table_finalization`:
   * Lakeflow Jobs: a table update trigger on the control table, then a task that
     reads `finalized_until` and an If/else condition (compare epoch numbers).
   * Airflow: `DatabricksSqlSensor` with
     `SELECT 1 FROM lab.cdc.table_finalization WHERE table_name = 'bronze_orders' AND finalized_until >= '{{ data_interval_end }}'`.

Checks to re-run on Databricks: `t5_engine`, `t6_delta_semantics --schema <catalog.schema>`,
`t7_end_to_end --schema <catalog.schema> --checkpoint /Volumes/...`.
