# Running on Databricks (classic compute)

Nothing in `mssql_cdc` imports Databricks APIs; the same package runs on any Spark
4.2+ runtime. Platform-specific concerns:

1. **Runtime.** The source needs the Python data source streaming API with
   admission control and `Trigger.AvailableNow` (Spark 4.2, SPARK-55304). Databricks
   release notes list a backport in DBR 18.x. Run `lab/checks/t5_engine.py` in a
   notebook to confirm on your runtime before anything else.
2. **Access mode.** Use dedicated. Python streaming data sources on standard access
   mode are untested.
3. **Install.** `%pip install "mssql-cdc-pyspark[mssql] @ git+https://github.com/emanuel-luis/mssql-cdc-pyspark.git"`
   or a cluster library. `mssql-python` needs `libltdl7`, `libkrb5-3`,
   `libgssapi-krb5-2` on the image (unverified on DBR; `t5` does not cover it, `t7` does).
4. **Network.** The SQL Server must be reachable from every worker (executors open
   their own connections in `read()`).
5. **Names and paths.** Unity Catalog managed tables for bronze, facts and control;
   checkpoints in a Volume.
6. **Consumers.** Gate downstream work on `table_finalization`:
   * Lakeflow Jobs: a table update trigger on the control table, then a task that
     reads `finalized_until` and an If/else condition (compare epoch numbers).
   * Airflow: `DatabricksSqlSensor` with
     `SELECT 1 FROM lab.cdc.table_finalization WHERE table_name = 'bronze_orders' AND finalized_until >= '{{ data_interval_end }}'`.

Checks to re-run on Databricks: `t5_engine`, `t6_delta_semantics --schema <catalog.schema>`,
`t7_end_to_end --schema <catalog.schema> --checkpoint /Volumes/...`.
