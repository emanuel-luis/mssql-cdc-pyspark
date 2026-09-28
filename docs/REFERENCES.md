# References

Primary sources behind the design.

## SQL Server CDC (Microsoft Learn)

* About CDC: https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server
* Work with change data (incremental pattern, wait loop, dummy entries): https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/work-with-change-data-sql-server
* Change table columns: https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql
* `cdc.lsn_time_mapping`: https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-lsn-time-mapping-transact-sql
* `sys.fn_cdc_get_max_lsn`: https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-max-lsn-transact-sql
* `sys.fn_cdc_get_min_lsn`: https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-min-lsn-transact-sql
* Administer and monitor (cleanup, latency): https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/administer-and-monitor-change-data-capture-sql-server
* Known issues: https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/known-issues-and-errors-change-data-capture
* Azure SQL Database CDC: https://learn.microsoft.com/en-us/azure/azure-sql/database/change-data-capture-overview

## Spark Python Data Source API

* Tutorial: https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html
* API source (v4.2.0): https://github.com/apache/spark/blob/v4.2.0/python/pyspark/sql/datasource.py
* Read limits / AvailableNow mixin: https://github.com/apache/spark/blob/v4.2.0/python/pyspark/sql/streaming/datasource.py
* SPARK-55304 (admission control + AvailableNow for Python sources): https://github.com/apache/spark/pull/54085
* DSv2 `Changelog` (Spark 4.2 CDC): https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/Changelog.java

## Delta Lake

* Idempotent writes in foreachBatch: https://docs.delta.io/delta-streaming/#idempotent-table-writes-in-foreachbatch
* Protocol (commitInfo not kept in checkpoints): https://github.com/delta-io/delta/blob/master/PROTOCOL.md
* Databricks: custom commit metadata: https://docs.databricks.com/aws/en/tables/operations/custom-metadata
* Databricks: isolation levels and metadata-change conflicts: https://docs.databricks.com/aws/en/optimizations/isolation-level

## Drivers

* mssql-python: https://github.com/microsoft/mssql-python (Arrow fetch: wiki "Cursor")
* arrow-odbc: https://github.com/pacman82/arrow-odbc-py

## Prior art

* Pinterest, Partition Finalization: https://medium.com/pinterest-engineering/partition-finalization-in-pinterests-next-generation-db-ingestion-framework-4c7da6e4cc8f
* Lakeflow Connect SQL Server (managed alternative): https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-overview
