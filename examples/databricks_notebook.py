# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server CDC -> Delta with a completeness signal (classic compute)
# MAGIC
# MAGIC * Runtime: validate with `lab/checks/t5_engine.py` first (needs the Spark 4.2 Python
# MAGIC   data source streaming API: admission control + `Trigger.AvailableNow`).
# MAGIC * Access mode: **dedicated** (standard mode is untested for Python streaming sources).
# MAGIC * Network: the SQL Server must be reachable from the cluster (a local Docker is not).
# MAGIC * The Databricks-specific bits are confined to this notebook: secrets, UC names, Volumes.

# COMMAND ----------

# MAGIC %pip install "mssql-cdc-pyspark[mssql] @ git+https://github.com/emanuel-luis/mssql-cdc-pyspark.git"

# COMMAND ----------

from mssql_cdc import finalization, register
from mssql_cdc.sink import delta_sink

register(spark)

SCHEMA = "lab.cdc"  # catalog.schema
conn = (
    f"Server={dbutils.secrets.get('cdc', 'mssql-host')},1433;Database=cdc_lab;"
    f"UID={dbutils.secrets.get('cdc', 'mssql-user')};PWD={dbutils.secrets.get('cdc', 'mssql-password')};"
    "Encrypt=yes;TrustServerCertificate=no"
)

query = (
    spark.readStream.format("mssql_cdc")
    .option("connectionString", conn)
    .option("captureInstance", "dbo_orders")
    .option("maxCommitsPerBatch", "500")
    .load()
    .writeStream.foreachBatch(delta_sink(f"{SCHEMA}.bronze_orders", "orders-bronze-v1",
                                         f"{SCHEMA}.ingestion_facts"))
    .option("checkpointLocation", "/Volumes/lab/cdc/checkpoints/orders_bronze")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

end = finalization.end_offset_from_progress(query.lastProgress)
fu = finalization.advance(spark, f"{SCHEMA}.table_finalization", "bronze_orders", end)
print("finalized_until:", fu)
# Expose it to downstream tasks (If/else condition task compares numbers):
dbutils.jobs.taskValues.set("finalized_until_epoch", int(fu.timestamp()) if fu else 0)
