# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server CDC -> Delta with a completeness signal
# MAGIC
# MAGIC * Runtime: validate with `lab/checks/t5_engine.py` first (needs the Spark 4.2 Python
# MAGIC   data source streaming API: admission control + `Trigger.AvailableNow`).
# MAGIC * Compute: classic in **dedicated** access mode (standard mode is untested for Python
# MAGIC   streaming sources), or serverless (Spark Connect: `availableNow` only, as below; not
# MAGIC   run there yet).
# MAGIC * Network: the SQL Server must be reachable from the compute (a local Docker is not).
# MAGIC * The Databricks-specific bits are confined to this notebook: secrets, UC names, Volumes.

# COMMAND ----------

# MAGIC %pip install mssql-cdc-pyspark==0.7.0

# COMMAND ----------

from datetime import timezone

from mssql_cdc import finalization, stream

SCHEMA = "lab.cdc"  # catalog.schema
host = dbutils.secrets.get("cdc", "mssql-host")
options = {
    # no UID or PWD here: Spark shows connectionString whole in the streaming query's plan
    "connectionString": f"Server={host},1433;Database=cdc_lab;Encrypt=yes",
    # passed to the driver apart, escaped by it; or, with an Entra ID login, tokenProvider
    "user": dbutils.secrets.get("cdc", "mssql-user"),
    "password": dbutils.secrets.get("cdc", "mssql-password"),
    "captureInstance": "dbo_orders",
    "maxCommitsPerBatch": "500",
}
# bootstrap: snapshot dbo.orders into bronze once, then stream the changes after it
query = stream(spark, options).to_delta(
    f"{SCHEMA}.bronze_orders",
    "orders-bronze-v1",
    checkpoint="/Volumes/lab/cdc/checkpoints/orders_bronze",
    facts_table=f"{SCHEMA}.ingestion_facts",
    trigger={"availableNow": True},
    bootstrap=True,
)
query.awaitTermination()

# COMMAND ----------

end = finalization.end_offset_from_progress(query.lastProgress)
# keyed by the stream's target, the name apply_changes and consumers look the verdict up by
fu = finalization.advance(spark, f"{SCHEMA}.table_finalization", f"{SCHEMA}.bronze_orders", end)
print("finalized_until:", fu)
# Expose it to downstream tasks (If/else condition task compares numbers):
# fu is a naive UTC datetime: .timestamp() alone would read it as the driver's local time
epoch = int(fu.replace(tzinfo=timezone.utc).timestamp()) if fu else 0
dbutils.jobs.taskValues.set("finalized_until_epoch", epoch)
