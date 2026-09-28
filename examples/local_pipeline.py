"""Local end-to-end pipeline: SQL Server (Docker) -> bronze Delta -> finalization.

    docker compose up -d
    python -m lab.workload setup && python -m lab.workload seed
    python examples/local_pipeline.py

Re-run it any time: it resumes from the checkpoint and only reads new commits.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mssql_cdc import finalization, register
from mssql_cdc.sink import delta_sink
from mssql_cdc.spark import get_spark

from lab.common import SOURCE_TZ, connection_string

import tempfile

# Delta tables and checkpoints live under MSSQL_CDC_WORK (default: system temp dir).
WORK = os.environ.get("MSSQL_CDC_WORK", os.path.join(tempfile.gettempdir(), "mssql-cdc-work"))
BRONZE = f"{WORK}/bronze_orders"
FACTS = f"{WORK}/ingestion_facts"
CONTROL = f"{WORK}/table_finalization"

spark = get_spark("mssql-cdc-local")
register(spark)

query = (
    spark.readStream.format("mssql_cdc")
    .option("connectionString", connection_string())
    .option("captureInstance", "dbo_orders")
    .option("sourceTimeZone", SOURCE_TZ)
    .option("maxCommitsPerBatch", "500")
    .load()
    .writeStream.foreachBatch(delta_sink(BRONZE, app_id="orders-bronze-v1", facts_table=FACTS))
    .option("checkpointLocation", f"{WORK}/_checkpoints/orders_bronze")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# Data is committed; now (and only now) advance the verdict.
end = finalization.end_offset_from_progress(query.lastProgress)
print("end offset:", end)
print("finalized_until:", finalization.advance(spark, CONTROL, "bronze_orders", end))
spark.read.format("delta").load(BRONZE).groupBy("_operation").count().show()
