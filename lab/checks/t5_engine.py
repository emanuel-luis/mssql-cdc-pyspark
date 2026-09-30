"""t5: can this Spark runtime run the source? (no SQL Server needed)

Prints versions and API support, then runs a small Trigger.AvailableNow stream
against the file-backed fake CDC with maxCommitsPerBatch, and checks that the
engine split it on commit boundaries and exposed commit_ts in the end offset.

Locally:     python -m lab.checks.t5_engine
Databricks:  main() in a notebook on a single-node cluster; leave --path at its
             default (see docs/DATABRICKS.md, item 1).
"""

import argparse
import inspect
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta

from mssql_cdc import HAS_ADMISSION_CONTROL, register
from mssql_cdc.fake import FakeCdcDatabase
from mssql_cdc.finalization import end_offset_from_progress
from mssql_cdc.spark import get_spark

from ..common import report


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--path", default=tempfile.mkdtemp(prefix="t5-"))
    a = p.parse_args(argv)
    spark = get_spark("t5-engine", delta=False)
    register(spark)
    import pyspark
    from pyspark.sql.datasource import DataSourceStreamReader

    db = FakeCdcDatabase(os.path.join(a.path, "src"), ["dbo_orders"])
    t0 = datetime(2026, 1, 1, 10, 0)
    for i in range(10):
        db.commit("dbo_orders", [(2, {"order_id": i})], at=t0 + timedelta(minutes=i))
    q = (spark.readStream.format("mssql_cdc")
         .option("backend", "fake").option("fakePath", os.path.join(a.path, "src"))
         .option("captureInstance", "dbo_orders").option("columns", "order_id INT")
         .option("maxCommitsPerBatch", "3").load()
         .writeStream.format("memory").queryName("t5_out")
         .option("checkpointLocation", os.path.join(a.path, "ckpt"))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    progress = [json.loads(x.json) if hasattr(x, "json") else x for x in q.recentProgress]
    sizes = [x["numInputRows"] for x in progress if x["numInputRows"]]
    end = end_offset_from_progress(q.lastProgress)
    total = spark.sql("SELECT count(*) FROM t5_out").first()[0]
    checks = [
        ("spark / pyspark", None, f"{spark.version} / {pyspark.__version__}"),
        ("runtime", None, os.environ.get("DATABRICKS_RUNTIME_VERSION", "local")),
        ("latestOffset signature", None, str(inspect.signature(DataSourceStreamReader.latestOffset))),
        ("admission control + AvailableNow API", HAS_ADMISSION_CONTROL, str(HAS_ADMISSION_CONTROL)),
        ("batches split on commit boundaries", sizes == [3, 3, 3, 1], f"{sizes} (expected [3, 3, 3, 1])"),
        ("rows read", total == 10, str(total)),
        ("commit_ts in end offset", bool(end and end.get("commit_ts")), json.dumps(end)),
    ]
    return report("t5_engine", checks)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
