"""t6: Delta behaviours the sink and finalization rely on.

* userMetadata via the session conf on MERGE (SQL and Python API);
* idempotent append with txnAppId/txnVersion (a replayed batch is skipped);
* idempotent MERGE via spark.databricks.delta.write.txnAppId/txnVersion
  (Delta OSS 2.3+; unverified on Databricks).

Locally:     python -m lab.checks.t6_delta_semantics
Databricks:  main(["--schema", "lab.cdc"])   # uses managed tables
"""

import argparse
import json
import sys
import tempfile

from mssql_cdc.spark import get_spark

from ..common import report


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--schema", help="catalog.schema for managed tables (default: temp paths)")
    a = p.parse_args(argv)
    spark = get_spark("t6-delta")
    base = tempfile.mkdtemp(prefix="t6-")

    def ref(name):
        return f"{a.schema}.{name}" if a.schema else f"delta.`{base}/{name}`"

    def create(name, ddl):
        # Drop, not CREATE OR REPLACE: a replaced table keeps its Delta log, so a rerun would
        # see the last run's txnAppId/txnVersion (appends skipped) and history (false passes).
        if a.schema:
            spark.sql(f"DROP TABLE IF EXISTS {ref(name)}")
        spark.sql(f"CREATE TABLE {ref(name)} ({ddl}) USING delta")

    def history(name, n=5):
        return (
            spark.sql(f"DESCRIBE HISTORY {ref(name)} LIMIT {n}")
            .select("version", "operation", "userMetadata")
            .collect()
        )

    checks = []
    create("um", "id INT, v STRING")
    spark.sql(f"INSERT INTO {ref('um')} VALUES (1, 'a')")
    key = "spark.databricks.delta.commitInfo.userMetadata"
    spark.conf.set(key, json.dumps({"probe": "merge-sql"}))
    spark.sql(
        f"MERGE INTO {ref('um')} t USING (SELECT 1 AS id, 'b' AS v) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )
    spark.conf.unset(key)
    from delta.tables import DeltaTable

    dt = (
        DeltaTable.forName(spark, ref("um"))
        if a.schema
        else DeltaTable.forPath(spark, f"{base}/um")
    )
    spark.conf.set(key, json.dumps({"probe": "merge-python"}))
    (
        dt.alias("t")
        .merge(spark.createDataFrame([(1, "c")], "id int, v string").alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .execute()
    )
    spark.conf.unset(key)
    metas = [h["userMetadata"] or "" for h in history("um")]
    checks.append(("userMetadata on SQL MERGE", any("merge-sql" in m for m in metas), str(metas)))
    checks.append(
        ("userMetadata on Python MERGE", any("merge-python" in m for m in metas), str(metas))
    )

    create("txn", "id INT")
    df = spark.createDataFrame([(1,), (2,)], "id int")
    for _ in range(2):
        w = df.write.format("delta").mode("append").option("txnAppId", "t6").option("txnVersion", 1)
        w.saveAsTable(ref("txn")) if a.schema else w.save(f"{base}/txn")
    n = spark.sql(f"SELECT count(*) FROM {ref('txn')}").first()[0]
    checks.append(
        ("replayed append skipped (txnAppId/txnVersion)", n == 2, f"{n} rows (expected 2)")
    )

    create("txn_merge", "id INT")
    spark.conf.set("spark.databricks.delta.write.txnAppId", "t6-merge")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
    for _ in range(2):
        spark.sql(
            f"MERGE INTO {ref('txn_merge')} t USING (SELECT 1 AS id) s ON t.id = s.id "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    spark.conf.unset("spark.databricks.delta.write.txnAppId")
    spark.conf.unset("spark.databricks.delta.write.txnVersion")
    merges = [h for h in history("txn_merge", 10) if h["operation"] == "MERGE"]
    checks.append(
        (
            "replayed MERGE skipped (session txn confs)",
            len(merges) == 1,
            f"{len(merges)} MERGE commits (expected 1)",
        )
    )
    return report("t6_delta_semantics", checks)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
