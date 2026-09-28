"""t7: SQL Server CDC -> PySpark source -> Delta bronze -> finalization.

1. generate a mixed workload with Faker (inserts, updates, deletes);
2. run the stream with Trigger.AvailableNow and maxCommitsPerBatch;
3. verify bronze == change table rows up to the end LSN, with no duplicates;
4. advance finalized_until and check it never exceeds the end commit time;
5. rerun on the same checkpoint: nothing new must be read;
6. more workload, rerun: only the new changes are read;
7. --idle-minutes N: wait with no writes, rerun, and see finalization move
   without new rows (depends on t1);
8. --destructive: purge the change table behind the stream and expect the
   retention guard to stop it.

Per-batch timings are saved in lab/results for the write-up.

Locally:     python -m lab.checks.t7_end_to_end
Databricks:  main(["--schema", "lab.cdc", "--checkpoint", "/Volumes/lab/cdc/ckpt/t7"])
             (the SQL Server must be reachable from the cluster)
"""

import argparse
import json
import os
import sys
import tempfile
import time
import uuid

from mssql_cdc import finalization, register
from mssql_cdc.sink import delta_sink
from mssql_cdc.spark import get_spark

from ..common import SOURCE_TZ, connect, connection_string, ct_count, max_lsn, report
from ..workload import Workload

COLUMNS = ("order_id INT, customer_id INT, status STRING, amount DECIMAL(18,2), "
           "created_at TIMESTAMP_NTZ, updated_at TIMESTAMP_NTZ")


def _wait_stable(conn, ci, polls=3, every=2.0, timeout=180):
    """Wait until capture has caught up: the change-table count stops moving."""
    last, same, deadline = -1, 0, time.time() + timeout
    while time.time() < deadline:
        n = ct_count(conn, ci)
        same = same + 1 if n == last else 0
        if same >= polls:
            return n
        last = n
        time.sleep(every)
    return last


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--schema", help="catalog.schema for managed tables (default: local paths)")
    p.add_argument("--checkpoint", help="checkpoint location (default: temp dir)")
    p.add_argument("--transactions", type=int, default=300)
    p.add_argument("--max-commits", type=int, default=100)
    p.add_argument("--num-partitions", type=int, default=1)
    p.add_argument("--backend", default="mssql-python", choices=["mssql-python", "arrow-odbc"])
    p.add_argument("--idle-minutes", type=float, default=0)
    p.add_argument("--destructive", action="store_true")
    a = p.parse_args(argv)

    base = tempfile.mkdtemp(prefix="t7-")
    target = f"{a.schema}.bronze_orders" if a.schema else os.path.join(base, "bronze_orders")
    facts = f"{a.schema}.ingestion_facts" if a.schema else os.path.join(base, "ingestion_facts")
    control = f"{a.schema}.table_finalization" if a.schema else os.path.join(base, "table_finalization")
    ckpt = a.checkpoint or os.path.join(base, "ckpt")
    app_id = f"t7-{uuid.uuid4().hex[:8]}"  # new checkpoint -> new app id

    spark = get_spark("t7-e2e")
    register(spark)
    conn = connect()
    wl = Workload(seed=7)
    mix = {"insert": 0.5, "update": 0.35, "delete": 0.15}
    for _ in range(a.transactions):
        wl.transaction(mix, 4)
    _wait_stable(conn, "dbo_orders")

    conn_str = connection_string()
    if a.backend == "arrow-odbc":
        conn_str = "Driver={ODBC Driver 18 for SQL Server};" + conn_str

    def run():
        q = (spark.readStream.format("mssql_cdc")
             .option("backend", a.backend).option("connectionString", conn_str)
             .option("captureInstance", "dbo_orders").option("columns", COLUMNS)
             .option("sourceTimeZone", SOURCE_TZ)
             .option("maxCommitsPerBatch", str(a.max_commits))
             .option("numPartitions", str(a.num_partitions)).load()
             .writeStream.foreachBatch(delta_sink(target, app_id, facts))
             .option("checkpointLocation", ckpt).trigger(availableNow=True).start())
        q.awaitTermination()
        prog = [json.loads(x.json) if hasattr(x, "json") else x for x in q.recentProgress]
        return q, prog

    def bronze_count():
        return spark.sql(f"SELECT count(*) FROM {finalization.table_ref(target)}").first()[0]

    checks, timings = [], []
    t_start = time.time()
    q, prog = run()
    elapsed = time.time() - t_start
    end = finalization.end_offset_from_progress(q.lastProgress)
    expected = ct_count(conn, "dbo_orders", end["lsn"])
    got = bronze_count()
    dups = spark.sql(f"SELECT count(*) - count(DISTINCT _start_lsn, _seqval, _operation) "
                     f"FROM {finalization.table_ref(target)}").first()[0]
    timings = [{"batch": x["batchId"], "rows": x["numInputRows"],
                "addBatch_ms": x["durationMs"].get("addBatch"),
                "trigger_ms": x["durationMs"].get("triggerExecution")} for x in prog if x["numInputRows"]]
    checks.append(("bronze rows == change-table rows up to end LSN", got == expected, f"{got} vs {expected}"))
    checks.append(("no duplicate change rows", dups == 0, str(dups)))
    checks.append(("batches (maxCommitsPerBatch)", None, f"{len(timings)} batches, {elapsed:.1f}s wall"))

    fu = finalization.advance(spark, control, "bronze_orders", end)
    end_ts = end["commit_ts"]
    checks.append(("finalized_until <= end commit time", fu is not None and fu.isoformat() <= end_ts,
                   f"{fu} vs {end_ts}"))

    q2, prog2 = run()
    new = sum(x["numInputRows"] for x in prog2)
    checks.append(("rerun on same checkpoint reads nothing new", new == 0, f"{new} rows"))

    before = bronze_count()
    for _ in range(20):
        wl.transaction(mix, 4)
    _wait_stable(conn, "dbo_orders")
    q3, _ = run()
    end3 = finalization.end_offset_from_progress(q3.lastProgress)
    delta_expected = ct_count(conn, "dbo_orders", end3["lsn"]) - expected
    checks.append(("incremental run reads only new changes", bronze_count() - before == delta_expected,
                   f"{bronze_count() - before} vs {delta_expected}"))
    fu3 = finalization.advance(spark, control, "bronze_orders", end3)

    if a.idle_minutes:
        time.sleep(a.idle_minutes * 60)
        q4, prog4 = run()
        end4 = finalization.end_offset_from_progress(q4.lastProgress)
        rows4 = sum(x["numInputRows"] for x in prog4)
        fu4 = finalization.advance(spark, control, "bronze_orders", end4)
        checks.append(("idle run: end offset advanced without rows",
                       bool(end4) and end4["lsn"] > end3["lsn"] and rows4 == 0,
                       f"{end3['lsn']} -> {end4['lsn'] if end4 else None}, rows={rows4}"))
        checks.append(("idle run: finalized_until", None, f"{fu3} -> {fu4}"))

    if a.destructive:
        for _ in range(3):
            wl.transaction(mix, 2)
        _wait_stable(conn, "dbo_orders")
        conn.cursor().execute(
            "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); "
            "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_orders', "
            "@low_water_mark = @lw, @threshold = 5000;")
        try:
            run()
            checks.append(("retention guard stops the stream", False, "stream ran; data loss was silent"))
        except Exception as exc:  # noqa: BLE001
            checks.append(("retention guard stops the stream", "re-snapshot" in str(exc),
                           str(exc).splitlines()[0][:200]))

    wl.close()
    return report("t7_end_to_end", checks, {
        "backend": a.backend, "max_commits_per_batch": a.max_commits,
        "num_partitions": a.num_partitions, "timings": timings,
        "server_max_lsn": max_lsn(conn), "target": target,
    })


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
