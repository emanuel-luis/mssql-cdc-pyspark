"""The library through Spark Connect, as on Databricks serverless or Databricks Connect: this
process holds a Connect client session and no JVM, and the ``connect_server`` fixture's local
Connect server (with Delta Connect) runs the queries. The fake backend stands in for SQL
Server.

Marked ``connect`` (and ``spark`` and ``delta``), so the default run deselects them. Run them
alone, after ``uv sync --group connect``: ``uv run pytest -m connect``. Alone, because a
Connect session turns the whole process to Connect (``delta.tables`` among others).
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

from mssql_cdc import (
    apply_changes,
    await_all,
    finalization,
    is_data_loss,
    reconcile,
    start_many,
    stream,
)
from mssql_cdc.fake import FakeCdcDatabase

CI = "dbo_orders"
COLUMNS = "order_id INT, status STRING"
T0 = datetime(2026, 10, 1, 9, 0)


def _orders(workdir, n=12):
    """A keyed fake with orders 0..n-1 in one commit, and the source options for it."""
    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"}, columns={CI: COLUMNS})
    db.commit(CI, [(2, {"order_id": i, "status": "new"}) for i in range(n)], at=T0)
    return db, {"backend": "fake", "fakePath": src, "captureInstance": CI, "numPartitions": "2"}


def _paths(workdir, *names):
    return (os.path.join(workdir, n) for n in names)


def _eventually(check, timeout=180):
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.5)


def test_lab_t5_runs_on_the_server_spark_remote_names(connect_server):
    # the check to run first on a new runtime (docs/DATABRICKS.md), as a Connect client: its
    # get_spark() under SPARK_REMOTE, in a process of its own (this one has a session)
    run = subprocess.run(
        [sys.executable, "-m", "lab.checks.t5_engine"],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),  # the repository
        env={**os.environ, "SPARK_REMOTE": connect_server},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,  # the assert shows its output
    )
    assert run.returncode == 0, run.stdout[-3000:] + run.stderr[-3000:]
    assert "[PASS] batches split on commit boundaries" in run.stdout


def test_available_now_into_bronze_with_facts_then_advance(connect_spark, workdir):
    from pyspark.sql.connect.session import SparkSession as ConnectSession

    spark = connect_spark
    assert isinstance(spark, ConnectSession)  # no JVM in this process
    db, options = _orders(workdir)
    db.commit(CI, [(1, {"order_id": 0, "status": "new"})], at=T0 + timedelta(hours=1))
    bronze, ckpt, facts, control = _paths(workdir, "bronze", "ckpt", "facts", "control")
    q = stream(spark, options).to_delta(
        bronze, "orders-v1", ckpt, facts, trigger={"availableNow": True}
    )
    q.awaitTermination()
    assert spark.read.format("delta").load(bronze).count() == 13
    rows = spark.read.format("delta").load(facts).where("event IS NULL").collect()
    assert sum(r["rows"] for r in rows) == 13
    # the reader's metrics reached the facts: executors wrote them, the sink folded them
    assert all(r["retention_watermark_ts"] is not None for r in rows if r["rows"])
    end = finalization.end_offset_from_progress(q.lastProgress)
    assert end is not None
    fu = finalization.advance(spark, control, "bronze_orders", end)
    assert fu == datetime(2026, 10, 1, 10, 0)  # last commit 10:00
    assert finalization.is_final(spark, control, "bronze_orders", fu)


def test_processing_time_stream_tracked_until_it_stops(connect_spark, workdir):
    spark = connect_spark
    db, options = _orders(workdir)
    bronze, ckpt, control = _paths(workdir, "bronze", "ckpt", "control")
    options["maxCommitsPerBatch"] = "1"
    q = stream(spark, options).to_delta(
        bronze, "tracked-v1", ckpt, trigger={"processingTime": "1 second"}
    )
    try:
        tracker = finalization.track(spark, q, control, "bronze_orders")

        def verdict():
            return finalization.finalized_until(spark, control, "bronze_orders")

        _eventually(lambda: verdict() == datetime(2026, 10, 1, 9, 0))
        db.commit(CI, [(2, {"order_id": 99, "status": "new"})], at=T0 + timedelta(hours=2))
        _eventually(lambda: verdict() == datetime(2026, 10, 1, 11, 0))
    finally:
        q.stop()
    assert tracker.join(timeout=60) and tracker.last_error is None
    assert spark.read.format("delta").load(bronze).count() == 13


def test_full_snapshot(connect_spark, workdir):
    spark = connect_spark
    _, options = _orders(workdir)
    (target,) = _paths(workdir, "snapshot")
    offset = stream(spark, options).snapshot(target)
    rows = spark.read.format("delta").load(target).collect()
    assert sorted(r["order_id"] for r in rows) == list(range(12))
    assert {(r["_operation"], r["_snapshot"]) for r in rows} == {(0, offset["lsn"])}


def test_chunked_bootstrap_backfill_apply_and_reconcile(connect_spark, workdir):
    spark = connect_spark
    db, options = _orders(workdir)
    bronze, silver, ckpt, facts, control = _paths(
        workdir, "bronze", "silver", "ckpt", "facts", "control"
    )
    cdc = stream(spark, options)

    def run():
        cdc.to_delta(
            bronze,
            "chunked-v1",
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            snapshot="chunked",
        ).awaitTermination()

    run()  # opens the snapshot at S, streams from it
    db.commit(CI, [(1, {"order_id": 3, "status": "new"})], at=T0 + timedelta(minutes=1))
    status = cdc.backfill(bronze, app_id="chunked-v1", facts_table=facts, chunk_rows=3)
    assert status["done"] and status["chunks_total"] == 4
    run()
    result = apply_changes(
        spark,
        bronze,
        silver,
        capture_instance=CI,
        keys=["order_id"],
        control_table=control,
        facts_table=facts,
    )
    assert result["rebuilt"]
    keys = sorted(r["order_id"] for r in spark.read.format("delta").load(silver).collect())
    assert keys == [i for i in range(12) if i != 3]
    checked = reconcile(
        spark,
        options,
        silver,
        bronze=bronze,
        control_table=control,
        facts_table=facts,
        bucket_rows=4,
        sample=1.0,
    )
    assert (checked["mismatch"], checked["failures"]) == (0, {})
    assert checked["match"] == checked["buckets"] > 0


def test_start_many_data_loss_then_a_resnapshot_recovers(connect_spark, workdir):
    # the error reaches this process as text; the pre-flight reads the checkpoint from here
    spark = connect_spark
    db, options = _orders(workdir)
    given = db.idle(at=T0 + timedelta(minutes=1))
    db.commit(CI, [(2, {"order_id": 20, "status": "new"})], at=T0 + timedelta(minutes=2))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=3)))  # purged before it was read
    templates = {
        "target": os.path.join(workdir, "bronze_{ci}"),
        "app_id": "{ci}-v1",
        "checkpoint": os.path.join(workdir, "ckpt", "{ci}"),
        "facts_table": os.path.join(workdir, "facts"),
        "trigger": {"availableNow": True},
    }
    tables = {CI: {"startingLsn": given}}
    failed = await_all(start_many(spark, options, tables, **templates))
    assert list(failed) == [CI] and is_data_loss(failed[CI])
    queries = start_many(spark, options, tables, on_data_loss="resnapshot", **templates)
    assert await_all(queries) == {}
    bronze = spark.read.format("delta").load(templates["target"].format(ci=CI))
    keys = sorted(r["order_id"] for r in bronze.where("_operation = 0").collect())
    assert keys == [*range(12), 20]


def test_where_caching_is_refused_the_sink_backfill_and_reconcile_still_count(
    connect_spark, workdir, refuse_caching, caplog
):
    # as on Databricks serverless: the cache API raises in this client (backfill, reconcile)
    # and in the server's foreachBatch worker, where the sink runs (ADR 0032)
    spark = connect_spark
    calls = refuse_caching()
    db, options = _orders(workdir)
    options["maxCommitsPerBatch"] = "1"
    bronze, silver, ckpt, facts, control = _paths(
        workdir, "bronze", "silver", "ckpt", "facts", "control"
    )
    cdc = stream(spark, options)

    def run():  # a new sink each time, as each scheduled availableNow job is
        cdc.to_delta(
            bronze,
            "refused-v1",
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            snapshot="chunked",
        ).awaitTermination()

    run()  # opens the snapshot at S, streams from it
    changed = [
        (3, {"order_id": 0, "status": "new"}),
        (4, {"order_id": 0, "status": "paid"}),
        (1, {"order_id": 1, "status": "new"}),
    ]
    db.commit(CI, changed, at=T0 + timedelta(minutes=1))
    db.idle(at=T0 + timedelta(minutes=2))
    db.idle(at=T0 + timedelta(minutes=3))
    db.commit(CI, [(2, {"order_id": 20, "status": "new"})], at=T0 + timedelta(minutes=4))
    run()  # a batch per commit: with rows, without (twice), with
    commits = os.path.join(ckpt, "commits")
    last = max(int(n) for n in os.listdir(commits) if n.isdigit())
    for name in os.listdir(commits):  # the query died before its checkpoint commit
        if name.strip(".").split(".")[0] == str(last):  # and its .crc
            os.remove(os.path.join(commits, name))
    run()  # replays the last batch: Delta skips both writes, its facts are counted again
    batches = (
        spark.read.format("delta")
        .load(facts)
        .where("event IS NULL")
        .orderBy("batch_id")
        .select("batch_id", "rows", "deletes", "inserts", "updates")
        .collect()
    )
    assert [tuple(r)[1:] for r in batches[-4:]] == [(3, 1, 0, 1), (0,) * 4, (0,) * 4, (1, 0, 1, 0)]
    # the sink ran uncached: no counts in its commits; one without rows after one with rows
    history = spark.sql(f"DESCRIBE HISTORY delta.`{bronze}`").where("operation = 'WRITE'")
    metas = [json.loads(r["userMetadata"]) for r in history.orderBy("version").collect()]
    first = batches[-4]["batch_id"]
    assert [(m["batch_id"], m["rows"]) for m in metas if "batch_id" in m][-3:] == [
        (first, None),
        (first + 1, None),
        (first + 3, None),
    ]
    assert cdc.backfill(bronze, app_id="refused-v1", facts_table=facts, chunk_rows=3)["done"]
    chunks = spark.read.format("delta").load(facts).where("event = 'snapshot_chunk'").collect()
    assert len(chunks) == 4 and sum(r["rows"] for r in chunks) == 11  # order 1 deleted
    apply_changes(
        spark,
        bronze,
        silver,
        capture_instance=CI,
        keys=["order_id"],
        control_table=control,
        facts_table=facts,
    )
    current = spark.read.format("delta").load(silver).collect()
    assert sorted((r["order_id"], r["status"]) for r in current) == [
        (0, "paid"),
        *((i, "new") for i in range(2, 12)),
        (20, "new"),
    ]
    with caplog.at_level("WARNING", logger="mssql_cdc.reconcile"):
        checked = reconcile(
            spark,
            options,
            silver,
            bronze=bronze,
            control_table=control,
            facts_table=facts,
            bucket_rows=4,
            sample=1.0,
        )
    assert (checked["mismatch"], checked["failures"], checked["hashed"]) == (0, {}, 0)
    assert checked["match"] == checked["buckets"] > 0 and "compared counts only" in caplog.text
    assert set(calls) == {"persist", "localCheckpoint"}  # backfill and reconcile, here
