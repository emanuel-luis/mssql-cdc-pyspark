"""Delta sink + finalization. Skipped when Delta jars are unavailable."""

import json
import os
from datetime import datetime, timedelta

import pytest

from mssql_cdc import finalization
from mssql_cdc.fake import FakeCdcDatabase
from mssql_cdc.sink import delta_sink

pytestmark = pytest.mark.delta
CI = "dbo_orders"
COLUMNS = "order_id INT, status STRING"
T0 = datetime(2026, 9, 28, 13, 50)


def _stream(spark, path, target, app_id, facts=None):
    q = (
        spark.readStream.format("mssql_cdc")
        .option("backend", "fake").option("fakePath", os.path.join(path, "src"))
        .option("captureInstance", CI).option("columns", COLUMNS)
        .option("maxCommitsPerBatch", "2").load()
        .writeStream.foreachBatch(delta_sink(target, app_id, facts))
        .option("checkpointLocation", os.path.join(path, "ckpt"))
        .trigger(availableNow=True).start()
    )
    q.awaitTermination()
    return q


def test_sink_facts_and_monotonic_finalization(delta_spark, workdir):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    for i in range(5):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=20 * i))
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    control = os.path.join(workdir, "control")

    q = _stream(spark, workdir, target, "orders-v1", facts)
    assert spark.read.format("delta").load(target).count() == 5
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{target}`").collect()
    metas = [json.loads(h["userMetadata"]) for h in hist if h["userMetadata"]]
    assert sum(m["rows"] for m in metas) == 5 and len(metas) == 3  # 2 + 2 + 1 commits
    fact_rows = spark.read.format("delta").load(facts).collect()
    assert len(fact_rows) == 3
    assert all(r["started_at"] <= r["written_at"] and r["duration_ms"] >= 0 for r in fact_rows)

    end = finalization.end_offset_from_progress(q.lastProgress)
    fu = finalization.advance(spark, control, "bronze_orders", end)
    assert fu == datetime(2026, 9, 28, 15, 0)  # last commit 15:10 -> 15:00

    # an older end offset must never move the verdict backwards
    older = {"lsn": end["lsn"], "commit_ts": "2026-09-28T13:55:00.000"}
    assert finalization.advance(spark, control, "bronze_orders", older) == fu
    assert finalization.is_final(spark, control, "bronze_orders", datetime(2026, 9, 28, 15))
    assert not finalization.is_final(spark, control, "bronze_orders", datetime(2026, 9, 28, 16))


def _comments(spark, path):
    """Column comments and the table description of a Delta table at ``path``."""
    fields = {f.name: (f.dataType.simpleString(), f.metadata.get("comment"))
              for f in spark.read.format("delta").load(path).schema}
    return fields, spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["description"]


def test_tables_are_created_typed_and_commented(delta_spark, workdir):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    db.commit(CI, [(2, {"order_id": 1, "status": "new"})], at=T0)
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    control = os.path.join(workdir, "control")
    q = _stream(spark, workdir, target, "typed-v1", facts)
    finalization.advance(spark, control, "bronze_orders", finalization.end_offset_from_progress(q.lastProgress))

    cols, description = _comments(spark, control)
    assert description and "finalized_until" in description
    assert cols["finalized_until"][0] == "timestamp_ntz" and "only moves forward" in cols["finalized_until"][1]
    assert cols["updated_at"][0] == cols["end_commit_ts"][0] == "timestamp_ntz"
    assert all(comment for _, comment in cols.values())
    cols, description = _comments(spark, facts)
    assert description and cols["min_commit_ts"][0] == "timestamp_ntz"
    # every time is TIMESTAMP_NTZ in UTC, so differences never depend on the session time zone
    assert {cols[c][0] for c in ("started_at", "written_at", "max_commit_ts")} == {"timestamp_ntz"}
    assert all(comment for _, comment in cols.values())
    cols, description = _comments(spark, target)
    assert description and cols["_start_lsn"][1] and cols["_operation"][1]
    assert cols["order_id"][1] is None  # captured columns keep the source's names and types only


def test_replayed_batch_is_ignored(delta_spark, workdir):
    spark = delta_spark
    target = os.path.join(workdir, "bronze")
    df = spark.createDataFrame([(1, 2, "0x" + "0" * 20, None)],
                               "order_id int, _operation int, _start_lsn string, _commit_ts timestamp_ntz")
    write = delta_sink(target, "replay-test")
    write(df, 7)
    write(df, 7)  # same batch id replayed after a failure
    assert spark.read.format("delta").load(target).count() == 1
