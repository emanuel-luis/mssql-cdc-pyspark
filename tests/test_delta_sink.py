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
    assert spark.read.format("delta").load(facts).count() == 3

    end = finalization.end_offset_from_progress(q.lastProgress)
    fu = finalization.advance(spark, control, "bronze_orders", end)
    assert fu == datetime(2026, 9, 28, 15, 0)  # last commit 15:10 -> 15:00

    # an older end offset must never move the verdict backwards
    older = {"lsn": end["lsn"], "commit_ts": "2026-09-28T13:55:00.000"}
    assert finalization.advance(spark, control, "bronze_orders", older) == fu
    assert finalization.is_final(spark, control, "bronze_orders", datetime(2026, 9, 28, 15))
    assert not finalization.is_final(spark, control, "bronze_orders", datetime(2026, 9, 28, 16))


def test_replayed_batch_is_ignored(delta_spark, workdir):
    spark = delta_spark
    target = os.path.join(workdir, "bronze")
    df = spark.createDataFrame([(1, 2, "0x" + "0" * 20, None)],
                               "order_id int, _operation int, _start_lsn string, _commit_ts timestamp_ntz")
    write = delta_sink(target, "replay-test")
    write(df, 7)
    write(df, 7)  # same batch id replayed after a failure
    assert spark.read.format("delta").load(target).count() == 1
