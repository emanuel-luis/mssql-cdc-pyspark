"""End-to-end tests of the DataSource V2 reader against the file-backed fake CDC.

They run the real Spark streaming engine (local mode, separate Python workers),
so offsets, checkpoints, Trigger.AvailableNow and admission control are exercised
for real; only SQL Server is simulated.
"""

import json
import os
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from mssql_cdc import HAS_ADMISSION_CONTROL
from mssql_cdc.fake import FakeCdcDatabase
from mssql_cdc.finalization import candidate, end_offset_from_progress

pytestmark = pytest.mark.skipif(not HAS_ADMISSION_CONTROL, reason="needs Spark 4.2+")

CI = "dbo_orders"
COLUMNS = "order_id INT, status STRING, amount DECIMAL(18,2), updated_at TIMESTAMP_NTZ"
T0 = datetime(2026, 9, 28, 13, 50, 0)


def _order(i, status="new", amount="10.00", at=T0):
    return {"order_id": i, "status": status, "amount": amount, "updated_at": at.isoformat()}


def _db(path, n_tx=0, rows_per_tx=3, start=T0):
    db = FakeCdcDatabase(os.path.join(path, "src"), [CI])
    for t in range(n_tx):
        db.commit(CI, [(2, _order(t * 100 + r)) for r in range(rows_per_tx)], at=start + timedelta(minutes=t))
    return db


def _run(spark, path, **options):
    opts = {
        "backend": "fake",
        "fakePath": os.path.join(path, "src"),
        "captureInstance": CI,
        "columns": COLUMNS,
    }
    opts.update({k: str(v) for k, v in options.items()})
    name = "q_" + uuid.uuid4().hex[:8]
    out = os.path.join(path, "out")
    q = (
        spark.readStream.format("mssql_cdc").options(**opts).load()
        .writeStream.format("parquet").option("path", out)
        .option("checkpointLocation", os.path.join(path, "ckpt"))
        .queryName(name).trigger(availableNow=True).start()
    )
    q.awaitTermination()
    progress = [json.loads(p.json) if hasattr(p, "json") else p for p in q.recentProgress]
    batches = [p for p in progress if p.get("sources") and p["sources"][0].get("endOffset")]
    return batches, out


def _read(spark, out):
    return spark.read.parquet(out) if os.path.exists(out) else None


def test_available_now_splits_on_commit_boundaries(spark, workdir):
    _db(workdir, n_tx=10, rows_per_tx=3)
    batches, out = _run(spark, workdir, maxCommitsPerBatch=4)
    sizes = [b["numInputRows"] for b in batches if b["numInputRows"]]
    assert sizes == [12, 12, 6]  # 4 + 4 + 2 commits, 3 rows each
    df = _read(spark, out)
    assert df.count() == 30
    assert df.select("order_id").distinct().count() == 30


def test_restart_reads_only_new_commits(spark, workdir):
    db = _db(workdir, n_tx=3)
    _run(spark, workdir)
    db.commit(CI, [(2, _order(900)), (2, _order(901))], at=T0 + timedelta(hours=1))
    batches, out = _run(spark, workdir)
    assert [b["numInputRows"] for b in batches if b["numInputRows"]] == [2]
    assert _read(spark, out).count() == 11


def test_idle_dummy_entries_advance_offset_and_finalization(spark, workdir):
    db = _db(workdir, n_tx=2)
    first, _ = _run(spark, workdir)
    before = candidate(end_offset_from_progress(first[-1]))
    assert before == datetime(2026, 9, 28, 13, 0)
    # No changes at all, only capture heartbeats two hours later
    db.idle(at=T0 + timedelta(hours=2, minutes=5))
    second, out = _run(spark, workdir)
    end = end_offset_from_progress(second[-1])
    assert second[-1]["numInputRows"] == 0
    assert candidate(end) == datetime(2026, 9, 28, 15, 0)  # advanced without data
    assert _read(spark, out).count() == 6


def test_retention_guard_fails_loudly(spark, workdir):
    db = _db(workdir, n_tx=2)
    _run(spark, workdir)
    db.commit(CI, [(2, _order(700))], at=T0 + timedelta(hours=1))
    last = db.commit(CI, [(2, _order(701))], at=T0 + timedelta(hours=2))
    db.cleanup(CI, last)  # cleanup purged the commit the stream still needs
    with pytest.raises(Exception, match="re-snapshot is required"):
        _run(spark, workdir)


def test_metadata_columns_and_types(spark, workdir):
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    db.commit(
        CI,
        [
            (2, _order(1, amount="10.50")),
            (3, _order(1, amount="10.50")),
            (4, _order(1, status="paid", amount="12.25")),
            (1, _order(1, status="paid", amount="12.25")),
        ],
        at=T0,
    )
    _, out = _run(spark, workdir)
    df = _read(spark, out)
    types = dict(df.dtypes)
    assert types["amount"] == "decimal(18,2)"
    assert types["updated_at"] == "timestamp_ntz"
    assert types["_commit_ts"] == "timestamp_ntz"
    rows = df.orderBy("_command_id").collect()
    assert [r["_operation"] for r in rows] == [2, 3, 4, 1]
    assert [r["_command_id"] for r in rows] == [1, 2, 3, 4]
    assert rows[2]["amount"] == Decimal("12.25")
    assert rows[0]["_capture_instance"] == CI
    assert rows[0]["_start_lsn"].startswith("0x") and len(rows[0]["_start_lsn"]) == 22


def test_num_partitions_splits_without_duplicates(spark, workdir):
    _db(workdir, n_tx=9, rows_per_tx=2)
    _run(spark, workdir, numPartitions=3)
    df = _read(spark, os.path.join(workdir, "out"))
    assert df.count() == 18
    assert df.select("order_id").distinct().count() == 18


def test_starting_latest_skips_history(spark, workdir):
    db = _db(workdir, n_tx=3)
    _run(spark, workdir, startingLsn="latest")
    db.commit(CI, [(2, _order(555))], at=T0 + timedelta(hours=1))
    _, out = _run(spark, workdir, startingLsn="latest")
    assert [r["order_id"] for r in _read(spark, out).collect()] == [555]


def test_timestamp_column_is_a_utc_instant(spark, workdir):
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    db.commit(CI, [(2, {"order_id": 1, "paid_at": "2026-09-28T13:50:00-03:00"})], at=T0)
    _, out = _run(spark, workdir, columns="order_id INT, paid_at TIMESTAMP")
    # rendered in the session time zone (UTC); collect() would use the local zone
    assert _read(spark, out).selectExpr("CAST(paid_at AS STRING)").first()[0] == "2026-09-28 16:50:00"
