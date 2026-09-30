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
from typing import ClassVar

import pytest

from mssql_cdc import HAS_ADMISSION_CONTROL, DataLossError, MssqlCdcDataSource
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
        db.commit(
            CI,
            [(2, _order(t * 100 + r)) for r in range(rows_per_tx)],
            at=start + timedelta(minutes=t),
        )
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
        spark.readStream.format("mssql_cdc")
        .options(**opts)
        .load()
        .writeStream.format("parquet")
        .option("path", out)
        .option("checkpointLocation", os.path.join(path, "ckpt"))
        .queryName(name)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    progress = [json.loads(p.json) if hasattr(p, "json") else p for p in q.recentProgress]
    batches = [p for p in progress if p.get("sources") and p["sources"][0].get("endOffset")]
    return batches, out


def _read(spark, out):
    return spark.read.parquet(out) if os.path.exists(out) else None


def _reader(workdir, **options):
    from pyspark.sql.types import IntegerType, StructField, StructType

    from mssql_cdc.source import MssqlCdcStreamReader

    opts = {
        "backend": "fake",
        "fakePath": os.path.join(workdir, "src"),
        "captureInstance": CI,
        "numPartitions": "1",
        **{k: str(v) for k, v in options.items()},
    }
    return MssqlCdcStreamReader(opts, StructType([StructField("order_id", IntegerType())]))


def _order_ids(reader, ranges):
    return [x for r in ranges for b in reader.read(r) for x in b.column("order_id").to_pylist()]


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


def test_cleanup_between_planning_and_read_fails_the_task(spark, workdir):
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    from mssql_cdc.source import MssqlCdcStreamReader

    db = _db(workdir, n_tx=3)
    opts = {
        "backend": "fake",
        "fakePath": os.path.join(workdir, "src"),
        "captureInstance": CI,
        "columns": COLUMNS,
        "numPartitions": "1",
    }
    schema = spark.createDataFrame([], MssqlCdcDataSource(opts).schema()).schema
    reader = MssqlCdcStreamReader(opts, schema)
    start = reader.initialOffset()
    [planned] = reader.partitions(start, reader.latestOffset(start, ReadAllAvailable()))
    # cleanup runs after planning: it purges the planned range before the task reads it
    db.cleanup(CI, db.commit(CI, [(2, _order(900))], at=T0 + timedelta(hours=1)))
    with pytest.raises(DataLossError, match="re-snapshot is required"):
        list(reader.read(planned))


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
    assert (
        _read(spark, out).selectExpr("CAST(paid_at AS STRING)").first()[0] == "2026-09-28 16:50:00"
    )


def test_register_carries_the_session_cores_to_the_workers():
    from pyspark import cloudpickle

    import mssql_cdc

    class StubSpark:
        class sparkContext:
            defaultParallelism = 7

        class dataSource:
            registered: ClassVar[list] = []

            @classmethod
            def register(cls, source):
                cls.registered.append(source)

    mssql_cdc.register(StubSpark)
    sources = StubSpark.dataSource.registered
    assert [s.name() for s in sources] == ["mssql_cdc", "mssql_cdc_snapshot"]
    for source in sources:
        assert issubclass(source, MssqlCdcDataSource)
        # the data source plans in a Python worker: the value must survive the pickling
        assert cloudpickle.loads(cloudpickle.dumps(source)).default_num_partitions == 7


def test_register_on_spark_connect_leaves_the_cores_to_the_planning_node():
    import mssql_cdc
    from mssql_cdc.spark import available_cores

    class StubConnect:  # Spark Connect: no sparkContext
        @property
        def sparkContext(self):
            raise RuntimeError("sparkContext is not supported in Spark Connect")

        class dataSource:
            registered: ClassVar[list] = []

            @classmethod
            def register(cls, source):
                cls.registered.append(source)

    spark = StubConnect()
    assert available_cores(spark) == 0
    mssql_cdc.register(spark)
    # not the CPU count of this process (a laptop on Databricks Connect)
    assert [s.default_num_partitions for s in spark.dataSource.registered] == [None, None]


def test_num_partitions_precedence():
    from pyspark.sql.types import IntegerType, StructField, StructType

    from mssql_cdc.source import MssqlCdcStreamReader

    schema = StructType([StructField("order_id", IntegerType())])
    opts = {"backend": "fake", "fakePath": "unused", "captureInstance": CI}
    assert MssqlCdcStreamReader({**opts, "numPartitions": "5"}, schema, 3).num_partitions == 5
    assert MssqlCdcStreamReader({**opts, "numPartitions": "auto"}, schema, 3).num_partitions == 3
    assert MssqlCdcStreamReader(opts, schema, 3).num_partitions == 3
    assert MssqlCdcStreamReader(opts, schema, None).num_partitions == max(1, os.cpu_count() or 1)


def test_num_partitions_defaults_to_the_session_cores(spark, workdir):
    from pyspark.sql import functions as F

    _db(workdir, n_tx=10, rows_per_tx=1)
    name = "q_" + uuid.uuid4().hex[:8]
    q = (
        spark.readStream.format("mssql_cdc")
        .options(
            backend="fake",
            fakePath=os.path.join(workdir, "src"),
            captureInstance=CI,
            columns=COLUMNS,
        )
        .load()
        .withColumn("pid", F.spark_partition_id())  # one Spark partition per planned LSN range
        .writeStream.format("memory")
        .queryName(name)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    pids = {r[0] for r in spark.sql(f"SELECT DISTINCT pid FROM {name}").collect()}
    assert len(pids) == spark.sparkContext.defaultParallelism  # conftest: local[2], via register()


def test_partitions_hold_the_same_rows_even_when_commits_differ_in_size(workdir):
    from pyspark.sql.streaming.datasource import ReadAllAvailable
    from pyspark.sql.types import IntegerType, StructField, StructType

    from mssql_cdc.source import MssqlCdcStreamReader

    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    for i in range(8):  # eight one-row commits, then one commit with eight rows
        db.commit(CI, [(2, {"order_id": i})], at=T0 + timedelta(minutes=i))
    db.commit(CI, [(2, {"order_id": 100 + r}) for r in range(8)], at=T0 + timedelta(minutes=9))
    opts = {
        "backend": "fake",
        "fakePath": os.path.join(workdir, "src"),
        "captureInstance": CI,
        "numPartitions": "2",
    }
    reader = MssqlCdcStreamReader(opts, StructType([StructField("order_id", IntegerType())]))
    start = reader.initialOffset()
    ranges = reader.partitions(start, reader.latestOffset(start, ReadAllAvailable()))
    client = reader.client
    sizes = [
        sum(b.num_rows for b in client.iter_changes(CI, r.from_lsn, r.to_lsn, [], False, 100))
        for r in ranges
    ]
    assert sizes == [8, 8]  # by commits it would be 5 commits / 5 rows and 4 commits / 11 rows


def test_snapshot_reads_the_current_rows_in_key_ranges(spark, workdir):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from mssql_cdc.fake import FakeCdcClient
    from mssql_cdc.source import MssqlCdcSnapshotReader

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    for i in range(10):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    db.commit(CI, [(3, {"order_id": 3, "status": "new"}), (4, {"order_id": 3, "status": "paid"})])
    db.commit(CI, [(1, {"order_id": 5, "status": "new"})])
    db.commit(CI, [(2, {"order_id": None, "status": "no key"})])  # a unique index allows one NULL
    at = db.idle(at=T0 + timedelta(hours=1))
    opts = {
        "backend": "fake",
        "fakePath": src,
        "captureInstance": CI,
        "columns": "order_id INT, status STRING",
        "numPartitions": "4",
    }

    schema = StructType(
        [StructField("order_id", IntegerType()), StructField("status", StringType())]
    )
    ranges = MssqlCdcSnapshotReader(opts, schema).partitions()
    assert [(r.lo, r.hi) for r in ranges] == [(None, 2), (2, 5), (5, 7), (7, None)]  # keys 0..9

    rows = spark.read.format("mssql_cdc_snapshot").options(**opts).load().collect()
    assert sorted(((r["order_id"], r["status"]) for r in rows), key=str) == sorted(
        [(i, "paid" if i == 3 else "new") for i in range(10) if i != 5] + [(None, "no key")],
        key=str,
    )
    assert {(r["_operation"], r["_start_lsn"], r["_seqval"], r["_command_id"]) for r in rows} == {
        (0, at, None, None)
    }
    assert {r["_commit_ts"] for r in rows} == {
        datetime.fromisoformat(FakeCdcClient(src).lsn_to_time(at))
    }


def test_available_now_stops_at_the_max_lsn_it_started_with(workdir):
    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxRows

    db = _db(workdir, n_tx=2)
    reader = _reader(workdir)
    start = reader.initialOffset()
    reader.prepareForTriggerAvailableNow()
    seen = reader.client.max_lsn()
    db.commit(CI, [(2, _order(900))], at=T0 + timedelta(hours=1))  # after the trigger started
    assert reader.latestOffset(start, ReadAllAvailable())["lsn"] == seen
    s = start
    while (nxt := reader.latestOffset(s, ReadMaxRows(1))) != s:
        s = nxt
    assert s["lsn"] == seen


def test_driver_guard_and_empty_ranges(workdir):
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    db = _db(workdir, n_tx=1)
    reader = _reader(workdir)
    start = reader.initialOffset()
    end = reader.latestOffset(start, ReadAllAvailable())
    db.cleanup(CI, db.commit(CI, [(2, _order(900))], at=T0 + timedelta(hours=1)))
    # after cleanup passed `end`, only the `end <= start` early return keeps these empty
    assert reader.partitions(end, end) == [] and reader.partitions(end, start) == []
    with pytest.raises(DataLossError, match="re-snapshot is required"):
        reader.partitions(start, reader.latestOffset(start, ReadAllAvailable()))


def test_fail_on_data_loss_false_skips_to_min_lsn_without_inverted_ranges(workdir):
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    lsns = [db.commit(CI, [(2, _order(i))], at=T0 + timedelta(minutes=i)) for i in range(10)]
    db.cleanup(CI, lsns[6])
    reader = _reader(workdir, failOnDataLoss="false", maxCommitsPerBatch=2)
    start, planned = {"lsn": lsns[0], "commit_ts": ""}, []
    while (end := reader.latestOffset(start, reader.getDefaultReadLimit())) != start:
        planned += reader.partitions(start, end)
        start = end
    assert all(r.from_lsn <= r.to_lsn for r in planned)  # invariant 3
    assert (planned[0].from_lsn, planned[0].to_lsn) == (lsns[6], lsns[6])
    assert _order_ids(reader, planned) == [6, 7, 8, 9]


def test_a_range_without_rows_leaves_no_metrics_file(workdir):
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    db = _db(workdir, n_tx=1)
    metrics = os.path.join(workdir, "metrics")
    reader = _reader(workdir, metricsPath=metrics)
    start = reader.initialOffset()
    busy = reader.latestOffset(start, ReadAllAvailable())
    db.idle(at=T0 + timedelta(hours=1))
    idle = reader.latestOffset(busy, ReadAllAvailable())
    # the sink skips a batch without rows, so it would never fold (and remove) its file
    _order_ids(reader, reader.partitions(start, busy) + reader.partitions(busy, idle))
    assert len([f for f in os.listdir(metrics) if f.endswith(".json")]) == 1


def test_snapshot_is_stamped_with_the_lsn_recorded_before_the_read(workdir):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from mssql_cdc.source import MssqlCdcSnapshotReader

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    db.commit(CI, [(2, {"order_id": 1})], at=T0)
    schema = StructType(
        [StructField("_start_lsn", StringType()), StructField("order_id", IntegerType())]
    )
    reader = MssqlCdcSnapshotReader(
        {"backend": "fake", "fakePath": src, "captureInstance": CI, "numPartitions": "1"}, schema
    )
    [part] = reader.partitions()
    newer = db.commit(CI, [(2, {"order_id": 2})], at=T0 + timedelta(minutes=1))  # during the read
    rows = [r for b in reader.read(part) for r in b.to_pylist()]
    assert {r["order_id"] for r in rows} == {1, 2}
    assert {r["_start_lsn"] for r in rows} == {part.lsn} and part.lsn < newer


def test_starting_lsn_is_exclusive(workdir):
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    lsns = [db.commit(CI, [(2, _order(i))], at=T0 + timedelta(minutes=i)) for i in range(5)]
    reader = _reader(workdir, startingLsn=lsns[1].lower())
    start = reader.initialOffset()
    assert start["lsn"] == lsns[1]
    ranges = reader.partitions(start, reader.latestOffset(start, ReadAllAvailable()))
    assert _order_ids(reader, ranges) == [2, 3, 4]


def test_snapshot_of_a_non_integer_key_is_one_partition(workdir):
    from pyspark.sql.types import StringType, StructField, StructType

    from mssql_cdc.source import MssqlCdcSnapshotReader

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "code"})
    for i in range(5):
        db.commit(CI, [(2, {"code": f"C{i}", "status": "new"})], at=T0 + timedelta(minutes=i))
    schema = StructType([StructField("code", StringType()), StructField("status", StringType())])
    reader = MssqlCdcSnapshotReader(
        {"backend": "fake", "fakePath": src, "captureInstance": CI, "numPartitions": "4"}, schema
    )
    [part] = reader.partitions()
    assert (part.key, part.lo, part.hi) == (None, None, None)
    assert sum(b.num_rows for b in reader.read(part)) == 5


def test_include_command_id_false_drops_the_column(spark, workdir):
    _db(workdir, n_tx=2)
    _, out = _run(spark, workdir, includeCommandId="false")
    df = _read(spark, out)
    assert "_command_id" not in df.columns and df.count() == 6
