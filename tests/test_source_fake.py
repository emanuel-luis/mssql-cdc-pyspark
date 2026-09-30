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
    assert [(r.lo, r.hi) for r in ranges] == [
        (None, (2,)),
        ((2,), (5,)),
        ((5,), (7,)),
        ((7,), None),
    ]
    assert {r.types for r in ranges} == {None}  # keys 0..9: MIN..MAX, integer bounds

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


def test_fake_resolves_the_exact_capture_instance_first(workdir):
    from mssql_cdc.fake import FakeCdcClient

    FakeCdcDatabase(os.path.join(workdir, "src"), ["dbo_Orders", "dbo_orders"])
    client = FakeCdcClient(os.path.join(workdir, "src"))
    # as SqlCdcClient.source_table: a case-sensitive database can hold both
    assert client.source_table("dbo_orders").table == "dbo_orders"
    assert client.source_table("dbo_Orders").table == "dbo_Orders"


def test_a_range_without_rows_leaves_its_metrics_file_with_its_end_commit_time(workdir):
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    db = _db(workdir, n_tx=1)
    metrics = os.path.join(workdir, "metrics")
    reader = _reader(workdir, metricsPath=metrics)
    start = reader.initialOffset()
    busy = reader.latestOffset(start, ReadAllAvailable())
    db.idle(at=T0 + timedelta(hours=1))
    idle = reader.latestOffset(busy, ReadAllAvailable())
    # the sink writes a facts row for a batch without rows too, from its files (ADR 0014)
    _order_ids(reader, reader.partitions(start, busy) + reader.partitions(busy, idle))
    files = {}
    for name in os.listdir(metrics):
        with open(os.path.join(metrics, name), encoding="utf-8") as fh:
            m = json.load(fh)
        files[m["to_lsn"]] = (m["rows"], m["to_commit_ts"])
    assert files == {busy["lsn"]: (3, busy["commit_ts"]), idle["lsn"]: (0, idle["commit_ts"])}


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


def _snapshot_partitions(src, schema, n):
    """The snapshot reader's partitions, each with the key tuples it reads."""
    from mssql_cdc.source import MssqlCdcSnapshotReader

    reader = MssqlCdcSnapshotReader(
        {"backend": "fake", "fakePath": src, "captureInstance": CI, "numPartitions": str(n)},
        schema,
    )
    keys = [f for f in schema.fieldNames() if f != "status"]
    return [
        (p, [tuple(r[k] for k in keys) for b in reader.read(p) for r in b.to_pylist()])
        for p in reader.partitions()
    ]


def test_snapshot_of_a_string_key_is_tiled(workdir):
    from pyspark.sql.types import StringType, StructField, StructType

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "code"})
    for i in range(5):
        db.commit(CI, [(2, {"code": f"C{i}", "status": "new"})], at=T0 + timedelta(minutes=i))
    schema = StructType([StructField("code", StringType()), StructField("status", StringType())])
    parts = _snapshot_partitions(src, schema, 4)
    # NTILE(4) of 5 rows: 2, 1, 1, 1; each range starts at its tile's first key
    assert [(p.lo, p.hi) for p, _ in parts] == [
        (None, ("C2",)),
        (("C2",), ("C3",)),
        (("C3",), ("C4",)),
        (("C4",), None),
    ]
    assert [rows for _, rows in parts] == [[("C0",), ("C1",)], [("C2",)], [("C3",)], [("C4",)]]
    assert {tuple(p.types or ()) for p, _ in parts} == {("sql_variant",)}  # typed bounds


def test_snapshot_of_a_composite_key_reads_every_row_once(spark, workdir):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: ["region", "id"]})
    # NULL keys (the fake allows them, SQL Server's CDC index does not): first, in any column
    keys = [(None, None), (None, "x"), (None, "y"), (1, None), (1, "a"), (1, "b"), (2, None)]
    keys.append((2, "b"))
    for a, b in keys:
        db.commit(CI, [(2, {"region": a, "id": b, "status": "new"})])
    schema = StructType(
        [
            StructField("region", IntegerType()),
            StructField("id", StringType()),
            StructField("status", StringType()),
        ]
    )
    parts = _snapshot_partitions(src, schema, 5)
    # NTILE(5) of 8 rows: 2, 2, 2, 1, 1; bounds with a NULL leading or trailing column
    assert [p.lo for p, _ in parts] == [None, (None, "y"), (1, "a"), (2, None), (2, "b")]
    assert [len(rows) for _, rows in parts] == [2, 2, 2, 1, 1]
    assert sorted((r for _, rows in parts for r in rows), key=str) == sorted(keys, key=str)
    # more partitions than rows: one row each
    many = _snapshot_partitions(src, schema, 20)
    assert [len(rows) for _, rows in many] == [1] * 8
    # through Spark: the tuple bounds pickle to the executors
    opts = {
        "backend": "fake",
        "fakePath": src,
        "captureInstance": CI,
        "columns": "region INT, id STRING, status STRING",
        "numPartitions": "5",
    }
    df = spark.read.format("mssql_cdc_snapshot").options(**opts).load()
    assert df.rdd.getNumPartitions() == 5
    assert sorted(((r["region"], r["id"]) for r in df.collect()), key=str) == sorted(keys, key=str)


def test_include_command_id_false_drops_the_column(spark, workdir):
    _db(workdir, n_tx=2)
    _, out = _run(spark, workdir, includeCommandId="false")
    df = _read(spark, out)
    assert "_command_id" not in df.columns and df.count() == 6
