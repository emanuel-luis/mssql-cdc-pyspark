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
from itertools import pairwise
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


def _opts(path, **options) -> dict:
    """The fake source's options; an option given as None is left out."""
    opts = {
        "backend": "fake",
        "fakePath": os.path.join(path, "src"),
        "captureInstance": CI,
        "columns": COLUMNS,
        **{k: str(v) for k, v in options.items()},
    }
    return {k: v for k, v in opts.items() if options.get(k, "") is not None}


def _run(spark, path, **options):
    opts = _opts(path, **options)
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
    rows = df.orderBy("_command_id", "_operation").collect()
    assert [r["_operation"] for r in rows] == [2, 3, 4, 1]
    # an update's 3 and 4 share __$command_id and __$seqval, as on SQL Server
    assert [r["_command_id"] for r in rows] == [1, 2, 2, 4]
    assert rows[1]["_seqval"] == rows[2]["_seqval"] != rows[0]["_seqval"]
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
    # another instance's low watermark keeps the cdc.lsn_time_mapping rows below CI's, so
    # the first batches end below CI's min_lsn
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI, "dbo_customers"])
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


# --------------------------------------------------------------------------- #
# Schema changes and a second capture instance (ADR 0023)
# --------------------------------------------------------------------------- #
V2_COLUMNS = "order_id INT, status STRING, amount DECIMAL(20,4), note STRING"


def _noted(i, **kw):
    return {**_order(i, **kw), "note": f"n{i}"}


def _switch(path, before=5, after=5, v2_columns=COLUMNS):
    """``before`` commits captured by CI only, then a newer instance of its table starting at
    the next commit, then ``after`` commits captured by both. Returns the database, the
    newer instance's name, its start S (the first commit after it) and the commit LSNs."""
    db = FakeCdcDatabase(os.path.join(path, "src"), [CI], columns={CI: COLUMNS})
    c = [db.commit(CI, [(2, _noted(i))], at=T0 + timedelta(minutes=i)) for i in range(before)]
    v2 = db.add_capture_instance(CI, v2_columns, at=T0 + timedelta(minutes=before))
    c += [
        db.commit(CI, [(2, _noted(i))], at=T0 + timedelta(minutes=i))
        for i in range(before, before + after)
    ]
    return db, v2, c[before], c


def _stream_reader(spark, path, **options):
    """A stream reader with the schema load() gives these options (inferred with
    ``columns=None``), as Spark builds it."""
    from mssql_cdc.source import MssqlCdcStreamReader

    opts = _opts(path, **{"numPartitions": 1, **options})
    schema = spark.createDataFrame([], MssqlCdcDataSource(opts).schema()).schema
    return MssqlCdcStreamReader(opts, schema)


def _plan(reader, start, end):
    return reader.partitions({"lsn": start, "commit_ts": ""}, {"lsn": end, "commit_ts": ""})


def _rows(reader, ranges):
    return [r for p in ranges for b in reader.read(p) for r in b.to_pylist()]


def _events(metrics) -> dict:
    """The event files the reader left in ``metrics``, by name."""
    out = {}
    for name in sorted(os.listdir(metrics)) if os.path.exists(metrics) else []:
        if name.startswith("event-"):
            with open(os.path.join(metrics, name), encoding="utf-8") as fh:
                out[name] = json.load(fh)
    return out


def _ms(minutes):
    return (T0 + timedelta(minutes=minutes)).isoformat(timespec="milliseconds")


def test_a_newer_capture_instance_takes_over_at_its_start_lsn(spark, workdir):
    _, v2, s, _ = _switch(workdir)
    metrics = os.path.join(workdir, "metrics")
    # 4 commits a batch: the second reads commits 4-7, across S (commit 5)
    batches, out = _run(
        spark, workdir, columns=None, numPartitions=3, maxCommitsPerBatch=4, metricsPath=metrics
    )
    assert [b["numInputRows"] for b in batches if b["numInputRows"]] == [4, 4, 2]
    rows = _read(spark, out).collect()
    assert len({(r["_start_lsn"], r["_seqval"], r["_operation"]) for r in rows}) == len(rows) == 10
    # the instance each row came from, with its own command ids (v2's differ, like SQL Server's)
    got = sorted((r["order_id"], r["_capture_instance"], r["_command_id"]) for r in rows)
    assert got == [(i, CI, 1) if i < 5 else (i, v2, 2) for i in range(10)]
    assert all((r["_start_lsn"] < s) == (r["_capture_instance"] == CI) for r in rows)
    # the batch that crossed S left the switch there, for the sink's facts
    assert _events(metrics) == {
        f"event-capture_instance_switched-{s}.json": {
            "event": "capture_instance_switched",
            "capture_instance": v2,
            "lsn": s,
            "commit_ts": _ms(5),
            "detail": f"{CI} -> {v2}",
        }
    }


def test_partitions_split_at_the_newer_start_without_empty_ranges(spark, workdir):
    from mssql_cdc.lsn import from_int, to_int

    _, v2, s, c = _switch(workdir)
    reader = _stream_reader(spark, workdir, numPartitions=2)

    def plan(start, end):
        ranges = _plan(reader, start, end)
        assert all(r.from_lsn <= r.to_lsn for r in ranges)  # invariant 3
        assert all(r.to_lsn < s if r.capture_instance == CI else r.from_lsn >= s for r in ranges)
        for a, b in pairwise(ranges):  # contiguous: every LSN in exactly one range
            assert to_int(b.from_lsn) == to_int(a.to_lsn) + 1
        return [(r.capture_instance, r.from_lsn, r.to_lsn) for r in ranges]

    before, below = from_int(to_int(c[0]) - 1), from_int(to_int(s) - 1)
    whole = plan(before, c[9])
    assert (whole[0][1], whole[-1][2]) == (c[0], c[9])
    assert [ci for ci, _, _ in whole].count(v2) == 2 and len(whole) >= 4  # both sides split
    ids = sorted(r["order_id"] for r in _rows(reader, _plan(reader, before, c[9])))
    assert ids == list(range(10))
    # S on an edge: a batch that starts at S, one that ends below it, one that ends at it
    assert plan(below, c[9])[0] == (v2, s, plan(below, c[9])[0][2])
    assert {ci for ci, _, _ in plan(below, c[9])} == {v2}
    assert {ci for ci, _, _ in plan(c[1], c[4])} == {CI}
    edge = plan(c[3], c[5])
    assert edge[-1] == (v2, s, s) and {ci for ci, _, _ in edge[:-1]} == {CI}


def test_replays_across_the_newer_start_plan_and_read_the_same(spark, workdir):
    _, _, _, c = _switch(workdir)
    first = _stream_reader(spark, workdir, numPartitions=3)
    planned = _plan(first, c[2], c[8])
    again = _stream_reader(spark, workdir, numPartitions=3)  # a restart replays the batch
    assert _plan(first, c[2], c[8]) == planned == _plan(again, c[2], c[8])
    assert _rows(first, planned) == _rows(again, _plan(again, c[2], c[8]))
    assert [r["order_id"] for r in _rows(first, planned)] == [3, 4, 5, 6, 7, 8]


def test_the_schema_is_the_union_and_a_missing_column_reads_null(spark, workdir):
    _, v2, _, _ = _switch(workdir, v2_columns=V2_COLUMNS)  # no updated_at, a note, wider amount
    assert (
        MssqlCdcDataSource(_opts(workdir, columns=None))
        .schema()
        .endswith(
            "`order_id` INT, `status` STRING, `amount` DECIMAL(20,4), "
            "`updated_at` TIMESTAMP_NTZ, `note` STRING"
        )
    )
    metrics = os.path.join(workdir, "metrics")
    _, out = _run(spark, workdir, columns=None, numPartitions=2, metricsPath=metrics)
    df = _read(spark, out)
    assert dict(df.dtypes)["amount"] == "decimal(20,4)"
    got = sorted((r["order_id"], r["updated_at"] is None, r["note"]) for r in df.collect())
    assert got == [(i, False, None) if i < 5 else (i, True, f"n{i}") for i in range(10)]
    # the switch's warning and event say which column reads NULL from there on
    assert [e["detail"] for e in _events(metrics).values()] == [
        f"{CI} -> {v2}; no longer captured, read as NULL: updated_at"
    ]


def test_a_narrower_type_in_the_newer_instance_fails_at_load(workdir):
    from mssql_cdc import SchemaChangedError

    _switch(workdir, v2_columns="order_id INT, amount DECIMAL(10,2)")
    with pytest.raises(
        SchemaChangedError, match=r"'amount' as DECIMAL\(18,2\) and DECIMAL\(10,2\)"
    ):
        MssqlCdcDataSource(_opts(workdir, columns=None)).schema()


def test_the_retention_guard_checks_each_instance(spark, workdir):
    db, v2, _, c = _switch(workdir)
    reader = _stream_reader(spark, workdir)
    db.cleanup(CI, c[3])  # the older instance lost its rows below commit 3
    assert _plan(reader, c[5], c[9])  # a batch past S reads only the newer one
    with pytest.raises(DataLossError, match=f"{CI}: change data from .* purged by CDC cleanup"):
        _plan(reader, c[1], c[9])
    # the newer instance's start moves with its cleanup; the older one still has what lies
    # below, so that is read there instead of failing
    db.cleanup(v2, c[7])
    got = [
        (r["order_id"], r["_capture_instance"]) for r in _rows(reader, _plan(reader, c[5], c[9]))
    ]
    assert got == [(6, CI), (7, v2), (8, v2), (9, v2)]
    planned = _plan(reader, c[7], c[9])
    db.cleanup(v2, c[9])  # after planning: the executor checks the range's own instance
    with pytest.raises(DataLossError, match=f"{v2}: change data from 0x"):
        _rows(reader, planned)


def test_a_type_change_fails_the_batch_before_it_reads(spark, workdir):
    from mssql_cdc import SchemaChangedError

    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI], columns={CI: COLUMNS})
    c = [db.commit(CI, [(2, _order(i))], at=T0 + timedelta(minutes=i)) for i in range(3)]
    reader = _stream_reader(spark, workdir, columns=None)  # amount DECIMAL(18,2)
    command = "ALTER TABLE dbo.orders ALTER COLUMN amount decimal(20,4)"
    ddl = db.ddl(CI, "amount", command, new_type="DECIMAL(20,4)")
    c += [db.commit(CI, [(2, _order(i, amount="1.2345"))]) for i in (3, 4)]
    assert [r["order_id"] for r in _rows(reader, _plan(reader, c[0], c[2]))] == [1, 2]
    with pytest.raises(SchemaChangedError, match=r"amount DECIMAL\(20,4\) \(read as decimal"):
        _plan(reader, c[2], c[4])
    # restarted: re-inferred as DECIMAL(20,4), the same batch goes through; the DDL an event
    metrics = os.path.join(workdir, "metrics")
    again = _stream_reader(spark, workdir, columns=None, metricsPath=metrics)
    assert [str(r["amount"]) for r in _rows(again, _plan(again, c[2], c[4]))] == ["1.2345"] * 2
    assert [(e["event"], e["lsn"]) for e in _events(metrics).values()] == [("schema_change", ddl)]


def test_a_running_query_stops_at_a_type_change(spark, workdir):
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI], columns={CI: COLUMNS})
    for i in range(3):
        db.commit(CI, [(2, _order(i))], at=T0 + timedelta(minutes=i))
    name = "q_" + uuid.uuid4().hex[:8]
    q = (
        spark.readStream.format("mssql_cdc")
        .options(**_opts(workdir, columns=None, numPartitions=1))
        .load()
        .writeStream.format("memory")
        .queryName(name)
        .option("checkpointLocation", os.path.join(workdir, "ckpt"))
        .trigger(processingTime="1 second")
        .start()
    )
    try:
        q.processAllAvailable()
        command = "ALTER TABLE dbo.orders ALTER COLUMN amount decimal(20,4)"
        db.ddl(CI, "amount", command, new_type="DECIMAL(20,4)")
        db.commit(CI, [(2, _order(3, amount="1.2345"))])
        with pytest.raises(Exception, match="Restart the query to re-infer the schema"):
            q.processAllAvailable()
    finally:
        q.stop()
    assert spark.sql(f"SELECT COUNT(*) FROM {name}").first()[0] == 3  # nothing of that batch


def test_add_and_drop_column_continue_with_an_event_file(spark, workdir):
    from mssql_cdc import SchemaChangedError

    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI], columns={CI: COLUMNS})
    c0 = db.commit(CI, [(2, _order(0))], at=T0)
    metrics = os.path.join(workdir, "metrics")
    reader = _stream_reader(spark, workdir, columns=None, metricsPath=metrics)
    quiet = _stream_reader(spark, workdir, columns=None)
    strict = _stream_reader(spark, workdir, columns=None, schemaChangePolicy="fail")
    add = db.ddl(CI, "note", "ALTER TABLE dbo.orders ADD note varchar(10) NULL")
    drop = db.ddl(CI, "status", "ALTER TABLE dbo.orders DROP COLUMN status")
    row = {k: v for k, v in _order(1).items() if k != "status"}  # captured after the drop
    c1 = db.commit(CI, [(2, row)], at=T0 + timedelta(minutes=1))
    assert [(r["order_id"], r["status"]) for r in _rows(reader, _plan(reader, c0, c1))] == [
        (1, None)
    ]
    assert _events(metrics) == {
        f"event-schema_change-{lsn}.json": {
            "event": "schema_change",
            "capture_instance": CI,
            "lsn": lsn,
            "commit_ts": _ms(0),  # the commit at or before the DDL
            "detail": detail,
        }
        for lsn, detail in [
            (add, "ALTER TABLE dbo.orders ADD note varchar(10) NULL"),
            (drop, "ALTER TABLE dbo.orders DROP COLUMN status"),
        ]
    }
    assert _plan(quiet, c0, c1)  # without metricsPath: a warning in the log only
    with pytest.raises(SchemaChangedError, match="schemaChangePolicy=fail"):
        _plan(strict, c0, c1)
    with pytest.raises(ValueError, match="schemaChangePolicy must be"):
        _stream_reader(spark, workdir, schemaChangePolicy="ignore")


def test_a_newer_instance_with_new_columns_stops_at_its_start(spark, workdir):
    from mssql_cdc import SchemaChangedError

    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI], columns={CI: COLUMNS})
    c = [db.commit(CI, [(2, _noted(i))], at=T0 + timedelta(minutes=i)) for i in range(3)]
    running = _stream_reader(spark, workdir, columns=None)  # loaded before the new instance
    declared = _stream_reader(spark, workdir)  # columns=COLUMNS
    v2 = db.add_capture_instance(CI, f"{COLUMNS}, note STRING")
    c += [db.commit(CI, [(2, _noted(i))], at=T0 + timedelta(minutes=i)) for i in (3, 4)]
    assert [r["order_id"] for r in _rows(running, _plan(running, c[0], c[2]))] == [1, 2]
    with pytest.raises(SchemaChangedError, match=f"reaches capture instance '{v2}' .* note"):
        _plan(running, c[2], c[4])  # before reading past S
    # the declared columns decide: followed in place, the new column not read
    assert [r["order_id"] for r in _rows(declared, _plan(declared, c[2], c[4]))] == [3, 4]
    # restarted, the schema has the new column and the batch goes through
    metrics = os.path.join(workdir, "metrics")
    again = _stream_reader(spark, workdir, columns=None, metricsPath=metrics)
    assert [r["note"] for r in _rows(again, _plan(again, c[2], c[4]))] == ["n3", "n4"]
    assert [e["detail"] for e in _events(metrics).values()] == [f"{CI} -> {v2}"]


def test_a_newer_instance_skipped_ahead_to_is_checked_like_one_reached(spark, workdir):
    from mssql_cdc import SchemaChangedError

    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI], columns={CI: COLUMNS})
    c = [db.commit(CI, [(2, _noted(i))], at=T0 + timedelta(minutes=i)) for i in range(3)]
    reader = _stream_reader(spark, workdir, columns=None, failOnDataLoss="false")
    assert [r["order_id"] for r in _rows(reader, _plan(reader, c[0], c[1]))] == [1]
    v2 = db.add_capture_instance(CI, f"{COLUMNS}, note STRING")
    c += [db.commit(CI, [(2, _noted(i))], at=T0 + timedelta(minutes=i)) for i in (3, 4)]
    db.drop_capture_instance(CI)  # too early: failOnDataLoss=false skips to v2's start
    with pytest.raises(SchemaChangedError, match=f"reaches capture instance '{v2}' .* note"):
        _plan(reader, c[1], c[4])


def test_a_declared_column_no_instance_captures_fails_instead_of_reading_null(spark, workdir):
    from mssql_cdc.source import MssqlCdcSnapshotReader

    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI], columns={CI: COLUMNS})
    c = [db.commit(CI, [(2, _order(i))], at=T0 + timedelta(minutes=i)) for i in range(2)]
    typo = COLUMNS.replace("amount", "amout")
    reader = _stream_reader(spark, workdir, columns=typo)
    with pytest.raises(
        ValueError, match=rf"No capture instance of the table \({CI}\) captures amout"
    ):
        _plan(reader, c[0], c[1])
    snapshot = MssqlCdcSnapshotReader(_opts(workdir, columns=typo, numPartitions=1), reader.schema)
    with pytest.raises(ValueError, match="captures amout"):
        snapshot.partitions()


def test_a_disabled_configured_instance_is_followed_to_the_newer_one(spark, workdir):
    from mssql_cdc.source import MssqlCdcSnapshotReader

    db, v2, _, c = _switch(workdir)
    _run(spark, workdir, columns=None)
    db.drop_capture_instance(CI)  # the DBA's last step, once the stream is past S
    db.commit(CI, [(2, _noted(10))], at=T0 + timedelta(minutes=10))
    batches, out = _run(spark, workdir, columns=None)  # still configured as CI
    assert [b["numInputRows"] for b in batches if b["numInputRows"]] == [1]
    assert {(r["order_id"], r["_capture_instance"]) for r in _read(spark, out).collect()} >= {
        (10, v2)
    }
    # a new stream from before S: those changes were only in the disabled instance
    reader = _stream_reader(spark, workdir, columns=None, startingLsn=c[1])
    with pytest.raises(DataLossError, match=f"or held only by capture instance '{CI}', disabled"):
        _plan(reader, c[1], c[9])
    # cleanup on the newer instance past a stream that is past S: cleanup is named first
    db.cleanup(v2, c[8])
    with pytest.raises(DataLossError, match=f"{v2}: change data from .* purged by CDC cleanup"):
        _plan(reader, c[6], c[9])
    snapshot = MssqlCdcSnapshotReader(_opts(workdir, numPartitions=1), reader.schema)
    assert {p.capture_instance for p in snapshot.partitions()} == {v2}


def test_a_snapshot_reads_null_for_a_column_the_table_no_longer_has(monkeypatch, workdir):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from mssql_cdc.fake import FakeCdcClient
    from mssql_cdc.source import MssqlCdcSnapshotReader

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    db.commit(CI, [(2, {"order_id": 1, "status": "new"})], at=T0)
    # SqlCdcClient matches by column_id (test_client_sql); here: status was dropped
    monkeypatch.setattr(
        FakeCdcClient, "present_columns", lambda self, ci, cols: [c for c in cols if c != "status"]
    )
    schema = StructType(
        [StructField("order_id", IntegerType()), StructField("status", StringType())]
    )
    reader = MssqlCdcSnapshotReader(
        {"backend": "fake", "fakePath": src, "captureInstance": CI, "numPartitions": "1"}, schema
    )
    [part] = reader.partitions()
    assert part.columns == ["order_id"]
    assert [r for b in reader.read(part) for r in b.to_pylist()] == [
        {"order_id": 1, "status": None}
    ]


def test_foreach_batch_finds_the_event_files_of_its_batch(spark, workdir):
    # the sink folds them at the start of foreachBatch (ADR 0023): planning comes first
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI], columns={CI: COLUMNS})
    db.commit(CI, [(2, _order(0))], at=T0)
    metrics, seen = os.path.join(workdir, "metrics"), []

    def sink(df, batch_id):
        seen.append((batch_id, list(_events(metrics))))
        df.count()

    q = (
        spark.readStream.format("mssql_cdc")
        .options(**_opts(workdir, columns=None, numPartitions=1, metricsPath=metrics))
        .load()
        .writeStream.foreachBatch(sink)
        .option("checkpointLocation", os.path.join(workdir, "ckpt"))
        .trigger(processingTime="1 second")
        .start()
    )
    try:
        q.processAllAvailable()
        add = db.ddl(CI, "note", "ALTER TABLE dbo.orders ADD note varchar(10) NULL")
        db.commit(CI, [(2, _order(1))], at=T0 + timedelta(minutes=1))
        q.processAllAvailable()
    finally:
        q.stop()
    assert seen[0] == (0, []) and seen[-1][1] == [f"event-schema_change-{add}.json"]


def test_a_datetimeoffset_read_as_text_becomes_its_utc_instant():
    # arrow-odbc reads datetimeoffset as SQL Server's text form; Spark's TIMESTAMP is UTC
    import pyarrow as pa

    from mssql_cdc.source import _to_schema

    text = ["2026-09-28 13:50:01.1234567 -03:00", None, "2026-09-28 01:00:00 +05:30"]
    target = pa.schema([("o", pa.timestamp("us", "UTC"))])
    out = _to_schema(pa.table({"o": text}), target).column(0).to_pylist()
    assert [v and v.replace(tzinfo=None) for v in out] == [
        datetime(2026, 9, 28, 16, 50, 1, 123456),
        None,
        datetime(2026, 9, 27, 19, 30),
    ]
    # other text, a varchar the columns option reads as TIMESTAMP, is pyarrow's to parse
    other = _to_schema(pa.table({"o": ["2026-09-28T13:50:01Z"]}), target).column(0)
    assert other.to_pylist()[0].replace(tzinfo=None) == datetime(2026, 9, 28, 13, 50, 1)


# --------------------------------------------------------------------------- #
# Chunked snapshots (ADR 0028)
# --------------------------------------------------------------------------- #
def _chunk_reader(src, schema, chunks, lsn, **options):
    from mssql_cdc.source import MssqlCdcSnapshotReader

    opts = {"backend": "fake", "fakePath": src, "captureInstance": CI}
    opts.update(snapshotChunks=json.dumps(chunks), snapshotLsn=lsn, **options)
    return MssqlCdcSnapshotReader(opts, schema)


@pytest.mark.parametrize(
    ("key", "values", "above", "chunk_rows", "kind", "sizes"),
    [
        # integers and a NULL key: slices of one key counted, 5 a chunk; NULL, uncounted, sorts
        # first, into the first
        ("order_id", [None, *range(12)], 99, 5, "int", [6, 5, 2]),
        # sparse integers: the empty keys 10..99 join the chunk before, no chunk is empty
        ("order_id", [None, *range(10), *range(100, 110)], 200, 3, "int", [4, 3, 3, 3, 3, 3, 2]),
        # one string key: keyset bounds, chunk_rows rows each up to the MAX at the open
        ("code", [f"C{i:02}" for i in range(12)], "C99", 5, "keyset", [5, 5, 2]),
        # a composite key with NULLs, as ORDER BY sorts them
        (
            ["region", "id"],
            [(None, "x"), (1, None), (1, "a"), (1, "b"), (2, "a"), (3, "z")],
            (4, "a"),
            2,
            "keyset",
            [2, 2, 2],
        ),
        # more rows per chunk than the table has: one chunk
        ("code", ["a", "b", "c"], "d", 100, "keyset", [3]),
    ],
)
def test_chunk_plans_tile_the_key_space(workdir, key, values, above, chunk_rows, kind, sizes):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from mssql_cdc.client import plan_chunks, snapshot_plan
    from mssql_cdc.fake import FakeCdcClient

    src = os.path.join(workdir, "src")
    keys = [key] if isinstance(key, str) else key
    db = FakeCdcDatabase(src, [CI], keys={CI: key})

    def insert(v):
        db.commit(CI, [(2, {**dict(zip(keys, v if len(keys) > 1 else (v,))), "status": "new"})])

    for v in values:
        insert(v)
    client = FakeCdcClient(src)
    source = client.source_table(CI)
    extent = snapshot_plan(client, CI, source)
    assert extent["kind"] == kind
    insert(above)  # after S, above the MAX: the stream's, and no chunk reads it
    chunks = [[i, *c] for i, c in enumerate(plan_chunks(client, CI, source, extent, chunk_rows))]
    assert chunks[0][1] is None and all(a[2] == b[1] for a, b in pairwise(chunks))

    def typ(name):
        return IntegerType() if name in ("order_id", "region") else StringType()

    schema = StructType([StructField(k, typ(k)) for k in keys] + [StructField("status", typ("s"))])
    reader = _chunk_reader(src, schema, chunks, client.max_lsn())
    parts = reader.partitions()
    read = [
        [tuple(r[k] for k in keys) for b in reader.read(p) for r in b.to_pylist()] for p in parts
    ]
    every = [k for rows in read for k in rows]
    assert sorted(every, key=str) == sorted(
        (v if len(keys) > 1 else (v,) for v in values), key=str
    )  # every row once
    assert [len(rows) for rows in read] == sizes


def test_an_integer_plan_packs_counted_slices_whatever_the_skew(workdir, monkeypatch):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    from mssql_cdc.client import plan_chunks, snapshot_plan
    from mssql_cdc.fake import FakeCdcClient

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    top = 2**63 - 10  # a sentinel near the bigint max, a sparse region, a dense cluster
    ids = [*range(1000), *range(10_000, 510_000, 10_000), top]
    db.commit(CI, [(2, {"order_id": i, "status": "new"}) for i in ids])
    client = FakeCdcClient(src)
    source = client.source_table(CI)
    extent = snapshot_plan(client, CI, source)
    assert extent == {"kind": "int", "lo": 0, "hi": top, "rows": len(ids)}
    widths, real = [], FakeCdcClient.key_buckets

    def counted(self, *args):
        widths.append(args[4])
        return real(self, *args)

    monkeypatch.setattr(FakeCdcClient, "key_buckets", counted)
    plan = plan_chunks(client, CI, source, extent, 100)
    assert plan[0][0] is None and plan[-1][1] == top + 1  # open below, MAX + 1
    assert all(a[1] == b[0] for a, b in pairwise(plan))
    sizes = [sum((lo is None or i >= lo) and i < hi for i in ids) for lo, hi in plan]
    # the top slice holds the cluster and the region: counted again over 0..500000, where the
    # first slice still holds the cluster: counted again over 0..999, in slices of 7 keys
    assert widths[1:] == [2977, 7]
    # 14 slices of 7 a chunk; the cluster's last 20, the sparse region and the sentinel: one
    assert sizes == [98] * 10 + [71]
    assert all(a + b > 100 for a, b in pairwise(sizes))  # no two neighbours fit in one

    schema = StructType([StructField("order_id", LongType()), StructField("status", StringType())])
    chunks = [[i, *c] for i, c in enumerate(plan)]
    reader = _chunk_reader(src, schema, chunks, client.max_lsn())
    read = [[r["order_id"] for r in _rows_of(reader, p)] for p in reader.partitions()]
    assert [len(r) for r in read] == sizes and sorted(i for r in read for i in r) == ids


def test_chunk_rows_are_stamped_numbered_and_leave_metrics(spark, workdir):
    from mssql_cdc.client import plan_chunks, snapshot_plan
    from mssql_cdc.fake import FakeCdcClient

    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    for i in range(6):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    client = FakeCdcClient(src)
    source = client.source_table(CI)
    s = client.max_lsn()
    extent = snapshot_plan(client, CI, source)
    assert extent == {"kind": "int", "lo": 0, "hi": 5, "rows": 6}
    chunks = [[i, *c] for i, c in enumerate(plan_chunks(client, CI, source, extent, 2))]
    assert chunks == [[0, None, 2], [1, 2, 4], [2, 4, 6]]  # MAX + 1
    lsn = db.idle(at=T0 + timedelta(minutes=7))  # the wave's stamp L, at or after S
    metrics = os.path.join(workdir, "metrics")
    opts = {
        "backend": "fake",
        "fakePath": src,
        "captureInstance": CI,
        "columns": "order_id INT, status STRING",
        "snapshotChunks": json.dumps(chunks),
        "snapshotLsn": lsn,
        "metricsPath": metrics,
    }
    df = spark.read.format("mssql_cdc_snapshot").options(**opts).load()
    assert df.columns[-1] == "_chunk" and df.rdd.getNumPartitions() == 3
    rows = df.collect()
    assert sorted((r["_chunk"], r["order_id"]) for r in rows) == [
        (0, 0), (0, 1), (1, 2), (1, 3), (2, 4), (2, 5)
    ]  # fmt: skip
    assert {(r["_start_lsn"], r["_operation"]) for r in rows} == {(lsn, 0)} and lsn >= s
    files = {}
    for name in os.listdir(metrics):
        with open(os.path.join(metrics, name), encoding="utf-8") as fh:
            files[name] = json.load(fh)
    assert sorted(files) == ["chunk-0.json", "chunk-1.json", "chunk-2.json"]
    assert [files[f"chunk-{i}.json"]["rows"] for i in range(3)] == [2, 2, 2]
    assert {m["high_lsn"] for m in files.values()} == {lsn}  # max_lsn after each read

    # a writer commits after the stamp L and before the read: the read sees it, and its
    # change has an LSN after L, so the stream has it too; 9, above MAX, is the stream's alone
    db.commit_before_read(
        CI, [(1, {"order_id": 3, "status": "new"}), (2, {"order_id": 9, "status": "new"})]
    )
    reader = _chunk_reader(src, df.schema, chunks, lsn)
    got = [(p.chunk, r["order_id"]) for p in reader.partitions() for r in _rows_of(reader, p)]
    assert got == [(0, 0), (0, 1), (1, 2), (2, 4), (2, 5)]
    later = client.max_lsn()  # the queued commit's
    assert later > lsn and db.commit(CI, [(2, {"order_id": 10})]) > later  # LSNs keep order
    with pytest.raises(ValueError, match="NOLOCK"):
        _chunk_reader(src, df.schema, chunks, lsn, isolationLevel="readUncommitted")


def _rows_of(reader, partition):
    return [r for b in reader.read(partition) for r in b.to_pylist()]
