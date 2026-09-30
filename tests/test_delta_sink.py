"""Delta sink + finalization. Skipped when Delta jars are unavailable."""

import json
import os
from datetime import datetime, timedelta
from uuid import uuid4

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
        .option("backend", "fake")
        .option("fakePath", os.path.join(path, "src"))
        .option("captureInstance", CI)
        .option("columns", COLUMNS)
        .option("maxCommitsPerBatch", "2")
        .load()
        .writeStream.foreachBatch(delta_sink(target, app_id, facts))
        .option("checkpointLocation", os.path.join(path, "ckpt"))
        .trigger(availableNow=True)
        .start()
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
    fields = {
        f.name: (f.dataType.simpleString(), f.metadata.get("comment"))
        for f in spark.read.format("delta").load(path).schema
    }
    return fields, spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["description"]


def test_tables_are_created_typed_and_commented(delta_spark, workdir):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    db.commit(CI, [(2, {"order_id": 1, "status": "new"})], at=T0)
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    control = os.path.join(workdir, "control")
    q = _stream(spark, workdir, target, "typed-v1", facts)
    finalization.advance(
        spark, control, "bronze_orders", finalization.end_offset_from_progress(q.lastProgress)
    )

    cols, description = _comments(spark, control)
    assert description and "finalized_until" in description
    assert (
        cols["finalized_until"][0] == "timestamp_ntz"
        and "only moves forward" in cols["finalized_until"][1]
    )
    assert cols["updated_at"][0] == cols["end_commit_ts"][0] == "timestamp_ntz"
    assert all(comment for _, comment in cols.values())
    cols, description = _comments(spark, facts)
    assert description and cols["min_commit_ts"][0] == "timestamp_ntz"
    # every time is TIMESTAMP_NTZ in UTC, so differences never depend on the session time zone
    assert {
        cols[c][0]
        for c in ("started_at", "written_at", "max_commit_ts", "lost_from_ts", "lost_to_ts")
    } == {"timestamp_ntz"}
    assert all(comment for _, comment in cols.values())
    cols, description = _comments(spark, target)
    assert description and cols["_start_lsn"][1] and cols["_operation"][1]
    assert cols["order_id"][1] is None  # captured columns keep the source's names and types only
    from mssql_cdc import migrations

    for path, kind in ((control, "control"), (facts, "facts"), (target, "bronze")):  # born current
        props = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["properties"]
        assert props["mssql_cdc.schema_version"] == str(migrations.current_version(kind))


def test_facts_table_at_version_0_gains_every_column_and_the_current_comments(delta_spark, workdir):
    from mssql_cdc import migrations, tables
    from mssql_cdc.migrations.facts import (
        DETAIL_COLUMNS,
        END_COLUMNS,
        EVENT_COLUMNS,
        LAG_COLUMNS,
        NETWORK_COLUMNS,
        RETENTION_COLUMNS,
    )
    from mssql_cdc.sink import FACTS_COLUMNS, FACTS_COMMENT

    spark = delta_spark
    old = os.path.join(workdir, "facts_v0")
    added = (
        NETWORK_COLUMNS
        + RETENTION_COLUMNS
        + EVENT_COLUMNS
        + LAG_COLUMNS
        + END_COLUMNS
        + DETAIL_COLUMNS
    )
    # the facts shape before migration 1, with the comments it was created with
    v0 = [(n, t, "old's") if n == "rows" else (n, t, c) for n, t, c in FACTS_COLUMNS]
    tables.create_if_not_exists(
        spark,
        old,
        [c for c in v0 if c not in added],
        "one row per non-empty batch",
        properties={migrations.SCHEMA_VERSION_PROPERTY: "0"},
    )
    assert migrations.migrate(spark, old, "facts") == 6
    cols, description = _comments(spark, old)
    assert all(name in cols and cols[name][1] for name, _, _ in added)
    # migrations 5 and 6 rewrote the comments whose meaning changed: as a new table has them
    assert {n: cols[n][1] for n, _, _ in FACTS_COLUMNS} == {n: c for n, _, c in FACTS_COLUMNS}
    assert description == FACTS_COMMENT


def test_network_and_read_metrics_reach_the_facts(delta_spark, workdir):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    kept_from = db.idle(at=T0 - timedelta(hours=70))
    for i in range(4):
        last = db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    db.cleanup(CI, kept_from)  # cleanup has deleted up to 70 h before the first commit
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    metrics = os.path.join(workdir, "metrics")
    os.makedirs(metrics)  # a dead attempt's file, from a split this batch does not plan
    with open(os.path.join(metrics, "stale.json"), "w", encoding="utf-8") as fh:
        json.dump({"from_lsn": last, "to_lsn": "0xFFFFFFFFFFFFFFFFFFFF", "bytes": 1e12}, fh)
    source = {
        "backend": "fake",
        "fakePath": os.path.join(workdir, "src"),
        "captureInstance": CI,
        "columns": COLUMNS,
        "numPartitions": "2",
        "metricsPath": metrics,
    }
    q = (
        spark.readStream.format("mssql_cdc")
        .options(**source)
        .load()
        .writeStream.foreachBatch(delta_sink(target, "metrics-v1", facts, metrics_path=metrics))
        .option("checkpointLocation", os.path.join(workdir, "ckpt"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    [row] = spark.read.format("delta").load(facts).collect()
    assert row["read_seconds"] > 0 and 0 < row["read_mb"] < 1e6
    assert row["end_lsn"] == last  # the stale file was removed before the batch was read
    assert row["network_wait_ms"] is None and row["source_rtt_ms"] is None  # the fake has no server
    assert row["retention_watermark_ts"] == T0 - timedelta(hours=70)
    assert row["retention_headroom_hours"] == 70.05  # the batch's last commit is T0 + 3 min
    assert not [f for f in os.listdir(metrics) if f.endswith(".json")]  # folded and removed


def test_capture_and_ingestion_lag_reach_the_facts(delta_spark, workdir):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    for i in range(3):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    metrics = os.path.join(workdir, "metrics")
    q = (
        spark.readStream.format("mssql_cdc")
        .option("backend", "fake")
        .option("fakePath", os.path.join(workdir, "src"))
        .option("captureInstance", CI)
        .option("columns", COLUMNS)
        .option("maxCommitsPerBatch", "2")
        .option("metricsPath", metrics)
        .load()
        .writeStream.foreachBatch(delta_sink(target, "lag-v1", facts, metrics_path=metrics))
        .option("checkpointLocation", os.path.join(workdir, "ckpt"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    first, second = spark.read.format("delta").load(facts).orderBy("batch_id").collect()
    # capture had processed the last commit (T0 + 2 min) when either batch was read
    assert (
        first["source_max_commit_ts"] == second["source_max_commit_ts"] == T0 + timedelta(minutes=2)
    )
    # batch 0 holds the commits up to T0 + 1 min: one minute behind capture; batch 1 caught up
    assert (first["ingestion_lag_seconds"], second["ingestion_lag_seconds"]) == (60.0, 0.0)
    for row in (first, second):  # seen by a partition while the sink processed the batch
        seen = row["source_max_commit_ts"] + timedelta(seconds=row["capture_lag_seconds"])
        assert row["capture_lag_seconds"] >= 0
        assert row["started_at"] - timedelta(milliseconds=1) <= seen <= row["written_at"]


def test_a_quiet_table_is_measured_from_the_end_offset_and_its_empty_batches_write_facts(
    delta_spark, workdir
):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI, "dbo_other"])
    kept_from = db.idle(at=T0 - timedelta(hours=70))
    db.commit(CI, [(2, {"order_id": 0, "status": "new"})], at=T0)
    db.commit("dbo_other", [(2, {"order_id": 0, "status": "new"})], at=T0 + timedelta(minutes=10))
    quiet = db.idle(at=T0 + timedelta(minutes=15))
    db.commit(CI, [(2, {"order_id": 1, "status": "new"})], at=T0 + timedelta(minutes=20))
    last = db.commit(
        "dbo_other", [(2, {"order_id": 1, "status": "new"})], at=T0 + timedelta(minutes=30)
    )
    db.cleanup(CI, kept_from)  # cleanup has deleted up to 70 h before the first commit
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    metrics = os.path.join(workdir, "metrics")
    sink, left = delta_sink(target, "quiet-v1", facts, metrics_path=metrics), []

    def write(df, batch_id):
        sink(df, batch_id)
        left.append(os.listdir(metrics))

    q = (
        spark.readStream.format("mssql_cdc")
        .option("backend", "fake")
        .option("fakePath", os.path.join(workdir, "src"))
        .option("captureInstance", CI)
        .option("columns", COLUMNS)
        .option("numPartitions", "2")
        .option("maxCommitsPerBatch", "2")  # lsn_time_mapping rows: idle entries and dbo_other's
        .option("metricsPath", metrics)
        .load()
        .writeStream.foreachBatch(write)
        .option("checkpointLocation", os.path.join(workdir, "ckpt"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    rows = spark.read.format("delta").load(facts).orderBy("batch_id").collect()
    assert [r["rows"] for r in rows] == [1, 0, 1]  # batch 1 read only dbo_other and an idle entry
    assert spark.read.format("delta").load(target).count() == 2  # the empty batch wrote nothing
    assert left == [[], [], []]  # every batch folded and removed its files
    empty = rows[1]
    assert (empty["min_lsn"], empty["max_lsn"], empty["max_commit_ts"]) == (None, None, None)
    assert (empty["deletes"], empty["inserts"], empty["updates"], empty["read_mb"]) == (0, 0, 0, 0)
    assert empty["end_lsn"] == quiet
    # where the stream is (end_commit_ts), not the batch's last change: 15, 30 and 30 minutes
    assert [r["end_commit_ts"] for r in rows] == [T0 + timedelta(minutes=m) for m in (0, 15, 30)]
    assert rows[2]["end_lsn"] == last  # past the batch's last change (T0 + 20 min)
    assert all(r["retention_watermark_ts"] == T0 - timedelta(hours=70) for r in rows)
    assert [r["retention_headroom_hours"] for r in rows] == [70.0, 70.25, 70.5]
    # capture had processed dbo_other's commit at T0 + 30 min when every batch was read
    assert [r["ingestion_lag_seconds"] for r in rows] == [1800.0, 900.0, 0.0]


def test_stream_facade_declares_the_options_once(delta_spark, workdir):
    from mssql_cdc import stream

    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    db.commit(CI, [(2, {"order_id": 1, "status": "new"})], at=T0)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    options = {
        "backend": "fake",
        "fakePath": os.path.join(workdir, "src"),
        "captureInstance": CI,
        "columns": COLUMNS,
    }
    q = stream(spark, options).to_delta(
        target, "facade-v1", ckpt, facts, trigger={"availableNow": True}
    )
    q.awaitTermination()
    assert spark.read.format("delta").load(target).count() == 1
    [row] = spark.read.format("delta").load(facts).collect()
    assert row["read_seconds"] > 0  # metrics defaulted under the (local) checkpoint
    assert os.path.isdir(os.path.join(ckpt, "_mssql_cdc_metrics"))
    assert "metricsPath" not in options  # the caller's dict is not changed
    # an explicit metricsPath is shared by the streams of a job: each gets its own directory
    shared = os.path.join(workdir, "metrics")
    q = stream(spark, {**options, "metricspath": shared}).to_delta(
        target + "2", "facade-v2", ckpt + "2", facts, trigger={"availableNow": True}
    )
    q.awaitTermination()
    assert os.listdir(shared) == ["facade-v2"]


def test_migrations_bring_an_older_table_up_once(delta_spark, workdir, monkeypatch):
    from mssql_cdc import migrations, tables
    from mssql_cdc.migrations import facts as facts_migrations

    spark = delta_spark
    old = os.path.join(workdir, "facts_old")
    tables.create_if_not_exists(spark, old, [("app_id", "STRING", None)])  # unstamped: version 0
    monkeypatch.setattr(
        facts_migrations,
        "MIGRATIONS",
        [
            migrations.Migration(
                "add x",
                lambda s, t: migrations.add_columns(
                    s, t, [("x", "BIGINT", "added by a migration")]
                ),
            )
        ],
    )

    assert migrations.migrate(spark, old, "facts") == 1
    field = spark.read.format("delta").load(old).schema["x"]
    assert (
        field.dataType.simpleString() == "bigint"
        and field.metadata["comment"] == "added by a migration"
    )
    history = spark.sql(f"DESCRIBE HISTORY delta.`{old}`").count()
    assert migrations.migrate(spark, old, "facts") == 1  # already current: nothing runs
    assert spark.sql(f"DESCRIBE HISTORY delta.`{old}`").count() == history
    props = spark.sql(f"DESCRIBE DETAIL delta.`{old}`").first()["properties"]
    assert props["mssql_cdc.schema_version"] == "1"


def test_replayed_batch_is_ignored(delta_spark, workdir):
    spark = delta_spark
    target = os.path.join(workdir, "bronze")
    df = spark.createDataFrame(
        [(1, 2, "0x" + "0" * 20, None)],
        "order_id int, _operation int, _start_lsn string, _commit_ts timestamp_ntz",
    )
    write = delta_sink(target, "replay-test")
    write(df, 7)
    write(df, 7)  # same batch id replayed after a failure
    assert spark.read.format("delta").load(target).count() == 1


def test_a_type_bronze_cannot_take_says_to_enable_type_widening(delta_spark, workdir):
    from mssql_cdc.client import SchemaChangedError
    from mssql_cdc.sink import _write

    spark, target = delta_spark, os.path.join(workdir, "bronze")
    _write(spark.sql("SELECT CAST(1.5 AS DECIMAL(9,2)) AS amount"), target, None, None)
    wider = spark.sql("SELECT CAST(2.5 AS DECIMAL(18,4)) AS amount")  # after an ALTER COLUMN
    with pytest.raises(SchemaChangedError, match=r"delta\.enableTypeWidening' = 'true'"):
        _write(wider, target, "type-test", 0, merge_schema=True)
    spark.sql(
        f"ALTER TABLE delta.`{target}` SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')"
    )
    _write(wider, target, "type-test", 0, merge_schema=True)  # mergeSchema widens it
    assert dict(spark.read.format("delta").load(target).dtypes)["amount"] == "decimal(18,4)"


def test_the_readers_events_become_event_rows_and_new_columns_join_bronze(delta_spark, workdir):
    from pyspark.sql import functions as F

    spark = delta_spark
    target, facts, metrics = (os.path.join(workdir, n) for n in ("bronze", "facts", "metrics"))
    os.makedirs(metrics)
    added, switch = "0x0000002A000001000021", "0x0000002A000001000031"

    def plan():  # what the reader leaves while it plans the batch (ADR 0023)
        for kind, lsn, ts, detail in (
            ("schema_change", added, "2026-09-28T13:51:00.000", "note: ALTER TABLE ADD note"),
            ("capture_instance_switched", switch, None, "dbo_orders -> dbo_orders_v2"),
        ):
            event = {"event": kind, "capture_instance": CI, "lsn": lsn, "commit_ts": ts}
            with open(os.path.join(metrics, f"event-{kind}-{lsn}.json"), "w") as fh:
                json.dump({**event, "detail": detail}, fh)

    df = spark.createDataFrame(
        [(CI, switch, switch, 2, 1, T0, 1, "new")],
        "_capture_instance STRING, _start_lsn STRING, _seqval STRING, _operation INT, "
        "_command_id INT, _commit_ts TIMESTAMP_NTZ, order_id INT, status STRING",
    )
    write = delta_sink(target, "events-v1", facts, metrics_path=metrics)
    plan()
    write(df, 3)
    plan()  # a replay: the reader plans the batch again and leaves the same files
    write(df, 3)
    assert not os.listdir(metrics)  # folded, then removed
    rows = spark.read.format("delta").load(facts).collect()
    assert sorted((r["event"] or "", r["batch_id"], r["rows"]) for r in rows) == [
        ("", 3, 1),
        ("capture_instance_switched", 3, 0),
        ("schema_change", 3, 0),
    ]  # once each
    ddl, switched = sorted((r for r in rows if r["event"]), key=lambda r: r["min_lsn"])
    assert ddl["min_lsn"] == ddl["max_lsn"] == ddl["end_lsn"] == added
    assert ddl["end_commit_ts"] == datetime(2026, 9, 28, 13, 51)
    assert (ddl["detail"], ddl["app_id"], ddl["target"]) == (
        "note: ALTER TABLE ADD note",
        "events-v1",
        target,
    )
    assert switched["detail"] == "dbo_orders -> dbo_orders_v2" and switched["end_commit_ts"] is None

    write(df.withColumn("note", F.lit("gift")), 4)  # a column a newer instance captures
    bronze = spark.read.format("delta").load(target)
    assert sorted((r["_batch_id"], r["note"]) for r in bronze.collect()) == [(3, None), (4, "gift")]


def test_a_bronze_table_at_version_0_gets_the_capture_instance_comments(delta_spark, workdir):
    from mssql_cdc import migrations, tables
    from mssql_cdc.sink import BRONZE_COLUMN_COMMENTS, BRONZE_COMMENT

    old = os.path.join(workdir, "bronze_v0")
    tables.create_if_not_exists(  # includeCommandId=false: no _command_id to comment
        delta_spark,
        old,
        [("_capture_instance", "STRING", "old's"), ("_start_lsn", "STRING", "old's")],
        properties={migrations.SCHEMA_VERSION_PROPERTY: "0"},
    )
    assert migrations.migrate(delta_spark, old, "bronze") == 1
    cols, description = _comments(delta_spark, old)
    assert cols["_capture_instance"][1] == BRONZE_COLUMN_COMMENTS["_capture_instance"]
    assert cols["_start_lsn"][1] == "old's" and description == BRONZE_COMMENT


# -- a newer capture instance of the table (ADR 0023) ----------------------------------------
def _switching(workdir):
    """A keyed fake whose capture instance reports its columns (the stream infers them), with
    orders 0..2 inserted, and the stream options for it."""
    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"}, columns={CI: COLUMNS})
    for i in range(3):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    return db, {"backend": "fake", "fakePath": src, "captureInstance": CI}


def test_a_switch_adds_the_new_column_and_its_events_and_a_rerun_takes_no_snapshot(
    delta_spark, workdir
):
    from mssql_cdc import stream

    spark = delta_spark
    db, options = _switching(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run():
        q = stream(spark, options).to_delta(
            target, "switch-v1", ckpt, facts, trigger={"availableNow": True}, bootstrap=True
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    run()  # the snapshot of orders 0..2
    added = db.ddl(CI, "note", False, "ALTER TABLE [dbo].[orders] ADD [note] varchar(20) NULL")
    db.commit(  # the old instance does not capture note
        CI,
        [(3, {"order_id": 1, "status": "new"}), (4, {"order_id": 1, "status": "paid"})],
        at=T0 + timedelta(minutes=5),
    )
    v2 = db.add_capture_instance(CI, COLUMNS + ", note STRING")
    start = db.commit(
        CI, [(2, {"order_id": 3, "status": "new", "note": "gift"})], at=T0 + timedelta(minutes=6)
    )
    bronze = run()  # load() takes both instances' columns; below v2's start the old one is read
    got = {(r["order_id"], r["_operation"]): r for r in bronze.where("_operation != 0").collect()}
    assert (got[(1, 4)]["_capture_instance"], got[(1, 4)]["note"]) == (CI, None)
    assert (got[(3, 2)]["_capture_instance"], got[(3, 2)]["note"]) == (v2, "gift")
    assert got[(3, 2)]["_start_lsn"] == start
    events = {
        r["event"]: r
        for r in spark.read.format("delta").load(facts).where("batch_id IS NOT NULL").collect()
        if r["event"]
    }
    assert (
        events["schema_change"]["min_lsn"] == added and "note" in events["schema_change"]["detail"]
    )
    switched = events["capture_instance_switched"]
    assert switched["detail"] == f"{CI} -> {v2}" and switched["min_lsn"] == start

    assert _snapshots(run()) == 1  # the snapshot is found under the older instance's name
    db.drop_capture_instance(CI)  # the documented last step; the stream follows v2
    db.commit(CI, [(1, {"order_id": 0, "status": "new"})], at=T0 + timedelta(minutes=7))
    bronze = run()
    assert _snapshots(bronze) == 1
    assert bronze.where(f"_capture_instance = '{v2}' AND _operation = 1").count() == 1


def test_snapshot_on_switch_fills_a_column_only_the_newer_instance_captures(
    delta_spark, workdir, latest
):
    from mssql_cdc import stream

    spark = delta_spark
    db, options = _switching(workdir)
    target, ckpt = os.path.join(workdir, "bronze"), os.path.join(workdir, "ckpt")

    def run():
        q = stream(spark, options).to_delta(
            target,
            "fill-v1",
            ckpt,
            trigger={"availableNow": True},
            bootstrap=True,
            snapshot_on_switch=True,
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    run()
    db.ddl(CI, "note", False, "ALTER TABLE [dbo].[orders] ADD [note] varchar(20) NULL")
    db.commit(  # captured without note: the old instance does not have it
        CI,
        [
            (3, {"order_id": 0, "status": "new", "note": None}),
            (4, {"order_id": 0, "status": "new", "note": "vip"}),
        ],
        at=T0 + timedelta(minutes=5),
    )
    db.add_capture_instance(CI, COLUMNS + ", note STRING")
    db.commit(
        CI, [(2, {"order_id": 3, "status": "new", "note": "gift"})], at=T0 + timedelta(minutes=6)
    )
    bronze = run()
    assert bronze.where("_operation IN (3, 4)").count() == 0  # note alone: no change row in v1
    assert _snapshots(bronze) == 2  # the bootstrap, and one after the switch
    assert latest(bronze, "order_id", "note") == [(0, "vip"), (1, None), (2, None), (3, "gift")]
    assert _snapshots(run()) == 2  # the next batches do not cross a switch


def test_a_snapshot_after_a_dropped_column_reads_it_as_null(delta_spark, workdir):
    from mssql_cdc import stream

    spark = delta_spark
    db, options = _switching(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    q = stream(spark, options).to_delta(
        target, "drop-v1", ckpt, facts, trigger={"availableNow": True}, bootstrap=True
    )
    q.awaitTermination()
    dropped = db.ddl(CI, "status", False, "ALTER TABLE [dbo].[orders] DROP COLUMN [status]")
    db.commit(CI, [(2, {"order_id": 3})], at=T0 + timedelta(minutes=5))  # CDC captures NULL now
    q = stream(spark, options).to_delta(
        target, "drop-v1", ckpt, facts, trigger={"availableNow": True}
    )
    q.awaitTermination()  # the stream reads past the DROP: it warns, and the facts get an event
    [event] = spark.read.format("delta").load(facts).where("event = 'schema_change'").collect()
    assert event["min_lsn"] == dropped and "status" in event["detail"]
    # SQL Server refuses to select the dropped column: the snapshot reads NULL for it instead
    taken = stream(spark, options).snapshot(target, resnapshot=True)
    rows = spark.read.format("delta").load(target).where(f"_start_lsn = '{taken['lsn']}'")
    assert sorted((r["order_id"], r["status"]) for r in rows.where("_operation = 0").collect()) == [
        (0, None),
        (1, None),
        (2, None),
        (3, None),
    ]


def test_resnapshot_recovers_changes_only_a_dropped_older_instance_held(
    delta_spark, workdir, latest
):
    from mssql_cdc import stream

    spark = delta_spark
    db, options = _switching(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run():
        q = stream(spark, options).to_delta(
            target,
            "gone-v1",
            ckpt,
            facts,
            trigger={"availableNow": True},
            on_data_loss="resnapshot",
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    run()  # from earliest: orders 0..2
    db.commit(CI, [(1, {"order_id": 0, "status": "new"})], at=T0 + timedelta(minutes=3))
    db.add_capture_instance(CI, COLUMNS)
    db.commit(CI, [(2, {"order_id": 5, "status": "new"})], at=T0 + timedelta(minutes=4))
    db.drop_capture_instance(CI)  # too early: the delete of order 0 was only in it
    bronze = run()
    assert _generation(ckpt)["generation"] == 1
    assert latest(bronze, "order_id", "status") == [(1, "new"), (2, "new"), (5, "new")]


def test_the_recovery_checks_the_capture_instance_the_source_reads_next(workdir):
    from mssql_cdc.fake import FakeCdcClient
    from mssql_cdc.pipeline import _lost

    db, _ = _switching(workdir)
    old = db.commit(CI, [(1, {"order_id": 0, "status": "new"})], at=T0 + timedelta(minutes=3))
    db.add_capture_instance(CI, COLUMNS)
    s = db.commit(CI, [(2, {"order_id": 5, "status": "new"})], at=T0 + timedelta(minutes=4))
    db.commit(CI, [(2, {"order_id": 6, "status": "new"})], at=T0 + timedelta(minutes=5))
    last = db.commit(CI, [(2, {"order_id": 7, "status": "new"})], at=T0 + timedelta(minutes=6))
    db.cleanup(CI, last)  # sp_cdc_cleanup_change_table on the older instance alone
    client = FakeCdcClient(db.path)
    assert _lost(client, CI, s) is None  # the newer instance holds everything after S
    assert _lost(client, CI, old) == last  # before S the older one is read: cleanup passed it


def _snapshots(df) -> int:
    return df.where("_operation = 0").select("_start_lsn").distinct().count()


def _generation(ckpt) -> dict:
    with open(os.path.join(ckpt, "_mssql_cdc_generation.json"), encoding="utf-8") as fh:
        return json.load(fh)


def test_bootstrap_snapshots_once_and_the_stream_continues_from_it(delta_spark, workdir, latest):
    from mssql_cdc import stream

    spark = delta_spark
    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    for i in range(6):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    db.commit(CI, [(1, {"order_id": 4, "status": "new"})], at=T0 + timedelta(minutes=7))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=8)))  # retention lost all of that history
    target, ckpt = os.path.join(workdir, "bronze"), os.path.join(workdir, "ckpt")
    options = {
        "backend": "fake",
        "fakePath": src,
        "captureInstance": CI,
        "columns": COLUMNS,
        "numPartitions": "2",
    }

    def run(ci=CI):
        q = stream(spark, {**options, "captureInstance": ci}).to_delta(
            target, "boot-v1", ckpt, trigger={"availableNow": True}, bootstrap=True
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    first = run("DBO_ORDERS")  # SQL Server matches the name ignoring case; so does the rerun
    assert first.where("_operation = 0").count() == 5 and first.count() == 5  # 0..5 minus 4
    db.commit(
        CI,
        [(3, {"order_id": 1, "status": "new"}), (4, {"order_id": 1, "status": "paid"})],
        at=T0 + timedelta(minutes=9),
    )
    db.commit(CI, [(2, {"order_id": 9, "status": "new"})], at=T0 + timedelta(minutes=10))
    second = run()  # a rerun: the same snapshot, then only the new changes
    assert second.where("_operation = 0").count() == 5 and second.count() == 8
    assert (
        stream(spark, options).snapshot(target)["lsn"]
        == second.where("_operation = 0").first()["_start_lsn"]
    )  # the snapshot's LSN, not the newest change's

    # the latest image per key, as a MERGE downstream would apply it, is the source table now
    assert latest(second, "order_id", "status") == [
        (0, "new"),
        (1, "paid"),
        (2, "new"),
        (3, "new"),
        (5, "new"),
        (9, "new"),
    ]

    with pytest.raises(ValueError, match="one or the other"):
        stream(spark, {**options, "startingLsn": "latest"}).to_delta(
            target, "x", ckpt, bootstrap=True
        )


def _orders(workdir, n=3):
    """A keyed fake with orders 0..n-1 inserted, and the stream options for it."""
    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    for i in range(n):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    return db, {"backend": "fake", "fakePath": src, "captureInstance": CI, "columns": COLUMNS}


def test_data_loss_resnapshots_into_a_new_generation_once_per_interval(
    delta_spark, workdir, latest
):
    from mssql_cdc import DataLossError, stream

    spark = delta_spark
    db, options = _orders(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run(**kw):  # the same job every time
        kw = {"bootstrap": True, "on_data_loss": "resnapshot", **kw}
        q = stream(spark, options).to_delta(
            target, "loss-v1", ckpt, facts, trigger={"availableNow": True}, **kw
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    def events():
        return (
            spark.read.format("delta")
            .load(facts)
            .where("event IS NOT NULL")
            .orderBy("written_at")
            .collect()
        )

    assert run().count() == 3  # the snapshot only; the commit at the snapshot LSN is not replayed
    [boot] = events()
    assert (boot["event"], boot["app_id"], boot["batch_id"], boot["rows"]) == (
        "bootstrap",
        "loss-v1",
        None,
        3,
    )
    assert boot["min_lsn"] == boot["max_lsn"] == boot["end_lsn"] and boot["lost_from_ts"] is None
    assert boot["end_commit_ts"] == boot["max_commit_ts"]  # the offset the stream starts from
    db.commit(CI, [(2, {"order_id": 3, "status": "new"})], at=T0 + timedelta(minutes=3))
    run()  # the checkpoint's last processed commit is now T0 + 3 min
    db.commit(
        CI,
        [(3, {"order_id": 1, "status": "new"}), (4, {"order_id": 1, "status": "paid"})],
        at=T0 + timedelta(minutes=4),
    )
    db.commit(CI, [(1, {"order_id": 2, "status": "new"})], at=T0 + timedelta(minutes=5))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=7)))  # purged before the stream read them

    bronze = run()
    assert _generation(ckpt)["generation"] == 1 and _snapshots(bronze) == 2
    _, resnap = events()
    assert (resnap["event"], resnap["app_id"], resnap["batch_id"], resnap["rows"]) == (
        "resnapshot",
        "loss-v1.g1",
        None,
        3,
    )
    # the gap: from the last processed commit to the retention watermark at detection
    assert (resnap["lost_from_ts"], resnap["lost_to_ts"]) == (
        T0 + timedelta(minutes=3),
        T0 + timedelta(minutes=7),
    )
    assert resnap["retention_watermark_ts"] == resnap["lost_to_ts"]
    # a crash just before the state file: the rerun reuses the snapshot and the event row
    os.remove(os.path.join(ckpt, "_mssql_cdc_generation.json"))
    bronze = run()
    assert _generation(ckpt)["generation"] == 1 and _snapshots(bronze) == 2 and len(events()) == 2

    db.commit(
        CI,
        [(3, {"order_id": 0, "status": "new"}), (4, {"order_id": 0, "status": "paid"})],
        at=T0 + timedelta(minutes=8),
    )
    db.commit(CI, [(1, {"order_id": 3, "status": "new"})], at=T0 + timedelta(minutes=9))
    # no new loss: generation 1 carries on, no snapshot, no event; the state is read whatever
    # on_data_loss says
    bronze = run(on_data_loss="fail")
    assert _snapshots(bronze) == 2 and len(events()) == 2
    gen1 = os.path.join(ckpt, "_generations", "1")
    assert os.listdir(os.path.join(gen1, "commits")) and os.path.isdir(
        os.path.join(gen1, "_mssql_cdc_metrics")
    )
    batches = spark.read.format("delta").load(facts).where("batch_id IS NOT NULL").collect()
    assert {(r["app_id"], r["event"]) for r in batches} == {("loss-v1", None), ("loss-v1.g1", None)}
    # key 2, deleted during the gap, has no delete row: rebuilt from the newest snapshot it is gone
    assert latest(bronze, "order_id", "status") == [
        (0, "paid"),
        (1, "paid"),
    ]  # the fake's table now

    db.commit(CI, [(2, {"order_id": 7, "status": "new"})], at=T0 + timedelta(minutes=10))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=11)))
    with pytest.raises(DataLossError, match="resnapshot_interval_days"):
        run()  # a second loss within the interval needs a person, not another snapshot
    assert _snapshots(spark.read.format("delta").load(target)) == 2
    assert _generation(ckpt)["generation"] == 1

    bronze = run(resnapshot_interval_days=0)  # the person decided: generation 2
    assert _generation(ckpt)["generation"] == 2 and _snapshots(bronze) == 3
    assert [(e["event"], e["app_id"]) for e in events()][2:] == [("resnapshot", "loss-v1.g2")]
    assert latest(bronze, "order_id", "status") == [(0, "paid"), (1, "paid"), (7, "new")]


def test_resnapshot_reuses_a_snapshot_taken_before_a_crash(delta_spark, workdir):
    from mssql_cdc import stream

    spark = delta_spark
    db, options = _orders(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run():
        q = stream(spark, options).to_delta(
            target,
            "crash-v1",
            ckpt,
            facts,
            trigger={"availableNow": True},
            on_data_loss="resnapshot",
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    run()  # from earliest, no bootstrap
    db.commit(CI, [(1, {"order_id": 0, "status": "new"})], at=T0 + timedelta(minutes=3))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=4)))
    taken = stream(spark, options).snapshot(target, resnapshot=True)  # then the job died
    bronze = run()
    assert bronze.where("_operation = 0").count() == 2  # that snapshot (orders 1, 2), no other
    state = _generation(ckpt)
    assert (state["generation"], state["snapshot_lsn"]) == (1, taken["lsn"])
    [event] = spark.read.format("delta").load(facts).where("event = 'resnapshot'").collect()
    assert event["app_id"] == "crash-v1.g1"
    assert event["rows"] is None and event["duration_ms"] is None  # reused: nothing was read


def test_a_resnapshot_purged_before_it_ends_counts_as_an_attempt(
    delta_spark, workdir, monkeypatch, latest
):
    from mssql_cdc import DataLossError, stream
    from mssql_cdc.pipeline import CdcStream

    spark = delta_spark
    db, options = _orders(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run(**kw):
        q = stream(spark, options).to_delta(
            target,
            "slow-v1",
            ckpt,
            facts,
            trigger={"availableNow": True},
            on_data_loss="resnapshot",
            **kw,
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    run()
    db.commit(CI, [(1, {"order_id": 0, "status": "new"})], at=T0 + timedelta(minutes=3))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=4)))
    take = CdcStream._take_snapshot

    def slow(self, target, ci):  # cleanup passes the snapshot's LSN while the table is read
        taken = take(self, target, ci)
        db.commit(CI, [(2, {"order_id": 5, "status": "new"})], at=T0 + timedelta(minutes=5))
        db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=6)))
        return taken

    monkeypatch.setattr(CdcStream, "_take_snapshot", slow)
    with pytest.raises(DataLossError, match="took longer"):
        run()
    monkeypatch.undo()
    with pytest.raises(DataLossError, match="or a failed one"):
        run()  # no second full read within the interval
    assert _snapshots(spark.read.format("delta").load(target)) == 1
    bronze = run(resnapshot_interval_days=0)  # the purged snapshot is not reused: a new one
    assert _snapshots(bronze) == 2 and _generation(ckpt)["generation"] == 1
    [event] = spark.read.format("delta").load(facts).where("event = 'resnapshot'").collect()
    assert (event["app_id"], event["rows"]) == ("slow-v1.g1", 3)
    assert latest(bronze, "order_id", "status") == [(1, "new"), (2, "new"), (5, "new")]


def test_resnapshot_of_an_emptied_table_is_marked_by_its_event(
    delta_spark, workdir, monkeypatch, latest
):
    from mssql_cdc import stream
    from mssql_cdc.fake import FakeCdcClient

    spark = delta_spark
    db, options = _orders(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run():
        q = stream(spark, options).to_delta(
            target,
            "empty-v1",
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            on_data_loss="resnapshot",
        )
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    run()  # the bootstrap; nothing after it, so no batch is committed

    def unset(self, ci):
        raise ValueError("fn_cdc_get_min_lsn returned 0x00")  # capture has not run yet

    monkeypatch.setattr(FakeCdcClient, "min_lsn", unset)
    run()  # nothing to read, nothing to check: like the driver guard, min_lsn is not asked
    monkeypatch.undo()
    db.commit(
        CI, [(1, {"order_id": i, "status": "new"}) for i in range(3)], at=T0 + timedelta(minutes=5)
    )
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=6)))  # every row deleted, then purged
    bronze = run()
    assert _generation(ckpt)["generation"] == 1 and _snapshots(bronze) == 1  # no rows to write
    fdf = spark.read.format("delta").load(facts)
    [event] = fdf.where("event = 'resnapshot'").collect()
    assert event["rows"] == 0 and event["max_lsn"] > bronze.agg({"_start_lsn": "max"}).first()[0]
    assert latest(bronze, "order_id", "status", facts=fdf) == []  # the event is the rebuild point


def test_resnapshot_recovers_a_stream_started_at_a_purged_lsn(delta_spark, workdir):
    from mssql_cdc import stream

    spark = delta_spark
    db, options = _orders(workdir)
    given = db.idle(at=T0 + timedelta(minutes=3))
    db.commit(CI, [(1, {"order_id": 0, "status": "new"})], at=T0 + timedelta(minutes=4))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=5)))
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    q = stream(spark, {**options, "startingLsn": given}).to_delta(
        target, "given-v1", ckpt, facts, trigger={"availableNow": True}, on_data_loss="resnapshot"
    )
    q.awaitTermination()  # recovered on the first run, before batch 0 could fail
    assert _generation(ckpt)["generation"] == 1
    assert spark.read.format("delta").load(target).where("_operation = 0").count() == 2


def test_on_data_loss_is_checked_before_the_query_starts(spark, workdir):
    from mssql_cdc import stream
    from mssql_cdc.pipeline import _last_offset

    _, options = _orders(workdir, n=1)
    cdc, target, ckpt = (
        stream(spark, options),
        os.path.join(workdir, "bronze"),
        os.path.join(workdir, "ckpt"),
    )
    with pytest.raises(ValueError, match="on_data_loss"):
        cdc.to_delta(target, "x", ckpt, on_data_loss="skip")
    # the generation state is a file, and the checkpoint is read from Python
    for uri in ("abfss://c@a.dfs.core.windows.net/x", "/dbfs/ckpt/orders"):
        with pytest.raises(ValueError, match="same directory"):
            cdc.to_delta(target, "x", uri, "facts", on_data_loss="resnapshot")
    with pytest.raises(ValueError, match="facts_table"):
        cdc.to_delta(target, "x", ckpt, on_data_loss="resnapshot")
    assert not spark.streams.active
    for name in ("offsets", "commits"):
        os.makedirs(os.path.join(ckpt, name))
        with open(os.path.join(ckpt, name, "0"), "w", encoding="utf-8") as fh:
            fh.write('v2\n{}\n{"lsn": "0x00000000000000000001"}\n')
    with pytest.raises(ValueError, match="offset log version 'v2'"):
        _last_offset(ckpt)


def test_managed_tables_by_name_take_the_catalog_branches(delta_spark, workdir):
    from mssql_cdc import migrations, stream, tables
    from mssql_cdc.migrations.facts import (
        DETAIL_COLUMNS,
        END_COLUMNS,
        EVENT_COLUMNS,
        LAG_COLUMNS,
        NETWORK_COLUMNS,
        RETENTION_COLUMNS,
    )
    from mssql_cdc.sink import FACTS_COLUMNS

    spark = delta_spark
    suffix = uuid4().hex[:8]
    bronze, facts, control, old = (
        f"{n}_{suffix}" for n in ("bronze", "facts", "control", "facts_v0")
    )
    db, options = _orders(workdir)
    ckpt = os.path.join(workdir, "ckpt")

    def run():
        q = stream(spark, options).to_delta(
            bronze, "named-v1", ckpt, facts, bootstrap=True, trigger={"availableNow": True}
        )
        q.awaitTermination()
        return q

    try:
        run()
        db.commit(CI, [(2, {"order_id": 9, "status": "new"})], at=T0 + timedelta(hours=1))
        q = run()
        assert spark.table(bronze).count() == 4  # the snapshot of orders 0..2, then order 9
        # the first run's batch 0 plans no range (it starts at the snapshot LSN) but writes
        # its facts row too
        assert sorted((r["event"] or "batch", r["rows"]) for r in spark.table(facts).collect()) == [
            ("batch", 0),
            ("batch", 1),
            ("bootstrap", 3),
        ]
        end = finalization.end_offset_from_progress(q.lastProgress)
        assert isinstance(finalization.advance(spark, control, "bronze_orders", end), datetime)
        for name, kind in ((bronze, "bronze"), (facts, "facts"), (control, "control")):
            props = spark.sql(f"DESCRIBE DETAIL {name}").first()["properties"]
            assert props["mssql_cdc.schema_version"] == str(migrations.current_version(kind))
        added = (
            NETWORK_COLUMNS
            + RETENTION_COLUMNS
            + EVENT_COLUMNS
            + LAG_COLUMNS
            + END_COLUMNS
            + DETAIL_COLUMNS
        )
        tables.create_if_not_exists(
            spark,
            old,
            [c for c in FACTS_COLUMNS if c not in added],
            properties={migrations.SCHEMA_VERSION_PROPERTY: "0"},
        )
        # add_columns through saveAsTable, set_comments on a table name
        assert migrations.migrate(spark, old, "facts") == 6
        assert {name for name, _, _ in added} <= set(spark.table(old).columns)
    finally:
        for name in (bronze, facts, control, old):
            spark.sql(f"DROP TABLE IF EXISTS {name}")
