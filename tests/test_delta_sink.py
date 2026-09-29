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
    assert {cols[c][0] for c in ("started_at", "written_at", "max_commit_ts", "lost_from_ts",
                                 "lost_to_ts")} == {"timestamp_ntz"}
    assert all(comment for _, comment in cols.values())
    cols, description = _comments(spark, target)
    assert description and cols["_start_lsn"][1] and cols["_operation"][1]
    assert cols["order_id"][1] is None  # captured columns keep the source's names and types only
    from mssql_cdc import migrations

    for path, kind in ((control, "control"), (facts, "facts"), (target, "bronze")):  # born current
        props = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["properties"]
        assert props["mssql_cdc.schema_version"] == str(migrations.current_version(kind))


def test_facts_table_at_version_0_gains_the_network_retention_and_event_columns(delta_spark, workdir):
    from mssql_cdc import migrations, tables
    from mssql_cdc.migrations.facts import EVENT_COLUMNS, NETWORK_COLUMNS, RETENTION_COLUMNS
    from mssql_cdc.sink import FACTS_COLUMNS

    spark = delta_spark
    old = os.path.join(workdir, "facts_v0")
    added = NETWORK_COLUMNS + RETENTION_COLUMNS + EVENT_COLUMNS
    v0 = [c for c in FACTS_COLUMNS if c not in added]  # the facts shape before migration 1
    tables.create_if_not_exists(spark, old, v0, properties={migrations.SCHEMA_VERSION_PROPERTY: "0"})
    assert migrations.migrate(spark, old, "facts") == 3
    fields = {f.name: f for f in spark.read.format("delta").load(old).schema}
    assert all(name in fields and fields[name].metadata.get("comment") for name, _, _ in added)


def test_network_and_read_metrics_reach_the_facts(delta_spark, workdir):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    kept_from = db.idle(at=T0 - timedelta(hours=70))
    for i in range(4):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    db.cleanup(CI, kept_from)  # cleanup has deleted up to 70 h before the first commit
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    metrics = os.path.join(workdir, "metrics")
    source = {"backend": "fake", "fakePath": os.path.join(workdir, "src"), "captureInstance": CI,
              "columns": COLUMNS, "numPartitions": "2", "metricsPath": metrics}
    q = (spark.readStream.format("mssql_cdc").options(**source).load()
         .writeStream.foreachBatch(delta_sink(target, "metrics-v1", facts, metrics_path=metrics))
         .option("checkpointLocation", os.path.join(workdir, "ckpt")).trigger(availableNow=True).start())
    q.awaitTermination()
    [row] = spark.read.format("delta").load(facts).collect()
    assert row["read_seconds"] > 0 and row["read_mb"] > 0
    assert row["network_wait_ms"] is None and row["source_rtt_ms"] is None  # the fake has no server
    assert row["retention_watermark_ts"] == T0 - timedelta(hours=70)
    assert row["retention_headroom_hours"] == 70.05  # the batch's last commit is T0 + 3 min
    assert not [f for f in os.listdir(metrics) if f.endswith(".json")]  # folded and removed


def test_stream_facade_declares_the_options_once(delta_spark, workdir):
    from mssql_cdc import stream

    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    db.commit(CI, [(2, {"order_id": 1, "status": "new"})], at=T0)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    options = {"backend": "fake", "fakePath": os.path.join(workdir, "src"), "captureInstance": CI,
               "columns": COLUMNS}
    q = stream(spark, options).to_delta(target, "facade-v1", ckpt, facts, trigger={"availableNow": True})
    q.awaitTermination()
    assert spark.read.format("delta").load(target).count() == 1
    [row] = spark.read.format("delta").load(facts).collect()
    assert row["read_seconds"] > 0  # metrics defaulted under the (local) checkpoint
    assert os.path.isdir(os.path.join(ckpt, "_mssql_cdc_metrics"))
    assert "metricsPath" not in options  # the caller's dict is not changed


def test_migrations_bring_an_older_table_up_once(delta_spark, workdir, monkeypatch):
    from mssql_cdc import migrations, tables
    from mssql_cdc.migrations import facts as facts_migrations

    spark = delta_spark
    old = os.path.join(workdir, "facts_old")
    tables.create_if_not_exists(spark, old, [("app_id", "STRING", None)])  # unstamped: version 0
    monkeypatch.setattr(facts_migrations, "MIGRATIONS", [migrations.Migration(
        "add x", lambda s, t: migrations.add_columns(s, t, [("x", "BIGINT", "added by a migration")]))])

    assert migrations.migrate(spark, old, "facts") == 1
    field = spark.read.format("delta").load(old).schema["x"]
    assert field.dataType.simpleString() == "bigint" and field.metadata["comment"] == "added by a migration"
    history = spark.sql(f"DESCRIBE HISTORY delta.`{old}`").count()
    assert migrations.migrate(spark, old, "facts") == 1  # already current: nothing runs
    assert spark.sql(f"DESCRIBE HISTORY delta.`{old}`").count() == history
    props = spark.sql(f"DESCRIBE DETAIL delta.`{old}`").first()["properties"]
    assert props["mssql_cdc.schema_version"] == "1"


def test_replayed_batch_is_ignored(delta_spark, workdir):
    spark = delta_spark
    target = os.path.join(workdir, "bronze")
    df = spark.createDataFrame([(1, 2, "0x" + "0" * 20, None)],
                               "order_id int, _operation int, _start_lsn string, _commit_ts timestamp_ntz")
    write = delta_sink(target, "replay-test")
    write(df, 7)
    write(df, 7)  # same batch id replayed after a failure
    assert spark.read.format("delta").load(target).count() == 1


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
    options = {"backend": "fake", "fakePath": src, "captureInstance": CI, "columns": COLUMNS,
               "numPartitions": "2"}

    def run():
        q = stream(spark, options).to_delta(target, "boot-v1", ckpt, trigger={"availableNow": True},
                                            bootstrap=True)
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    first = run()
    assert first.where("_operation = 0").count() == 5 and first.count() == 5  # 0..5 minus 4
    db.commit(CI, [(3, {"order_id": 1, "status": "new"}), (4, {"order_id": 1, "status": "paid"})],
              at=T0 + timedelta(minutes=9))
    db.commit(CI, [(2, {"order_id": 9, "status": "new"})], at=T0 + timedelta(minutes=10))
    second = run()  # a rerun: the same snapshot, then only the new changes
    assert second.where("_operation = 0").count() == 5 and second.count() == 8

    # the latest image per key, as a MERGE downstream would apply it, is the source table now
    assert latest(second, "order_id", "status") == [
        (0, "new"), (1, "paid"), (2, "new"), (3, "new"), (5, "new"), (9, "new")]

    with pytest.raises(ValueError, match="one or the other"):
        stream(spark, {**options, "startingLsn": "latest"}).to_delta(target, "x", ckpt, bootstrap=True)


def _orders(workdir, n=3):
    """A keyed fake with orders 0..n-1 inserted, and the stream options for it."""
    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, [CI], keys={CI: "order_id"})
    for i in range(n):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    return db, {"backend": "fake", "fakePath": src, "captureInstance": CI, "columns": COLUMNS}


def test_data_loss_resnapshots_into_a_new_generation_once_per_interval(delta_spark, workdir, latest):
    from mssql_cdc import DataLossError, stream

    spark = delta_spark
    db, options = _orders(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run(**kw):  # the same job every time
        kw = {"bootstrap": True, "on_data_loss": "resnapshot", **kw}
        q = stream(spark, options).to_delta(target, "loss-v1", ckpt, facts, trigger={"availableNow": True},
                                            **kw)
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    def events():
        return (spark.read.format("delta").load(facts).where("event IS NOT NULL")
                .orderBy("written_at").collect())

    run()
    [boot] = events()
    assert (boot["event"], boot["app_id"], boot["batch_id"], boot["rows"]) == ("bootstrap", "loss-v1", None, 3)
    assert boot["min_lsn"] == boot["max_lsn"] and boot["lost_from_ts"] is None
    db.commit(CI, [(2, {"order_id": 3, "status": "new"})], at=T0 + timedelta(minutes=3))
    run()  # the checkpoint's last processed commit is now T0 + 3 min
    db.commit(CI, [(3, {"order_id": 1, "status": "new"}), (4, {"order_id": 1, "status": "paid"})],
              at=T0 + timedelta(minutes=4))
    db.commit(CI, [(1, {"order_id": 2, "status": "new"})], at=T0 + timedelta(minutes=5))
    db.cleanup(CI, db.idle(at=T0 + timedelta(minutes=7)))  # purged before the stream read them

    bronze = run()
    assert _generation(ckpt)["generation"] == 1 and _snapshots(bronze) == 2
    _, resnap = events()
    assert (resnap["event"], resnap["app_id"], resnap["batch_id"], resnap["rows"]) == (
        "resnapshot", "loss-v1.g1", None, 3)
    # the gap: from the last processed commit to the retention watermark at detection
    assert (resnap["lost_from_ts"], resnap["lost_to_ts"]) == (T0 + timedelta(minutes=3),
                                                             T0 + timedelta(minutes=7))
    assert resnap["retention_watermark_ts"] == resnap["lost_to_ts"]
    # a crash just before the state file: the rerun reuses the snapshot and the event row
    os.remove(os.path.join(ckpt, "_mssql_cdc_generation.json"))
    bronze = run()
    assert _generation(ckpt)["generation"] == 1 and _snapshots(bronze) == 2 and len(events()) == 2

    db.commit(CI, [(3, {"order_id": 0, "status": "new"}), (4, {"order_id": 0, "status": "paid"})],
              at=T0 + timedelta(minutes=8))
    db.commit(CI, [(1, {"order_id": 3, "status": "new"})], at=T0 + timedelta(minutes=9))
    # no new loss: generation 1 carries on, no snapshot, no event; the state is read whatever
    # on_data_loss says
    bronze = run(on_data_loss="fail")
    assert _snapshots(bronze) == 2 and len(events()) == 2
    gen1 = os.path.join(ckpt, "_generations", "1")
    assert os.listdir(os.path.join(gen1, "commits")) and os.path.isdir(os.path.join(gen1, "_mssql_cdc_metrics"))
    batches = spark.read.format("delta").load(facts).where("batch_id IS NOT NULL").collect()
    assert {(r["app_id"], r["event"]) for r in batches} == {("loss-v1", None), ("loss-v1.g1", None)}
    # key 2, deleted during the gap, has no delete row: rebuilt from the newest snapshot it is gone
    assert latest(bronze, "order_id", "status") == [(0, "paid"), (1, "paid")]  # the fake's table now

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
        q = stream(spark, options).to_delta(target, "crash-v1", ckpt, facts, trigger={"availableNow": True},
                                            on_data_loss="resnapshot")
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


def test_a_resnapshot_purged_before_it_ends_counts_as_an_attempt(delta_spark, workdir, monkeypatch, latest):
    from mssql_cdc import DataLossError, stream
    from mssql_cdc.pipeline import CdcStream

    spark = delta_spark
    db, options = _orders(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run(**kw):
        q = stream(spark, options).to_delta(target, "slow-v1", ckpt, facts, trigger={"availableNow": True},
                                            on_data_loss="resnapshot", **kw)
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


def test_resnapshot_of_an_emptied_table_is_marked_by_its_event(delta_spark, workdir, monkeypatch, latest):
    from mssql_cdc import stream
    from mssql_cdc.fake import FakeCdcClient

    spark = delta_spark
    db, options = _orders(workdir)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run():
        q = stream(spark, options).to_delta(target, "empty-v1", ckpt, facts, trigger={"availableNow": True},
                                            bootstrap=True, on_data_loss="resnapshot")
        q.awaitTermination()
        return spark.read.format("delta").load(target)

    run()  # the bootstrap; nothing after it, so no batch is committed

    def unset(self, ci):
        raise ValueError("fn_cdc_get_min_lsn returned 0x00")  # capture has not run yet

    monkeypatch.setattr(FakeCdcClient, "min_lsn", unset)
    run()  # nothing to read, nothing to check: like the driver guard, min_lsn is not asked
    monkeypatch.undo()
    db.commit(CI, [(1, {"order_id": i, "status": "new"}) for i in range(3)], at=T0 + timedelta(minutes=5))
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
        target, "given-v1", ckpt, facts, trigger={"availableNow": True}, on_data_loss="resnapshot")
    q.awaitTermination()  # recovered on the first run, before batch 0 could fail
    assert _generation(ckpt)["generation"] == 1
    assert spark.read.format("delta").load(target).where("_operation = 0").count() == 2


def test_on_data_loss_is_checked_before_the_query_starts(spark, workdir):
    from mssql_cdc import stream
    from mssql_cdc.pipeline import _last_offset

    _, options = _orders(workdir, n=1)
    cdc, target, ckpt = stream(spark, options), os.path.join(workdir, "bronze"), os.path.join(workdir, "ckpt")
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
