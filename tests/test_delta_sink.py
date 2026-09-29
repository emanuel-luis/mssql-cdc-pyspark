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
    from mssql_cdc import migrations

    for path, kind in ((control, "control"), (facts, "facts"), (target, "bronze")):  # born current
        props = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["properties"]
        assert props["mssql_cdc.schema_version"] == str(migrations.current_version(kind))


def test_facts_table_at_version_0_gains_the_network_columns(delta_spark, workdir):
    from mssql_cdc import migrations, tables
    from mssql_cdc.migrations.facts import NETWORK_COLUMNS
    from mssql_cdc.sink import FACTS_COLUMNS

    spark = delta_spark
    old = os.path.join(workdir, "facts_v0")
    v0 = [c for c in FACTS_COLUMNS if c not in NETWORK_COLUMNS]  # the facts shape before migration 1
    tables.create_if_not_exists(spark, old, v0, properties={migrations.SCHEMA_VERSION_PROPERTY: "0"})
    assert migrations.migrate(spark, old, "facts") == 1
    fields = {f.name: f for f in spark.read.format("delta").load(old).schema}
    assert all(name in fields and fields[name].metadata.get("comment") for name, _, _ in NETWORK_COLUMNS)


def test_network_and_read_metrics_reach_the_facts(delta_spark, workdir):
    spark = delta_spark
    db = FakeCdcDatabase(os.path.join(workdir, "src"), [CI])
    for i in range(4):
        db.commit(CI, [(2, {"order_id": i, "status": "new"})], at=T0 + timedelta(minutes=i))
    target, facts = os.path.join(workdir, "bronze"), os.path.join(workdir, "facts")
    metrics = os.path.join(workdir, "metrics")
    source = {"backend": "fake", "fakePath": os.path.join(workdir, "src"), "captureInstance": CI,
              "columns": COLUMNS, "numPartitions": "2", "metricsPath": metrics}
    q = (spark.readStream.format("mssql_cdc").options(**source).load()
         .writeStream.foreachBatch(delta_sink(target, "metrics-v1", facts, source_options=source,
                                              metrics_path=metrics))
         .option("checkpointLocation", os.path.join(workdir, "ckpt")).trigger(availableNow=True).start())
    q.awaitTermination()
    [row] = spark.read.format("delta").load(facts).collect()
    assert row["read_seconds"] > 0 and row["read_mb"] > 0
    assert row["network_wait_ms"] is None and row["source_rtt_ms"] is None  # the fake has no server
    assert not [f for f in os.listdir(metrics) if f.endswith(".json")]  # folded and removed


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
