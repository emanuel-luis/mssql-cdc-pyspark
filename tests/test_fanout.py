"""start_many: one to_delta stream per capture instance (ADR 0027). Skipped without Delta."""

import os
from datetime import datetime, timedelta

import pytest

from mssql_cdc import (
    DataLossError,
    SchemaChangedError,
    await_all,
    is_data_loss,
    is_schema_changed,
    start_many,
    stop_all,
)
from mssql_cdc.fake import FakeCdcDatabase

pytestmark = pytest.mark.delta
T0 = datetime(2026, 9, 28, 13, 50)
COLUMNS = {"dbo_orders": "order_id INT, status STRING", "dbo_customers": "id INT, name STRING"}


def _fake(workdir):
    """Two tracked tables with a commit each, the shared options, and the templates."""
    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, list(COLUMNS), columns=COLUMNS)
    db.commit("dbo_orders", [(2, {"order_id": 1, "status": "new"})], at=T0)
    db.commit("dbo_orders", [(2, {"order_id": 2, "status": "new"})], at=T0)
    db.commit("dbo_customers", [(2, {"id": 7, "name": "Ana"})], at=T0 + timedelta(minutes=1))
    templates = {
        "target": os.path.join(workdir, "bronze_{ci}"),
        "app_id": "{ci}-v1",
        "checkpoint": os.path.join(workdir, "ckpt", "{ci}"),
    }
    return db, {"backend": "fake", "fakePath": src}, templates


def test_the_error_helpers_recognise_the_library_errors_themselves():
    assert is_data_loss(DataLossError("x")) and not is_data_loss(SchemaChangedError("x"))
    assert is_schema_changed(SchemaChangedError("x")) and not is_schema_changed(ValueError("x"))
    # a message that merely mentions the name is no such error
    assert not is_data_loss(RuntimeError("DataLossError: retry with failOnDataLoss=false"))


def _bronze(spark, workdir, ci):
    return spark.read.format("delta").load(os.path.join(workdir, f"bronze_{ci}"))


def test_each_capture_instance_streams_into_its_own_target(delta_spark, workdir):
    spark = delta_spark
    _, options, templates = _fake(workdir)
    facts = os.path.join(workdir, "facts")
    with pytest.raises(ValueError, match=r"app_id='orders-v1' must contain '\{ci\}'"):
        start_many(spark, options, list(COLUMNS), **{**templates, "app_id": "orders-v1"})

    queries = start_many(
        spark,
        options,
        list(COLUMNS),
        facts_table=facts,
        trigger={"availableNow": True},
        **templates,
    )
    assert await_all(queries) == {}
    assert [q.name for q in queries.values()] == ["dbo_orders-v1", "dbo_customers-v1"]
    orders, customers = (_bronze(spark, workdir, ci).collect() for ci in COLUMNS)
    assert sorted((r["order_id"], r["_capture_instance"]) for r in orders) == [
        (1, "dbo_orders"),
        (2, "dbo_orders"),
    ]
    assert [(r["id"], r["name"], r["_capture_instance"]) for r in customers] == [
        (7, "Ana", "dbo_customers")
    ]
    # one shared facts table, its rows keyed by each stream's app_id and target
    rows = spark.read.format("delta").load(facts).collect()
    assert {(r["app_id"], r["target"], r["rows"]) for r in rows} == {
        ("dbo_orders-v1", os.path.join(workdir, "bronze_dbo_orders"), 2),
        ("dbo_customers-v1", os.path.join(workdir, "bronze_dbo_customers"), 1),
    }
    assert all(r["read_seconds"] is not None for r in rows)  # metrics under each checkpoint
    for ci in COLUMNS:
        assert os.listdir(os.path.join(workdir, "ckpt", ci, "commits"))


def test_a_failing_stream_leaves_the_others_running(delta_spark, workdir):
    from pyspark.errors import StreamingQueryException

    spark = delta_spark
    db, options, templates = _fake(workdir)
    given = db.idle(at=T0 + timedelta(minutes=2))
    db.commit("dbo_orders", [(2, {"order_id": 3, "status": "new"})], at=T0 + timedelta(minutes=3))
    db.cleanup("dbo_orders", db.idle(at=T0 + timedelta(minutes=4)))  # purged before it was read
    # the default trigger keeps the queries running; on_data_loss="fail" is the default
    tables = {"dbo_orders": {"startingLsn": given}, "dbo_customers": {}}
    queries = start_many(spark, options, tables, **templates)
    try:
        with pytest.raises(StreamingQueryException, match="re-snapshot is required"):
            queries["dbo_orders"].awaitTermination(120)
        customers = queries["dbo_customers"]
        assert customers.isActive
        db.commit("dbo_customers", [(2, {"id": 8, "name": "Bia"})], at=T0 + timedelta(minutes=5))
        customers.processAllAvailable()
        assert sorted(r["id"] for r in _bronze(spark, workdir, "dbo_customers").collect()) == [7, 8]
        failed = await_all(queries, timeout=1)
        assert list(failed) == ["dbo_orders"] and customers.isActive
        # Spark's exception, recognised as the DataLossError the source raised
        assert is_data_loss(failed["dbo_orders"])
        assert not is_schema_changed(failed["dbo_orders"])
    finally:
        stop_all(queries)
    assert not customers.isActive

    # a start that raises stops the queries started before it
    with pytest.raises(Exception, match="dbo_missing"):
        start_many(spark, options, ["dbo_customers", "dbo_missing"], **templates)
    assert "dbo_customers-v1" not in [q.name for q in spark.streams.active]
