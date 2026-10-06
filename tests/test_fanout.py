"""start_many: one to_delta stream per capture instance (ADR 0027). The engine tests are
skipped without Delta; the rest run start_many and await_all on autospecced mocks."""

import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import DEFAULT, call, create_autospec, patch

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


# -- start_many and await_all on autospecced mocks --------------------------------------
TEMPLATES = {"target": "bronze_{ci}", "app_id": "{ci}-v1", "checkpoint": "ckpt/{ci}"}


def _queries(n):
    from pyspark.sql.streaming import StreamingQuery

    return [create_autospec(StreamingQuery, instance=True) for _ in range(n)]


@contextmanager
def _mocked(started):
    """stream() and migrations.ensure autospecced, each to_delta returning, or raising, the
    next of ``started``. Yields the stream() and ensure mocks, and how many times ensure had
    run as each stream was made."""
    from mssql_cdc import fanout, migrations
    from mssql_cdc.pipeline import CdcStream

    with (
        patch.object(
            fanout, "stream", autospec=True, return_value=create_autospec(CdcStream, instance=True)
        ) as stream,
        patch.object(migrations, "ensure", autospec=True) as ensure,
    ):
        ensured: list[int] = []
        stream.side_effect = lambda spark, options: ensured.append(ensure.call_count) or DEFAULT
        stream.return_value.to_delta.side_effect = started
        yield stream, ensure, ensured


def test_start_many_gives_each_stream_its_options_over_the_shared_ones():
    from mssql_cdc.sink import FACTS_COLUMNS, FACTS_COMMENT

    spark, trigger = object(), {"availableNow": True}
    shared = {"backend": "fake", "NumPartitions": "4", "columns": "id INT", "CaptureInstance": "x"}
    own = {"dbo_orders": {"NUMPARTITIONS": "1", "captureINSTANCE": "y"}, "dbo_customers": {}}
    q1, q2 = _queries(2)
    with _mocked([q1, q2]) as (stream, ensure, ensured):
        queries = start_many(spark, shared, own, facts_table="facts", trigger=trigger, **TEMPLATES)
    assert queries == {"dbo_orders": q1, "dbo_customers": q2}
    # one of each option whatever its case: the stream's own wins, captureInstance is its name
    orders = {"NUMPARTITIONS": "1", "captureInstance": "dbo_orders"}
    customers = {"NumPartitions": "4", "captureInstance": "dbo_customers"}
    assert stream.call_args_list == [
        call(spark, {"backend": "fake", "columns": "id INT", **orders}),
        call(spark, {"backend": "fake", "NumPartitions": "4", "columns": "id INT", **customers}),
    ]
    assert stream.return_value.to_delta.call_args_list == [
        call(f"bronze_{ci}", f"{ci}-v1", f"ckpt/{ci}", facts_table="facts", trigger=trigger)
        for ci in own
    ]
    # the facts table once, before the first stream: writers creating it together conflict
    ensure.assert_called_once_with(spark, "facts", "facts", FACTS_COLUMNS, FACTS_COMMENT)
    assert ensured == [1, 1]


def test_a_start_that_raises_stops_the_queries_started_before_it():
    [q1] = _queries(1)
    missing = ValueError("Capture instance 'dbo_missing' not found")
    with _mocked([q1, missing]) as (_, ensure, _), pytest.raises(ValueError) as raised:
        start_many(object(), {"backend": "fake"}, ["dbo_customers", "dbo_missing"], **TEMPLATES)
    assert raised.value is missing  # raised as it was
    q1.stop.assert_called_once_with()
    ensure.assert_not_called()  # no facts_table


def test_await_all_waits_for_each_query_within_one_timeout(monkeypatch):
    from pyspark.errors import StreamingQueryException

    from mssql_cdc import fanout

    failed, slow, late = _queries(3)
    error = StreamingQueryException("boom")
    failed.awaitTermination.side_effect = error  # read back through exception()
    failed.exception.return_value = error
    slow.exception.return_value = late.exception.return_value = None
    clock = iter([100.0, 100.0, 107.5, 111.0])  # the deadline at 110: 10 s, then 2.5, then none
    monkeypatch.setattr(fanout, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    queries = {"a": failed, "b": slow, "c": late}
    assert await_all(queries, timeout=10) == {"a": error}
    failed.awaitTermination.assert_called_once_with(10)
    slow.awaitTermination.assert_called_once_with(3)  # whole seconds, rounded up
    late.awaitTermination.assert_not_called()  # past the deadline: still running
    for query in queries.values():
        query.reset_mock()
    assert await_all(queries) == {"a": error}  # no timeout: each until it stops
    for query in queries.values():
        query.awaitTermination.assert_called_once_with()
