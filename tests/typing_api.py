"""Type checks of the public API, run by mypy (``files`` in pyproject.toml), never by pytest.

The results' types and the mode ``Literal``s; a Spark Connect session and query where a classic
one goes; and the call forms 0.3 made keyword-only, and
wrong modes, which must stay type errors: ``warn_unused_ignores`` fails on an ignore that no
longer hides one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
from pyspark.sql.connect.streaming.query import StreamingQuery as ConnectStreamingQuery
from pyspark.sql.streaming.query import StreamingQuery
from typing_extensions import assert_type

from mssql_cdc import (
    ApplyResult,
    BackfillState,
    BackfillStatus,
    Offset,
    ReconcileResult,
    apply_changes,
    await_all,
    finalization,
    reconcile,
    register,
    start_many,
    stop_all,
    stream,
)
from mssql_cdc.sink import delta_sink
from mssql_cdc.spark import get_spark


def results(spark: SparkSession, options: dict[str, Any], copy: DataFrame) -> None:
    cdc = stream(spark, options)
    offset = cdc.snapshot("bronze.orders", resnapshot=True)
    assert_type(offset, Offset)
    assert_type(offset["lsn"], str)
    assert_type(cdc.seed("bronze.orders", copy, offset["lsn"]), Offset)
    status = cdc.backfill("bronze.orders", app_id="orders-v1", facts_table="ops.facts")
    assert_type(status, BackfillStatus)
    assert_type(status["state"], BackfillState)
    assert_type(status["chunks_total"], int | None)
    applied = apply_changes(
        spark, "bronze.orders", "silver.orders", capture_instance="dbo_orders", control_table="c"
    )
    assert_type(applied, ApplyResult)
    assert_type(applied["finalized_until"], datetime | None)
    result = reconcile(spark, options, "silver.orders", bronze="bronze.orders", control_table="c")
    assert_type(result, ReconcileResult)
    assert_type(result["report"], DataFrame)
    query = cdc.to_delta(
        "bronze.orders",
        "orders-v1",
        "/ckpt",
        "ops.facts",
        bootstrap=True,
        on_data_loss="resnapshot",
        snapshot="chunked",
    )
    assert_type(query, StreamingQuery)
    # an offset is what advance() takes, as end_offset_from_progress's dict is
    finalization.advance(spark, "c", "bronze.orders", offset, granularity="day")
    assert_type(finalization.end_offset_from_progress(query.lastProgress), Offset | None)


def spark_connect(
    spark: ConnectSparkSession, options: dict[str, Any], query: ConnectStreamingQuery
) -> None:
    # not a subclass of the classic SparkSession or StreamingQuery: SparkSessionLike takes both
    register(spark)
    cdc = stream(spark, options)
    cdc.snapshot("bronze.orders")
    queries = start_many(
        spark, options, ["dbo_orders"], target="b.{ci}", app_id="{ci}", checkpoint="/c/{ci}"
    )
    apply_changes(spark, "bronze.orders", "silver.orders", control_table="c")
    reconcile(spark, options, "silver.orders", bronze="bronze.orders", control_table="c")
    finalization.advance(spark, "c", "bronze.orders", None)
    finalization.finalized_until(spark, "c", "bronze.orders")
    finalization.is_final(spark, "c", "bronze.orders", datetime(2026, 1, 1))
    finalization.track(spark, query, "c", "bronze.orders").join(timeout=60)
    await_all({"dbo_orders": query, **queries})
    stop_all({"dbo_orders": query})


def old_forms(spark: SparkSession, options: dict[str, Any], query: StreamingQuery) -> None:
    cdc = stream(spark, options)
    cdc.to_delta("t", "a", "/c", "f", {"availableNow": True})  # type: ignore[call-arg]
    cdc.snapshot("t", True)  # type: ignore[call-arg]
    apply_changes(spark, "b", "s", "dbo_t", ["k"], control_table="c")  # type: ignore[call-arg]
    reconcile(spark, options, "s", ["k"], bronze="b", control_table="c")  # type: ignore[call-arg]
    finalization.advance(spark, "c", "t", None, "day")  # type: ignore[call-arg]
    finalization.track(spark, query, "c", "t", "day")  # type: ignore[call-arg]
    finalization.candidate(None, "day")  # type: ignore[call-arg]
    tracker = finalization.FinalizationListener(spark, "r", "c", "t", "day")  # type: ignore[call-arg]
    tracker.join(60)  # type: ignore[call-arg]
    finalization.end_offset_from_progress(query.lastProgress, 0)  # type: ignore[call-arg]
    await_all({}, 600)  # type: ignore[call-arg]
    delta_sink("t", "a", "f", "/metrics")  # type: ignore[call-arg]
    get_spark("app", "local[1]", False)  # type: ignore[call-arg]


def wrong_modes(spark: SparkSession, options: dict[str, Any]) -> None:
    cdc = stream(spark, options)
    cdc.to_delta("t", "a", "/c", snapshot="chunks")  # type: ignore[arg-type]
    cdc.to_delta("t", "a", "/c", on_data_loss="skip")  # type: ignore[arg-type]
    cdc.backfill("t", app_id="a", facts_table="f", isolation="dirty")  # type: ignore[arg-type]
    apply_changes(spark, "b", "s", control_table="c", granularity="week")  # type: ignore[arg-type]
