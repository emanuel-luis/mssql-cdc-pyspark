"""Type checks of the payload types and the source options, run by mypy, never by pytest.

The payloads' keys as a query of the facts table reads them; and ``SourceOptions``: what takes
options takes it, a plain dict and any mapping alike, while a misspelt or non-string option
in one is a type error (``warn_unused_ignores`` fails on an ignore that no longer hides one).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from pyspark.sql import SparkSession
from typing_extensions import assert_type

from mssql_cdc import (
    BatchDetail,
    DataSkippedDetail,
    SnapshotChunkDetail,
    SnapshotCompletionDetail,
    SnapshotMode,
    SnapshotOpenDetail,
    SnapshotPlanDetail,
    SourceOptions,
    WaveMetadata,
    apply_changes,
    reconcile,
    start_many,
    stream,
)
from mssql_cdc.payloads import SnapshotExtent, WaveChunk


def details(text: str) -> None:
    opened: SnapshotOpenDetail = json.loads(text)
    assert_type(opened["mode"], SnapshotMode)
    assert_type(opened["generation"], int)
    assert_type(opened["lost_from_ts"], str | None)
    assert_type(opened.get("keys"), list[str] | None)  # a chunked one's
    plan = opened["plan"]
    assert_type(plan, SnapshotExtent)
    if plan["kind"] == "int":
        assert_type(plan["hi"], int)
    else:
        assert_type(plan["max"], Any)
    opened["keyz"]  # type: ignore[typeddict-item]

    planned: SnapshotPlanDetail = json.loads(text)
    assert_type(planned["chunks"], list[list[Any]])
    chunk: SnapshotChunkDetail = json.loads(text)
    assert_type(chunk["last"], bool)
    assert_type(chunk["wave"], int)
    done: SnapshotCompletionDetail = json.loads(text)
    assert_type(done["last_lsn"], str)
    skipped: DataSkippedDetail = json.loads(text)
    assert_type(skipped["from"], str)
    assert_type(skipped["certain"], bool)
    assert_type(skipped.get("reason"), str | None)  # a task's only
    batch: BatchDetail = json.loads(text)
    assert_type(batch["warnings"], list[str])
    wave: WaveMetadata = json.loads(text)
    assert_type(wave["chunks"][0], WaveChunk)
    assert_type(wave["chunks"][0]["read_mb"], float | None)


def options(spark: SparkSession, plain: dict[str, Any], mapping: Mapping[str, str]) -> None:
    typed: SourceOptions = {"captureInstance": "dbo_orders", "connectionString": "Server=h"}
    stream(spark, typed)
    stream(spark, plain)
    stream(spark, mapping)
    stream(spark, {"captureinstance": "dbo_orders", "numPartitions": 4})  # as Spark takes them
    reconcile(spark, typed, "silver.orders", bronze="bronze.orders", control_table="c")
    apply_changes(spark, "bronze.orders", "silver.orders", control_table="c", options=typed)
    start_many(
        spark,
        typed,
        {"dbo_orders": {"numPartitions": "2"}, "dbo_items": typed},
        target="bronze.{ci}",
        app_id="{ci}-v1",
        checkpoint="/ckpt/{ci}",
    )
    _misspelt: SourceOptions = {"conectionString": "Server=h"}  # type: ignore[typeddict-unknown-key]
    _number: SourceOptions = {"numPartitions": 4}  # type: ignore[typeddict-item]
