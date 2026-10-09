"""SparkSession helpers: reuse the platform session or build a local one; count cores; a
Spark schema in Arrow on every supported PySpark."""

from __future__ import annotations

import functools
import inspect
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    import pyarrow as pa
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from .types import SparkSessionLike


@functools.cache
def _takes_timezone(to_arrow_schema: Callable[..., pa.Schema]) -> bool:
    return "timezone" in inspect.signature(to_arrow_schema).parameters


def _arrow_schema(schema: StructType) -> pa.Schema:
    """``schema`` in Arrow, TIMESTAMP columns as UTC instants, on every supported PySpark:
    4.2 takes ``timezone`` (and fails a TIMESTAMP column without it); 4.0 and 4.1 take no
    ``timezone`` but ``timestamp_utc``, which defaults to UTC."""
    from pyspark.sql.pandas.types import to_arrow_schema

    if _takes_timezone(to_arrow_schema):
        return to_arrow_schema(schema, timezone="UTC")
    return to_arrow_schema(schema)


def available_cores(spark: SparkSessionLike) -> int:
    """Cores the session's compute runs tasks on (``defaultParallelism``); 0 when unknown,
    as on Spark Connect, which has no ``sparkContext``. ``numPartitions=auto`` then uses the
    CPU count of the node that plans, not of the process that called ``register()``."""
    try:
        cores = int(spark.sparkContext.defaultParallelism)
    except Exception:  # noqa: BLE001 - counting cores must never break a read
        return 0
    return max(0, cores)


def get_spark(
    app_name: str = "mssql-cdc", master: str = "local[*]", *, delta: bool = True
) -> SparkSession:
    """Return the active SparkSession (Databricks, EMR, Fabric...) or create a
    local one, with Delta configured when ``delta-spark`` is installed (``delta``,
    keyword-only). With ``SPARK_REMOTE`` set, a Spark Connect session on that server, which
    has its own configuration: ``master`` and ``delta`` are not used."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if "SPARK_REMOTE" in os.environ:  # PySpark refuses a master next to it
        return SparkSession.builder.appName(app_name).getOrCreate()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
    )
    if delta:
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError:
            delta = False
    if delta:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()
