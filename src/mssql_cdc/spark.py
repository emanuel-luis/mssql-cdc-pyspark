"""SparkSession helper: reuse the platform session, or build a local one."""

from __future__ import annotations


def get_spark(app_name: str = "mssql-cdc", master: str = "local[*]", delta: bool = True):
    """Return the active SparkSession (Databricks, EMR, Fabric...) or create a
    local one, with Delta configured when ``delta-spark`` is installed."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
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
