import os
import shutil
import tempfile

import pytest


def _builder(delta: bool):
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.master("local[2]")
        .appName("mssql-cdc-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="mssql-cdc-wh-"))  # static conf
    )
    if delta:
        from delta import configure_spark_with_delta_pip

        b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ).config("spark.databricks.delta.snapshotPartitions", "1")  # tiny tables: not 50 tasks per read
        b = configure_spark_with_delta_pip(b)
    return b


@pytest.fixture(scope="session")
def spark():
    """One session for the whole run. Uses Delta when the jars resolve (set
    MSSQL_CDC_TEST_DELTA=0 to skip trying), otherwise plain Spark."""
    session, has_delta = None, False
    if os.environ.get("MSSQL_CDC_TEST_DELTA", "1") != "0":
        try:
            session = _builder(delta=True).getOrCreate()
            session.range(1).write.format("delta").mode("overwrite").save(tempfile.mkdtemp())
            has_delta = True
        except Exception:  # noqa: BLE001 - jars unavailable, no Maven access, etc.
            if session is not None:
                session.stop()
            session = None
    if session is None:
        session = _builder(delta=False).getOrCreate()
    session.conf.set("mssql_cdc.test.delta", str(has_delta).lower())
    from mssql_cdc import register

    register(session)
    yield session
    session.stop()


@pytest.fixture
def delta_spark(spark):
    if spark.conf.get("mssql_cdc.test.delta") != "true":
        pytest.skip("Delta Lake jars unavailable")
    return spark


@pytest.fixture
def workdir():
    path = tempfile.mkdtemp(prefix="mssql-cdc-")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def latest():
    """``latest(df, key, value, facts=None)``: sorted (key, value) of the latest image per key
    in a bronze DataFrame, as a MERGE downstream applies it, rebuilt from the newest snapshot
    on: a re-snapshot leaves no delete row for the gap (ADR 0016). With ``facts``, a newer
    snapshot event counts too: an empty table's snapshot has no rows (ADR 0018)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def rebuild(df, key, value, facts=None):
        points = [df.where("_operation = 0").agg(F.max("_start_lsn")).first()[0]]
        if facts is not None:
            points.append(facts.where("event IS NOT NULL").agg(F.max("max_lsn")).first()[0])
        since = max(p for p in points if p)
        last = Window.partitionBy(key).orderBy(
            F.col("_start_lsn").desc(), F.col("_command_id").desc_nulls_last(),
            F.col("_seqval").desc_nulls_last(), F.col("_operation").desc())
        rows = (df.where((F.col("_start_lsn") >= since) & (F.col("_operation") != 3))
                .withColumn("n", F.row_number().over(last)).where("n = 1 AND _operation != 1")
                .collect())
        return sorted((r[key], r[value]) for r in rows)

    return rebuild
