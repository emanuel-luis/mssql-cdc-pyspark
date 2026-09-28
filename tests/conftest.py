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
    )
    if delta:
        from delta import configure_spark_with_delta_pip

        b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
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
