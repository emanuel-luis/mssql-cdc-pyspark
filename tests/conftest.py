import os
import shutil
import tempfile
import traceback

import pytest

# CI sets MSSQL_CDC_TEST_DELTA=require: a Delta session that cannot be built, or a PySpark
# without admission control, ends the run instead of skipping the tests that need them.
REQUIRE = os.environ.get("MSSQL_CDC_TEST_DELTA") == "require"
# Whether a selected test takes delta_spark, set when collection ends: a run that selects none
# builds a plain session and skips the Delta warm-up (jar resolution and the probe write).
NEEDS_DELTA = False


def pytest_sessionstart(session):
    if REQUIRE:
        from mssql_cdc import source

        if not source.HAS_ADMISSION_CONTROL:
            pytest.exit("MSSQL_CDC_TEST_DELTA=require: this PySpark has no admission control")


@pytest.hookimpl(tryfirst=True)  # before -m deselects by marker
def pytest_collection_modifyitems(items):
    """Mark every test that takes the ``spark`` or ``delta_spark`` fixture (directly or through
    another fixture): ``-m "not spark and not sqlserver"`` is the loop that starts no JVM,
    ``-m "not delta and not sqlserver"`` the one without Delta."""
    for item in items:
        names = getattr(item, "fixturenames", ())
        if "spark" in names:
            item.add_marker("spark")
        if "delta_spark" in names:
            item.add_marker("delta")


def pytest_collection_finish(session):
    global NEEDS_DELTA
    NEEDS_DELTA = any("delta_spark" in getattr(i, "fixturenames", ()) for i in session.items)


def _builder(tmp_path_factory, delta: bool):
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.master("local[2]")
        .appName("mssql-cdc-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")  # no progress bars in the test log
        # static conf; pytest's base temp keeps the last few runs, not every one
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    if delta:
        from delta import configure_spark_with_delta_pip

        b = (
            b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.databricks.delta.snapshotPartitions", "1")  # not 50 tasks per read
            # tiny tables with many commits: no checkpoint file every 10 (3-6% of a Delta test's
            # time, measured); turning off AQE or whole-stage codegen measured no steady win
            .config("spark.databricks.delta.properties.defaults.checkpointInterval", "100")
        )
        b = configure_spark_with_delta_pip(b)
    return b


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One session for the whole run. Uses Delta when a selected test takes ``delta_spark`` and
    the jars resolve (set MSSQL_CDC_TEST_DELTA=0 to skip trying, =require to fail without it),
    otherwise plain Spark."""
    session, has_delta = None, False
    if NEEDS_DELTA and os.environ.get("MSSQL_CDC_TEST_DELTA", "1") != "0":
        try:
            session = _builder(tmp_path_factory, delta=True).getOrCreate()
            probe = str(tmp_path_factory.mktemp("delta-probe"))
            session.range(1).write.format("delta").mode("overwrite").save(probe)
            has_delta = True
        except Exception as e:  # noqa: BLE001 - jars unavailable, no Maven access, etc.
            if session is not None:
                session.stop()
            if REQUIRE:
                pytest.exit(
                    "MSSQL_CDC_TEST_DELTA=require, but no Delta session:\n"
                    + "".join(traceback.format_exception(e))
                )
            session = None
    if session is None:
        session = _builder(tmp_path_factory, delta=False).getOrCreate()
    session.conf.set("mssql_cdc.test.delta", str(has_delta).lower())
    from mssql_cdc import register

    register(session)
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _no_query_left(request):
    """Errors, by name, a test that leaves a streaming query running, and stops the query: no
    test inherits another's, so its asserts on ``spark.streams.active`` are its own. Only for
    tests that take ``spark``: the others never start the JVM."""
    if "spark" not in request.fixturenames:
        yield
        return
    spark = request.getfixturevalue("spark")
    yield
    left = spark.streams.active
    for q in left:
        q.stop()
    if left:
        names = [q.name or str(q.id) for q in left]
        pytest.fail(f"{request.node.nodeid} left streaming queries running: {names}")


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
def refuse_caching(monkeypatch):
    """``refuse_caching()``: from then on every cache API raises, as on Databricks serverless
    (ADR 0032); returns the list the refused calls' names go to."""
    from pyspark.sql.classic.dataframe import DataFrame

    def refuse() -> list:
        calls: list = []

        def refusing(name):
            def refused(self, *args, **kwargs):
                calls.append(name)
                raise RuntimeError(f"[NOT_SUPPORTED_WITH_SERVERLESS] {name} is not supported")

            return refused

        for name in ("persist", "cache", "unpersist", "localCheckpoint", "checkpoint"):
            monkeypatch.setattr(DataFrame, name, refusing(name))
        return calls

    return refuse


@pytest.fixture
def latest():
    """``latest(df, key, value, facts=None)``: sorted (key, value) of the latest image per key
    in a bronze DataFrame, as a MERGE downstream applies it, rebuilt from the newest snapshot
    on: a re-snapshot leaves no delete row for the gap (ADR 0016). With ``facts``, a newer
    snapshot event counts too: an empty table's snapshot has no rows (ADR 0018); the source's
    change events are no snapshots (ADR 0023).

    Computed in Python from the collected rows, in the order bronze's table comment documents,
    so the oracle shares no code with ``silver.apply_changes``' window."""

    def order(r):  # (_start_lsn, _command_id, _seqval, _operation), a NULL below any value
        cid, seqval = r["_command_id"], r["_seqval"]
        nulls_last = (cid is not None, cid or 0, seqval is not None, seqval or "")
        return (r["_start_lsn"], *nulls_last, r["_operation"])

    def rebuild(df, key, value, facts=None):
        meta = ("_start_lsn", "_command_id", "_seqval", "_operation")
        rows = df.select(key, value, *meta).collect()
        points = [r["_start_lsn"] for r in rows if r["_operation"] == 0]
        if facts is not None:
            snapshots = ("bootstrap", "resnapshot")
            events = facts.select("event", "max_lsn").collect()
            points += [f["max_lsn"] for f in events if f["event"] in snapshots]
        since = max(p for p in points if p)
        images: dict = {}
        for r in rows:
            if r["_start_lsn"] < since or r["_operation"] == 3:
                continue
            seen = images.get(r[key])
            if seen is None or order(r) > order(seen):
                images[r[key]] = r
        return sorted((k, r[value]) for k, r in images.items() if r["_operation"] != 1)

    return rebuild
