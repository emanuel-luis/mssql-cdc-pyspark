import importlib.metadata
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
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
    ``-m "not delta and not sqlserver"`` the one without Delta. A test that takes
    ``connect_server`` (or ``connect_spark``) is marked ``connect``, and ``spark`` and
    ``delta`` too, so neither loop starts a Connect server."""
    for item in items:
        names = getattr(item, "fixturenames", ())
        if "spark" in names:
            item.add_marker("spark")
        if "delta_spark" in names:
            item.add_marker("delta")
        if "connect_server" in names:
            for marker in ("connect", "spark", "delta"):
                item.add_marker(marker)


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
def spark(tmp_path_factory, request):
    """One session for the whole run. Uses Delta when a selected test takes ``delta_spark`` and
    the jars resolve (set MSSQL_CDC_TEST_DELTA=0 to skip trying, =require to fail without it),
    otherwise plain Spark. With MSSQL_CDC_TEST_SPARK=connect, ``connect_spark`` instead: the
    tests that drive the library through its API run as a Spark Connect client (ADR 0032)."""
    from mssql_cdc import register

    if os.environ.get("MSSQL_CDC_TEST_SPARK") == "connect":
        session = request.getfixturevalue("connect_spark")
        session.conf.set("mssql_cdc.test.delta", "true")
        register(session)
        yield session
        return
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


@pytest.fixture(scope="session")
def connect_server(tmp_path_factory):
    """The URL (``sc://localhost:<port>``) of a local Spark Connect server with Delta Connect,
    which this fixture starts in its own JVM and stops after the run (tests marked
    ``connect``). Its Python workers run this interpreter, so they import this ``mssql_cdc``.
    The first run downloads the Delta Connect jars."""
    import pyspark

    home = os.path.dirname(pyspark.__file__)
    with socket.socket() as s:
        s.bind(("localhost", 0))
        port = s.getsockname()[1]
    base = tmp_path_factory.mktemp("connect")
    spark_version = ".".join(pyspark.__version__.split(".")[:2])
    delta = importlib.metadata.version("delta-spark")
    packages = [
        f"io.delta:delta-connect-server_{spark_version}_2.13:{delta}",
        # Delta Connect's classes need a newer protobuf runtime than the one its POM declares
        "com.google.protobuf:protobuf-java:3.25.1",
    ]
    conf = {
        "spark.connect.grpc.binding.port": port,
        "spark.connect.extensions.relation.classes": (
            "org.apache.spark.sql.connect.delta.DeltaRelationPlugin"
        ),
        "spark.connect.extensions.command.classes": (
            "org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
        ),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": 2,
        "spark.sql.warehouse.dir": base / "warehouse",
        "spark.databricks.delta.snapshotPartitions": 1,
        "spark.ui.enabled": "false",
    }
    submit = os.path.join(home, "bin", "spark-submit.cmd" if os.name == "nt" else "spark-submit")
    cmd = [submit, "--master", "local[2]", "--packages", ",".join(packages)]
    cmd += ["--class", "org.apache.spark.sql.connect.service.SparkConnectServer"]
    for key, value in conf.items():
        cmd += ["--conf", f"{key}={value}"]
    log_path = base / "server.log"
    with open(log_path, "wb") as log:
        env = {**os.environ, "SPARK_HOME": home, "PYSPARK_PYTHON": sys.executable}
        server = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
    try:
        deadline = time.monotonic() + 600  # the jars' first download included
        while True:
            if server.poll() is not None or time.monotonic() > deadline:
                tail = log_path.read_text(errors="replace")[-4000:]
                pytest.fail(f"no Spark Connect server on port {port}:\n{tail}")
            try:
                socket.create_connection(("localhost", port), timeout=1).close()
                break
            except OSError:
                time.sleep(1)
        yield f"sc://localhost:{port}"
    finally:
        server.terminate()
        try:
            server.wait(60)
        except subprocess.TimeoutExpired:
            server.kill()


@pytest.fixture(scope="session")
def connect_spark(connect_server):
    """A Spark Connect client session on ``connect_server``: no JVM in this process."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(connect_server).getOrCreate()
    yield spark
    for q in spark.streams.active:
        q.stop()
    spark.stop()  # before the server stops: a client left open retries it at exit
