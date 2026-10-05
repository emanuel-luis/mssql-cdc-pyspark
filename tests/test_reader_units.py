"""The stream reader's own logic without a SparkSession (pyspark is imported, never started):
what crosses to the executors, read()'s cleanup, offsets and option checks."""

import re
import threading
from datetime import datetime, timedelta

import pytest

from mssql_cdc import HAS_ADMISSION_CONTROL
from mssql_cdc.client import Backend, SqlCdcClient
from mssql_cdc.fake import FakeCdcClient, FakeCdcDatabase

pytestmark = pytest.mark.skipif(not HAS_ADMISSION_CONTROL, reason="needs Spark 4.2+")

CI = "dbo_orders"
T0 = datetime(2026, 9, 28, 13, 50, 0)


def _reader(path="unused", **options):
    from pyspark.sql.types import IntegerType, StructField, StructType

    from mssql_cdc.source import MssqlCdcStreamReader

    opts = {
        "backend": "fake",
        "fakePath": path,
        "captureInstance": CI,
        "numPartitions": "1",
        **{k: str(v) for k, v in options.items()},
    }
    return MssqlCdcStreamReader(opts, StructType([StructField("order_id", IntegerType())]))


def _db(path, n_tx=3):
    db = FakeCdcDatabase(path, [CI])
    lsns = [
        db.commit(CI, [(2, {"order_id": i})], at=T0 + timedelta(minutes=i)) for i in range(n_tx)
    ]
    return db, lsns


class Server(Backend):
    """Records every query; a scalar is an LSN (min_lsn's), a query returns no rows."""

    def __init__(self):
        self.sql = []

    def batches(self, sql, params, batch_size):
        self.sql.append(sql)
        return iter(())

    def scalar(self, sql, params=()):
        self.sql.append(sql)
        return "0x00000000000000000001"


def _lsn(n: int) -> str:
    return f"0x{n:020X}"


class Live:
    """A client with a live connection (an unpicklable lock) whose read fails midway."""

    def __init__(self):
        self.connection, self.closed = threading.Lock(), False

    def set_clock(self, zone, offset_min):
        pass

    def iter_changes(self, *args):
        raise RuntimeError("connection reset")

    def close(self):
        self.closed = True


# -- options ------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("numPartitions", "four", "numPartitions must be 'auto' or a positive integer, not 'four'"),
        ("numPartitions", "0", "numPartitions must be 'auto' or a positive integer, not '0'"),
        ("arrowBatchSize", "0", "arrowBatchSize must be a positive integer, not '0'"),
        ("arrowBatchSize", "1e4", "arrowBatchSize must be a positive integer, not '1e4'"),
        (  # it used to mean unlimited, silently
            "maxCommitsPerBatch",
            "0",
            "maxCommitsPerBatch must be a positive integer, not '0'; omit it to read up to max_lsn",
        ),
        ("maxCommitsPerBatch", "-5", "maxCommitsPerBatch must be a positive integer, not '-5'"),
    ],
)
def test_a_count_option_must_be_a_positive_integer(option, value, message):
    with pytest.raises(ValueError, match=re.escape(message)):
        _reader(**{option: value})


def test_counts_read_as_integers():
    reader = _reader(numPartitions=" 3 ", arrowBatchSize="500", maxCommitsPerBatch="7")
    assert (reader.num_partitions, reader.batch_size, reader._max_commits) == (3, 500, 7)
    assert _reader(maxCommitsPerBatch="")._max_commits is None  # unlimited


@pytest.mark.parametrize(
    ("option", "attribute"),
    [("failOnDataLoss", "fail_on_data_loss"), ("includeCommandId", "include_command_id")],
)
def test_a_boolean_option_is_true_or_false_or_an_error(option, attribute):
    from mssql_cdc.source import MssqlCdcDataSource

    for value in ("true", "TRUE", "1", " yes ", "Y"):
        assert getattr(_reader(**{option: value}), attribute) is True
    for value in ("false", "False", "0", "no", "N"):
        assert getattr(_reader(**{option: value}), attribute) is False
    for value in ("ture", "on", "off", ""):  # each used to read as false: the guard off
        message = f"{option} must be true or false (or 1/0, yes/no, y/n), not {value!r}"
        with pytest.raises(ValueError, match=re.escape(message)):
            _reader(**{option: value})
    with pytest.raises(ValueError, match="includeCommandId must be true or false"):
        MssqlCdcDataSource(
            {"captureInstance": CI, "columns": "id INT", "includeCommandId": "on"}
        ).schema()


def test_connect_timeout_must_be_a_non_negative_integer():
    from mssql_cdc.client import make_client

    for value in ("-1", "soon"):
        message = f"connectTimeout must be a non-negative integer (seconds), not '{value}'"
        with pytest.raises(ValueError, match=re.escape(message)):
            make_client({"connectionString": "Server=x", "connectTimeout": value})


def test_ddl_names():
    from mssql_cdc.source import _ddl_names

    ddl = "a INT, `b c` DECIMAL(18, 2), `d``e` STRUCT<x: INT, y: INT>, f: STRING, g MAP<INT,INT>"
    assert _ddl_names(ddl) == ["a", "b c", "d`e", "f", "g"]


@pytest.mark.parametrize(
    ("columns", "name"),
    [
        ("id INT, _Seqval STRING", "_Seqval"),  # in any case
        ("`_batch_id` BIGINT, id INT", "_batch_id"),
        ("id INT, _chunk INT", "_chunk"),  # a snapshot would read it as the chunk number
        ("amount DECIMAL(18, 2), _commit_ts TIMESTAMP", "_commit_ts"),
    ],
)
def test_a_source_column_named_as_a_metadata_column_fails_at_load(columns, name):
    from mssql_cdc.source import MssqlCdcDataSource, MssqlCdcSnapshotDataSource

    for source in (MssqlCdcDataSource, MssqlCdcSnapshotDataSource):
        with pytest.raises(
            ValueError, match=rf"Source column\(s\) {name} take the name.*'columns'"
        ):
            source({"captureInstance": CI, "columns": columns}).schema()


def test_inferred_columns_are_checked_and_the_snapshot_still_adds_its_chunk(tmp_path):
    from mssql_cdc.source import MssqlCdcDataSource, MssqlCdcSnapshotDataSource

    FakeCdcDatabase(str(tmp_path), [CI], columns={CI: "id INT, _operation INT"})
    inferred = {"backend": "fake", "fakePath": str(tmp_path), "captureInstance": CI}
    with pytest.raises(ValueError, match="_operation take the name.*capture instance"):
        MssqlCdcDataSource(inferred).schema()
    chunked = {"captureInstance": CI, "columns": "id INT", "snapshotChunks": "[]"}
    assert MssqlCdcSnapshotDataSource(chunked).schema().endswith(", id INT, _chunk INT")


def test_metrics_path_must_be_a_path_every_node_writes_with_open():
    for uri in ("s3://bucket/metrics", "abfss://c@a.dfs.core.windows.net/m", "dbfs:/m"):
        with pytest.raises(ValueError, match="is a URI.*local or FUSE path"):
            _reader(metricsPath=uri)
    for path in ("/Volumes/cat/sch/vol/metrics", "C:/metrics", "metrics"):
        assert _reader(metricsPath=path).metrics_path == path


def test_a_metrics_file_that_cannot_be_written_is_logged(tmp_path, caplog):
    from mssql_cdc.source import _write_metrics

    taken = tmp_path / "taken"
    taken.write_text("a file, not a directory")
    with caplog.at_level("WARNING", logger="mssql_cdc.source"):
        _write_metrics(str(taken), "0x01-0x02", {"rows": 1})  # never raises
    [warning] = [r.getMessage() for r in caplog.records]
    assert f"metrics file 0x01-0x02.json in {taken}" in warning


def test_an_unknown_option_is_warned_about(caplog):
    with caplog.at_level("WARNING", logger="mssql_cdc.source"):
        _reader(maxCommitPerBatch="5", NUMPARTITIONS="2")  # a typo; known in any case
    [warning] = [r.getMessage() for r in caplog.records if "unknown option" in r.getMessage()]
    assert "maxCommitPerBatch" in warning and "NUMPARTITIONS" not in warning


# -- invariant 5: executors are stateless ---------------------------------------------------
def test_a_reader_reaches_the_executors_without_its_client():
    from pyspark import cloudpickle

    reader = _reader()
    reader._client = Live()
    with pytest.raises(TypeError):
        cloudpickle.dumps(reader._client)  # what Spark would fail on
    assert cloudpickle.loads(cloudpickle.dumps(reader))._client is None
    assert isinstance(reader._client, Live)  # the driver keeps its own


def test_a_failed_read_closes_its_client():
    from mssql_cdc.source import LsnRange

    reader, live = _reader(), Live()
    reader._client = live
    with pytest.raises(RuntimeError, match="connection reset"):
        list(reader.read(LsnRange(CI, _lsn(1), _lsn(2))))
    assert live.closed and reader._client is None


# -- offsets -----------------------------------------------------------------------------
def test_latest_on_a_database_capture_has_not_written_to_starts_before_the_instance(tmp_path):
    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxRows

    from mssql_cdc.lsn import ZERO_LSN

    db = FakeCdcDatabase(str(tmp_path), [CI])  # no cdc.lsn_time_mapping entry: max_lsn NULL
    fake = FakeCdcClient(str(tmp_path))
    assert fake.max_lsn() is None
    reader = _reader(str(tmp_path), startingLsn="latest")
    start = reader.initialOffset()
    # just before the instance's first LSN, as a snapshot is stamped; not the zero LSN
    assert start == {"lsn": fake.decrement_lsn(fake.min_lsn(CI)), "commit_ts": ""}
    assert start["lsn"] > ZERO_LSN
    assert reader.latestOffset(start, ReadAllAvailable()) == start
    assert reader.latestOffset(start, ReadMaxRows(5)) == start
    assert reader.reportLatestOffset()["lsn"] == ZERO_LSN  # what capture has reached: nothing
    reader.prepareForTriggerAvailableNow()
    assert reader.latestOffset(start, ReadAllAvailable()) == start
    reader._target = None  # back to a trigger that keeps running
    # capture's first commit: read, not a DataLossError from below min_lsn
    lsn = db.commit(CI, [(2, {"order_id": 1})], at=T0)
    end = reader.latestOffset(start, ReadAllAvailable())
    assert end["lsn"] == lsn
    [r] = reader.partitions(start, end)
    assert (r.from_lsn, r.to_lsn) == (lsn, lsn)


def test_latest_with_no_max_lsn_and_no_instance_start_asks_to_retry(monkeypatch):
    reader = _reader(startingLsn="latest")
    monkeypatch.setattr("mssql_cdc.source.snapshot_lsn", lambda client, source: _lsn(0))
    monkeypatch.setattr(reader, "_instances", lambda client: [None])
    with pytest.raises(ValueError, match="retry once the capture job has run"):
        reader.initialOffset()


class Counting:
    """The calls latestOffset and reportLatestOffset make, each a query on SQL Server."""

    def __init__(self, max_lsn):
        self.max, self.calls = max_lsn, []

    def max_lsn(self):
        self.calls.append("max_lsn")
        return self.max

    def nth_commit_after(self, lsn, n):  # fewer than n commits after lsn: None
        self.calls.append("nth_commit_after")

    def lsn_to_time(self, lsn):
        self.calls.append("lsn_to_time")
        return "2026-09-28T13:50:00.000"


def test_an_idle_stream_asks_sql_server_for_max_lsn_alone_each_poll():
    from pyspark.sql.streaming.datasource import ReadMaxRows

    reader, client = _reader(), Counting(_lsn(5))
    reader._client = client
    start = {"lsn": _lsn(5), "commit_ts": "2026-09-28T13:50:00.000"}
    for _ in range(3):  # Spark's polls, every 10 ms or so without a trigger
        assert reader.latestOffset(start, ReadMaxRows(10)) == start
        assert reader.reportLatestOffset()["lsn"] == _lsn(5)
    assert client.calls == ["max_lsn", "lsn_to_time", "max_lsn", "max_lsn"]
    client.calls.clear()
    client.max = _lsn(9)  # capture moved: planned up to it, and reported
    assert reader.latestOffset(start, ReadMaxRows(10))["lsn"] == _lsn(9)
    assert reader.reportLatestOffset()["lsn"] == _lsn(9)
    assert client.calls == ["max_lsn", "nth_commit_after", "lsn_to_time", "lsn_to_time"]


# -- driver-side retries (ADR 0029) ---------------------------------------------------------
def _broken(error):
    """A driver client whose every call to SQL Server raises ``error``."""

    class Broken(Counting):
        closed = False

        def max_lsn(self):
            raise error

        def close(self):
            self.closed = True

    return Broken(None)


def test_a_transient_error_on_the_driver_is_retried_on_a_new_connection(monkeypatch, caplog):
    from mssql_python.exceptions import OperationalError
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    from mssql_cdc import source

    broken = _broken(OperationalError("Communication link failure", "TCP Provider: reset"))
    fresh, slept = Counting(_lsn(9)), []
    monkeypatch.setattr("mssql_cdc.client.make_client", lambda options: fresh)
    monkeypatch.setattr(source.time, "sleep", slept.append)
    reader = _reader()
    reader._client = broken  # the connection a failover broke
    start = {"lsn": _lsn(5), "commit_ts": "2026-09-28T13:50:00.000"}
    with caplog.at_level("WARNING", logger="mssql_cdc.source"):
        assert reader.latestOffset(start, ReadAllAvailable())["lsn"] == _lsn(9)
    assert broken.closed and reader._client is fresh
    assert len(slept) == 1 and 1 <= slept[0] <= 2
    [warning] = [r.getMessage() for r in caplog.records]
    assert "latestOffset failed" in warning and "Communication link failure" in warning
    assert "retry 1 of 3 on a new connection" in warning


def test_the_driver_gives_up_after_three_retries_and_never_retries_a_decision(monkeypatch):
    from mssql_cdc import source
    from mssql_cdc.client import DataLossError

    slept: list[float] = []
    monkeypatch.setattr(source.time, "sleep", slept.append)

    def connections(error) -> list:  # every client make_client opens, each failing with error
        made: list = []
        monkeypatch.setattr(
            "mssql_cdc.client.make_client", lambda options: made.append(_broken(error)) or made[-1]
        )
        return made

    made = connections(RuntimeError("State: 08S01, Native error: 10054, Message: TCP reset"))
    with pytest.raises(RuntimeError, match="08S01"):
        _reader().reportLatestOffset()
    assert len(made) == 4 and all(c.closed for c in made[:3])  # the first and 3 retries
    assert len(slept) == 3 and 1 <= slept[0] <= 2 and 2 <= slept[1] <= 4 and 4 <= slept[2] <= 8
    for error in (DataLossError("gone"), ValueError("State: 08S01"), PermissionError("grant")):
        made = connections(error)
        with pytest.raises(type(error)):
            _reader().reportLatestOffset()
        assert len(made) == 1  # raised at once


def test_transient_errors_are_told_by_their_sqlstate():
    from mssql_python.exceptions import sqlstate_to_exception

    from mssql_cdc.client import SchemaChangedError
    from mssql_cdc.source import _transient

    # mssql-python: the exception it raises for each SQLSTATE, as installed
    for state in ("08001", "08002", "08003", "08004", "08007", "08S01", "HYT00", "HYT01", "40001"):
        assert _transient(sqlstate_to_exception(state, "from the server")), state
    for state in ("28000", "HY000", "42S02", "22003", "IM002"):  # a login refused, SQL errors
        error = sqlstate_to_exception(state, "from the server")
        assert error is None or not _transient(error), state
    # arrow-odbc prints the SQLSTATE in the message
    assert _transient(RuntimeError("ODBC emitted an error:\nState: HYT00, Native error: 0, ..."))
    assert _transient(RuntimeError("State: 40001, Native error: 1205, Message: deadlock victim"))
    assert not _transient(RuntimeError("State: 42S02, Native error: 208, Message: Invalid object"))
    assert not _transient(SchemaChangedError("State: 08S01"))


def test_the_fake_maps_only_an_entrys_own_lsn_to_its_time(tmp_path):
    db, lsns = _db(str(tmp_path), n_tx=2)
    client = FakeCdcClient(str(tmp_path))
    first = T0.isoformat(timespec="milliseconds")
    assert client.lsn_to_time(lsns[0]) == first
    # like sys.fn_cdc_map_lsn_to_time: NULL for an LSN that is no entry's (tests/integration)
    assert client.lsn_to_time(client.increment_lsn(lsns[0])) is None
    assert client.lsn_to_time(client.decrement_lsn(lsns[0])) is None
    # a DDL's LSN is no commit's: its time is the last commit's at or before it
    ddl = db.ddl(CI, None, "ALTER TABLE dbo.orders ADD CONSTRAINT d DEFAULT 0 FOR x")
    assert client.lsn_to_time(ddl) is None
    second = (T0 + timedelta(minutes=1)).isoformat(timespec="milliseconds")
    assert [d.commit_ts for d in client.ddl_history(CI, lsns[0], ddl)] == [second]


def test_a_skip_past_purged_changes_is_logged_on_the_driver_and_in_the_task(tmp_path, caplog):
    db, lsns = _db(str(tmp_path), n_tx=4)
    db.cleanup(CI, lsns[2])  # purged below commit 2
    reader = _reader(str(tmp_path), failOnDataLoss="false")
    with caplog.at_level("WARNING", logger="mssql_cdc.source"):
        [planned] = reader.partitions({"lsn": lsns[0], "commit_ts": ""}, {"lsn": lsns[-1]})
        assert planned.from_lsn == lsns[2]
        db.cleanup(CI, lsns[3])  # after planning, before the read
        list(reader.read(planned))
    skipped = [r.getMessage() for r in caplog.records if "failOnDataLoss=false" in r.getMessage()]
    at = (T0 + timedelta(minutes=2)).isoformat(timespec="milliseconds")
    assert len(skipped) == 2
    assert (
        f"{CI}: change data from 0x" in skipped[0]
        and f"{lsns[2]} (committed at {at} UTC)" in skipped[0]
    )
    assert f"from {lsns[2]} up to min_lsn {lsns[3]}" in skipped[1]


def test_the_fake_retries_a_replace_that_a_reader_holds_up(tmp_path, monkeypatch):
    import os

    from mssql_cdc import fake

    real, refused = os.replace, []

    def held(src, dst):  # Windows refuses to replace a file another process has open
        if len(refused) < 2:
            refused.append(dst)
            raise PermissionError(13, "The process cannot access the file", dst)
        real(src, dst)

    monkeypatch.setattr(fake.time, "sleep", lambda s: None)
    monkeypatch.setattr(fake.os, "replace", held)
    db, lsns = _db(str(tmp_path), n_tx=3)
    refused.clear()
    db.cleanup(CI, lsns[1])  # rewrites the mapping and the change rows the same way
    client = FakeCdcClient(str(tmp_path))
    assert len(refused) == 2 and client.min_lsn(CI) == lsns[1]
    assert [r["start_lsn"] for r in client._mapping()] == lsns[1:]

    def always(src, dst):
        raise PermissionError(13, "The process cannot access the file", dst)

    monkeypatch.setattr(fake.os, "replace", always)
    with pytest.raises(PermissionError):  # not forever
        db.cleanup(CI, lsns[2])


# -- planning ----------------------------------------------------------------------------
def test_split_starts_each_range_after_its_bound_without_a_query():
    from mssql_cdc.client import CaptureInstance

    class Points:
        def clock(self):
            return None, None

        def split_points(self, ci, lo, hi, n):  # a bound two tiles share, and one at hi
            return [(_lsn(5), _lsn(6)), (_lsn(5), _lsn(6)), (_lsn(9), _lsn(10))]

        def increment_lsn(self, lsn):
            raise AssertionError("a round trip per bound")

    ranges = _reader(numPartitions=3)._split(
        Points(), CaptureInstance(CI, None, [], []), _lsn(1), _lsn(9)
    )
    assert [(r.from_lsn, r.to_lsn) for r in ranges] == [(_lsn(1), _lsn(5)), (_lsn(6), _lsn(9))]


# -- the driver's clock in every task -------------------------------------------------
def test_planned_ranges_carry_the_drivers_clock(tmp_path, monkeypatch):
    zone = "E. South America Standard Time"
    monkeypatch.setattr(FakeCdcClient, "clock", lambda self: (zone, None))
    _, lsns = _db(str(tmp_path), n_tx=4)
    for n in (1, 2):  # one range, and ranges cut at split points
        reader = _reader(str(tmp_path), numPartitions=n)
        ranges = reader.partitions({"lsn": lsns[0], "commit_ts": ""}, {"lsn": lsns[-1]})
        assert len(ranges) == n and {(r.zone, r.offset_min) for r in ranges} == {(zone, None)}


def test_a_task_converts_commit_times_with_the_drivers_clock():
    from mssql_cdc.source import LsnRange

    reader, server = _reader(), Server()
    reader._client = SqlCdcClient(server)  # sourceTimeZone=auto: would detect
    part = LsnRange(CI, "0x0000002A000001000001", "0x0000002A000001000009", None, None, -180)
    assert list(reader.read(part)) == []
    assert not any("SERVERPROPERTY" in s or "CURRENT_TIMEZONE_ID" in s for s in server.sql)
    assert any(
        "CAST(DATEADD(minute, 180, m.tran_end_time) AS datetime2(3))" in s for s in server.sql
    )
    assert reader._client is None  # closed: executors are stateless
