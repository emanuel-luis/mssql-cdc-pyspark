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


def test_connect_timeout_must_be_a_non_negative_integer():
    from mssql_cdc.client import make_client

    for value in ("-1", "soon"):
        message = f"connectTimeout must be a non-negative integer (seconds), not '{value}'"
        with pytest.raises(ValueError, match=re.escape(message)):
            make_client({"connectionString": "Server=x", "connectTimeout": value})


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
def test_a_database_capture_has_not_written_to_yet_starts_at_zero(tmp_path):
    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxRows

    from mssql_cdc.lsn import ZERO_LSN

    FakeCdcDatabase(str(tmp_path), [CI])  # no entry in cdc.lsn_time_mapping: max_lsn is NULL
    assert FakeCdcClient(str(tmp_path)).max_lsn() is None
    reader = _reader(str(tmp_path), startingLsn="latest")
    start = reader.initialOffset()
    assert start == {"lsn": ZERO_LSN, "commit_ts": ""}
    assert reader.latestOffset(start, ReadAllAvailable()) == start
    assert reader.latestOffset(start, ReadMaxRows(5)) == start
    assert reader.reportLatestOffset() == start
    reader.prepareForTriggerAvailableNow()
    assert reader.latestOffset(start, ReadAllAvailable()) == start


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
