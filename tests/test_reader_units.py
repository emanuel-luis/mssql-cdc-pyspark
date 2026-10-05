"""The stream reader's own logic without a SparkSession (pyspark is imported, never started):
what crosses to the executors, read()'s cleanup, offsets and option checks."""

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
