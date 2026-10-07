"""apply_changes' decisions that need no Spark session: a chunk bound as a key value, the gate
on range deletes, and the keys and plans no range deletes by (ADR 0028)."""

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampNTZType,
    TimestampType,
)

from mssql_cdc.silver import _absent, _bound, _Chunk, _range_key

BIGINT = 2**63 - 1
INTEGRAL = [ByteType(), ShortType(), IntegerType(), LongType()]  # bound as BIGINT
STAMP = "0x" + format(110, "020X")


@pytest.mark.parametrize("lower", [True, False])
@pytest.mark.parametrize("key_type", [*INTEGRAL, DateType(), TimestampNTZType()], ids=repr)
def test_a_null_bound_is_an_open_side(key_type, lower):
    assert _bound(None, key_type, lower) is None


@pytest.mark.parametrize("lower", [True, False])
@pytest.mark.parametrize("key_type", INTEGRAL, ids=repr)
@pytest.mark.parametrize(
    ("v", "want"),
    [(3, 3), ("3", 3), (0, 0), (-(2**63), -(2**63)), (BIGINT, BIGINT), (BIGINT + 1, None)],
)
def test_an_integer_bound_is_itself_and_open_past_bigint(key_type, lower, v, want):
    # the plan's last bound is MAX + 1: past BIGINT, no key is above it
    got = _bound(v, key_type, lower)
    assert got == want and type(got) is type(want)


@pytest.mark.parametrize("lower", [True, False])
@pytest.mark.parametrize("v", ["2026-01-03", "2026-01-03 00:00:00", "2026-01-03T00:00:00.0000000"])
def test_a_date_bound_is_its_day(v, lower):
    got = _bound(v, DateType(), lower)
    assert got == date(2026, 1, 3) and type(got) is date


@pytest.mark.parametrize("sep", ["T", " "])
@pytest.mark.parametrize(
    ("fraction", "lower", "upper"),
    [
        ("", 0, 0),
        (".5", 500000, 500000),
        (".123456", 123456, 123456),  # Spark's own microseconds
        (".1234560", 123456, 123456),  # datetime2(7), zero below the microsecond
        # 100 ns below it: SQL Server puts that microsecond's keys on either side, so a lower
        # bound moves up past them, and an upper one, truncated, already excludes them
        (".1234567", 123457, 123456),
        (".9999999", 1_000_000, 999999),  # into the next second
    ],
)
def test_a_timestamp_bound_in_microseconds(sep, fraction, lower, upper):
    v, start = f"2026-01-01{sep}00:00:03{fraction}", datetime(2026, 1, 1, 0, 0, 3)
    assert _bound(v, TimestampNTZType(), True) == start + timedelta(microseconds=lower)
    assert _bound(v, TimestampNTZType(), False) == start + timedelta(microseconds=upper)


def test_a_lower_bound_past_the_last_microsecond_overflows():
    last = "9999-12-31 23:59:59.9999999"
    assert _bound(last, TimestampNTZType(), False) == datetime.max
    assert _bound("9999-12-31T23:59:59.9999990", TimestampNTZType(), True) == datetime.max
    with pytest.raises(OverflowError):
        _bound(last, TimestampNTZType(), True)


@pytest.mark.parametrize(
    ("keys", "cut", "by_range"),
    [
        (["order_id"], ["order_id"], True),
        (["order_id"], ["status"], False),  # cut on another column
        (["order_id", "status"], ["order_id", "status"], False),  # two columns
        (["order_id", "status"], ["order_id"], False),
        (["order_id"], None, False),  # an open row that names no keys
    ],
)
def test_ranges_delete_only_on_silvers_one_key_column_the_chunks_are_cut_on(keys, cut, by_range):
    assert _range_key(keys, cut) is by_range


def _stubs():
    return MagicMock(spec_set=SparkSession), MagicMock(spec_set=DataFrame)


@pytest.mark.parametrize(
    "key_type",
    [
        StringType(),  # Spark orders bytes, SQL Server its collation
        DecimalType(38, 0),
        DoubleType(),
        FloatType(),
        BooleanType(),
        BinaryType(),
        TimestampType(),
    ],
    ids=repr,
)
def test_no_range_deletes_on_a_key_of_another_type(key_type):
    spark, held = _stubs()
    chunks = {0: _Chunk(0, None, "c", STAMP), 1: _Chunk(0, "c", None, STAMP)}
    assert _absent(spark, "silver", "k", key_type, chunks, held) is None
    assert not spark.method_calls and not held.method_calls


def test_no_range_deletes_when_every_lower_bound_overflows():
    spark, held = _stubs()
    last = "9999-12-31 23:59:59.9999999"
    chunks = {0: _Chunk(0, last, None, STAMP), 1: _Chunk(1, last, None, STAMP)}
    assert _absent(spark, "silver", "k", TimestampNTZType(), chunks, held) is None
    assert not spark.method_calls and not held.method_calls
