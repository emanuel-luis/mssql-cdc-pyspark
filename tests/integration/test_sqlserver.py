"""The source against a real SQL Server 2022 with CDC (see conftest.py)."""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from mssql_cdc.client import make_client
from mssql_cdc.finalization import end_offset_from_progress


pytestmark = pytest.mark.sqlserver


def _read(spark, server, ci, checkpoint=None, **options):
    """One Trigger.AvailableNow run; returns (rows as DataFrame, query)."""
    name = "it_" + uuid.uuid4().hex[:8]
    writer = (
        spark.readStream.format("mssql_cdc")
        .option("connectionString", server.connection_string)
        .option("captureInstance", ci)
        .options(**options).load()
        .writeStream.trigger(availableNow=True)
    )
    if checkpoint:  # file sink next to the checkpoint: its metadata must live as long
        out = os.path.join(checkpoint, "out")
        q = writer.format("parquet").option("path", out).option(
            "checkpointLocation", os.path.join(checkpoint, "ckpt")).start()
        q.awaitTermination()
        return (spark.read.parquet(out) if os.path.exists(out) else None), q
    q = writer.format("memory").queryName(name).start()
    q.awaitTermination()
    return spark.table(name), q


def test_time_zone_is_detected_by_name(sqlserver):
    client = make_client({"connectionString": sqlserver.connection_string})
    try:
        assert client.timezone == sqlserver.timezone_name
    finally:
        client.close()


def test_commit_times_are_utc_on_a_non_utc_server(spark, sqlserver):
    ci = sqlserver.cdc_table("tz_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.tz_probe VALUES (1)")
    utc_now = sqlserver.run("SELECT SYSUTCDATETIME()")[0][0]
    sqlserver.wait_for_changes(ci, 1)

    df, q = _read(spark, sqlserver, ci, includeCommandId="false")
    commit_ts = df.first()["_commit_ts"]  # TIMESTAMP_NTZ: collect() does no zone shift
    assert abs(commit_ts - utc_now) < timedelta(minutes=1)
    offset_ts = datetime.fromisoformat(end_offset_from_progress(q.lastProgress)["commit_ts"])
    assert abs(offset_ts - utc_now) < timedelta(minutes=1)


TYPES_DDL = """
  id int NOT NULL PRIMARY KEY,
  c_bit bit, c_tiny tinyint, c_small smallint, c_big bigint,
  c_real real, c_float float, c_dec decimal(18,2), c_num numeric(38,10),
  c_money money, c_smallmoney smallmoney,
  c_date date, c_dt datetime, c_dt2 datetime2(7), c_sdt smalldatetime,
  c_dto datetimeoffset(7), c_time time(7),
  c_char char(3), c_vc varchar(20), c_nchar nchar(3), c_nvc nvarchar(20),
  c_vcmax varchar(max), c_text text, c_ntext ntext, c_xml xml,
  c_guid uniqueidentifier, c_bin binary(4), c_vbin varbinary(20), c_image image,
  c_rv rowversion, c_alias sysname"""

EXPECTED_TYPES = [
    ("id", "int"), ("c_bit", "boolean"), ("c_tiny", "smallint"), ("c_small", "smallint"),
    ("c_big", "bigint"), ("c_real", "float"), ("c_float", "double"),
    ("c_dec", "decimal(18,2)"), ("c_num", "decimal(38,10)"), ("c_money", "decimal(19,4)"),
    ("c_smallmoney", "decimal(10,4)"), ("c_date", "date"), ("c_dt", "timestamp_ntz"),
    ("c_dt2", "timestamp_ntz"), ("c_sdt", "timestamp_ntz"), ("c_dto", "timestamp"),
    ("c_time", "string"), ("c_char", "string"), ("c_vc", "string"), ("c_nchar", "string"),
    ("c_nvc", "string"), ("c_vcmax", "string"), ("c_text", "string"), ("c_ntext", "string"),
    ("c_xml", "string"), ("c_guid", "string"), ("c_bin", "binary"), ("c_vbin", "binary"),
    ("c_image", "binary"), ("c_rv", "binary"), ("c_alias", "string"),
]


def test_inferred_columns_round_trip_every_mapped_type(spark, sqlserver):
    ci = sqlserver.cdc_table("types_probe", TYPES_DDL)
    sqlserver.run("""
        INSERT dbo.types_probe (id, c_bit, c_tiny, c_small, c_big, c_real, c_float, c_dec,
          c_num, c_money, c_smallmoney, c_date, c_dt, c_dt2, c_sdt, c_dto, c_time, c_char,
          c_vc, c_nchar, c_nvc, c_vcmax, c_text, c_ntext, c_xml, c_guid, c_bin, c_vbin,
          c_image, c_alias)
        VALUES (1, 1, 200, -3, 9000000000, 1.5, 2.25, 12.34, 1.0000000001, 12.3456, 1.2345,
          '2026-09-28', '2026-09-28T13:50:01.123', '2026-09-28T13:50:01.1234567',
          '2026-09-28T13:50:00', '2026-09-28T13:50:01.1234567-03:00', '13:50:01.1234567',
          'abc', 'hello', N'xyz', N'olá', REPLICATE('a', 10), 'txt', N'ntxt', '<a>1</a>',
          '6F9619FF-8B86-D011-B42D-00C04FC964FF', 0x01020304, 0x0A0B, 0x0C, N'alias')""")
    sqlserver.wait_for_changes(ci, 1)

    df, _ = _read(spark, sqlserver, ci, includeCommandId="false")  # no "columns" option
    assert [(n, t) for n, t in df.dtypes if not n.startswith("_")] == EXPECTED_TYPES

    row = df.first().asDict()
    assert {k: row[k] for k in ("id", "c_bit", "c_tiny", "c_small", "c_big", "c_real", "c_float")} == {
        "id": 1, "c_bit": True, "c_tiny": 200, "c_small": -3, "c_big": 9000000000,
        "c_real": 1.5, "c_float": 2.25}
    assert (row["c_dec"], row["c_num"], row["c_money"], row["c_smallmoney"]) == (
        Decimal("12.34"), Decimal("1.0000000001"), Decimal("12.3456"), Decimal("1.2345"))
    assert row["c_date"] == date(2026, 9, 28)
    assert row["c_dt"] == datetime(2026, 9, 28, 13, 50, 1, 123000)
    assert row["c_dt2"] == datetime(2026, 9, 28, 13, 50, 1, 123456)  # Spark keeps microseconds
    assert row["c_sdt"] == datetime(2026, 9, 28, 13, 50)
    # TIMESTAMP is an instant; render it in the session zone (UTC), not the local one
    assert df.selectExpr("CAST(c_dto AS STRING)").first()[0] == "2026-09-28 16:50:01.123456"
    assert row["c_time"] == "13:50:01.123456700"
    assert (row["c_char"], row["c_vc"], row["c_nchar"], row["c_nvc"]) == ("abc", "hello", "xyz", "olá")
    assert (row["c_vcmax"], row["c_text"], row["c_ntext"], row["c_xml"]) == ("a" * 10, "txt", "ntxt", "<a>1</a>")
    assert row["c_guid"] == "6F9619FF-8B86-D011-B42D-00C04FC964FF"
    assert (bytes(row["c_bin"]), bytes(row["c_vbin"]), bytes(row["c_image"])) == (
        b"\x01\x02\x03\x04", b"\x0a\x0b", b"\x0c")
    assert len(row["c_rv"]) == 8 and row["c_alias"] == "alias"


def test_unsupported_type_fails_at_load_with_a_pointer_to_columns(spark, sqlserver):
    ci = sqlserver.cdc_table("geo_probe", "id INT NOT NULL PRIMARY KEY, g geography")
    with pytest.raises(Exception, match="geography.*'columns'"):
        _read(spark, sqlserver, ci, includeCommandId="false")


def test_stream_resumes_from_checkpoint_with_transactions_in_order(spark, sqlserver, workdir):
    ci = sqlserver.cdc_table("orders", "order_id INT NOT NULL PRIMARY KEY, status VARCHAR(20) NOT NULL")
    sqlserver.run("BEGIN TRAN; INSERT INTO dbo.orders VALUES (1, 'new'); "
                  "UPDATE dbo.orders SET status = 'paid' WHERE order_id = 1; "
                  "DELETE FROM dbo.orders WHERE order_id = 1; COMMIT")
    sqlserver.wait_for_changes(ci, 4)  # insert, update before/after, delete

    first, _ = _read(spark, sqlserver, ci, checkpoint=workdir, includeCommandId="false")
    rows = first.orderBy("_start_lsn", "_seqval", "_operation").collect()
    assert [r["_operation"] for r in rows] == [2, 3, 4, 1]
    assert len({r["_start_lsn"] for r in rows}) == 1  # one commit

    sqlserver.run("INSERT INTO dbo.orders VALUES (2, 'new')")
    sqlserver.wait_for_changes(ci, 5)
    after, _ = _read(spark, sqlserver, ci, checkpoint=workdir, includeCommandId="false")
    after_rows = after.collect()  # same sink: first-run rows must not repeat
    new = [r for r in after_rows if r not in rows]
    assert len(after_rows) == 5 and [(r["order_id"], r["_operation"]) for r in new] == [(2, 2)]


@pytest.mark.xfail(strict=True, reason=(
    "fn_cdc_get_all_changes_* does not return __$command_id on SQL Server 2022 (lab t3), "
    "so the default includeCommandId=true fails; flip when the read path gets it elsewhere"))
def test_default_options_read_command_id(spark, sqlserver):
    ci = sqlserver.cdc_table("cmd_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.cmd_probe VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    df, _ = _read(spark, sqlserver, ci)
    assert df.first()["_command_id"] is not None
