"""The source against a real SQL Server 2022 with CDC (see conftest.py)."""

from __future__ import annotations

import os
import re
import time
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

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

    df, q = _read(spark, sqlserver, ci)
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

    df, _ = _read(spark, sqlserver, ci)  # no "columns" option
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
        _read(spark, sqlserver, ci)


def test_stream_resumes_from_checkpoint_with_transactions_in_order(spark, sqlserver, workdir):
    ci = sqlserver.cdc_table("orders", "order_id INT NOT NULL PRIMARY KEY, status VARCHAR(20) NOT NULL")
    sqlserver.run("BEGIN TRAN; INSERT INTO dbo.orders VALUES (1, 'new'); "
                  "UPDATE dbo.orders SET status = 'paid' WHERE order_id = 1; "
                  "DELETE FROM dbo.orders WHERE order_id = 1; COMMIT")
    sqlserver.wait_for_changes(ci, 4)  # insert, update before/after, delete

    first, _ = _read(spark, sqlserver, ci, checkpoint=workdir)
    rows = first.orderBy("_start_lsn", "_command_id", "_seqval", "_operation").collect()
    assert [r["_operation"] for r in rows] == [2, 3, 4, 1]
    ids = [r["_command_id"] for r in rows]
    assert None not in ids and ids == sorted(ids)  # __$command_id orders the statements
    assert len({r["_start_lsn"] for r in rows}) == 1  # one commit

    sqlserver.run("INSERT INTO dbo.orders VALUES (2, 'new')")
    sqlserver.wait_for_changes(ci, 5)
    after, _ = _read(spark, sqlserver, ci, checkpoint=workdir)
    after_rows = after.collect()  # same sink: first-run rows must not repeat
    new = [r for r in after_rows if r not in rows]
    assert len(after_rows) == 5 and [(r["order_id"], r["_operation"]) for r in new] == [(2, 2)]


def test_least_privilege_login_needs_one_grant_on_the_change_table(spark, sqlserver):
    ci = sqlserver.cdc_table("priv_probe", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10)")
    sqlserver.run("INSERT INTO dbo.priv_probe VALUES (1, 'a')")
    sqlserver.wait_for_changes(ci, 1)
    # what the CDC query functions need: enough to plan and to infer the schema...
    conn = sqlserver.login("cdc_reader", "GRANT SELECT ON dbo.priv_probe TO cdc_reader")
    with pytest.raises(Exception, match=r"GRANT SELECT ON cdc\.\[dbo_priv_probe_CT\]"):
        _read(spark, sqlserver, ci, connectionString=conn)
    # ...plus SELECT on this one change table to read it
    sqlserver.run("GRANT SELECT ON cdc.dbo_priv_probe_CT TO cdc_reader")
    df, _ = _read(spark, sqlserver, ci, connectionString=conn)
    assert [(r["id"], r["v"]) for r in df.collect()] == [(1, "a")]


def test_purged_range_stops_the_stream(spark, sqlserver, workdir):
    ci = sqlserver.cdc_table("purge_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.purge_probe VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    _read(spark, sqlserver, ci, checkpoint=workdir)  # checkpoint now at the first commit
    sqlserver.run("INSERT INTO dbo.purge_probe VALUES (2)")
    sqlserver.run("INSERT INTO dbo.purge_probe VALUES (3)")
    sqlserver.wait_for_changes(ci, 3)
    # the cleanup job's work, done now: rows below the new low watermark are deleted
    sqlserver.run("DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); "
                  "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = ?, "
                  "@low_water_mark = @lw, @threshold = 5000", (ci,))
    with pytest.raises(Exception, match="re-snapshot is required"):
        _read(spark, sqlserver, ci, checkpoint=workdir)


def test_heartbeat_script_keeps_an_idle_stream_current(spark, sqlserver, workdir):
    ci = sqlserver.cdc_table("quiet", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.quiet VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    script = Path(__file__).resolve().parents[2] / "sql" / "heartbeat.sql"
    for batch in re.split(r"^\s*GO\s*$", script.read_text(), flags=re.MULTILINE):
        if batch.strip():
            sqlserver.run(batch)
    job = "cdc_heartbeat_" + sqlserver.run("SELECT DB_NAME()")[0][0]
    try:
        _, q1 = _read(spark, sqlserver, ci, checkpoint=workdir)
        time.sleep(25)  # dbo.quiet gets no writes; only the Agent job does
        _, q2 = _read(spark, sqlserver, ci, checkpoint=workdir)
        first, second = end_offset_from_progress(q1.lastProgress), end_offset_from_progress(q2.lastProgress)
        utc_now = sqlserver.run("SELECT SYSUTCDATETIME()")[0][0]
        assert second["lsn"] > first["lsn"]
        # idle, SQL Server alone moves max_lsn about every 5 minutes (lab t1)
        assert utc_now - datetime.fromisoformat(second["commit_ts"]) < timedelta(seconds=30)
    finally:
        sqlserver.run("EXEC msdb.dbo.sp_delete_job @job_name = ?", (job,))


def test_pre_2022_offset_fallback_matches_the_named_zone(sqlserver):
    from mssql_cdc.client import MssqlPythonBackend, SqlCdcClient

    class Pre2022(MssqlPythonBackend):  # SQL Server 2016-2019 have no CURRENT_TIMEZONE_ID()
        def scalar(self, sql, params=()):
            if "CURRENT_TIMEZONE_ID" in sql:
                raise RuntimeError("'CURRENT_TIMEZONE_ID' is not a recognized built-in function name.")
            return super().scalar(sql, params)

    named = make_client({"connectionString": sqlserver.connection_string})
    fallback = SqlCdcClient(Pre2022(sqlserver.connection_string))
    try:
        lsn = named.max_lsn()
        assert fallback.timezone == "UTC-03:00"  # America/Sao_Paulo has no daylight saving now
        assert fallback.lsn_to_time(lsn) == named.lsn_to_time(lsn)
    finally:
        named.close()
        fallback.close()


def test_round_trip_and_network_wait_on_a_real_server(sqlserver):
    client = make_client({"connectionString": sqlserver.connection_string})
    try:
        times = client.ping(3)
        assert len(times) == 3 and min(times) > 0
        assert isinstance(client.network_wait_ms(), int)  # own session: no VIEW SERVER STATE needed
    finally:
        client.close()


def test_split_points_balance_rows_across_uneven_commits(sqlserver):
    ci = sqlserver.cdc_table("skewed", "id INT NOT NULL PRIMARY KEY")
    for i in range(8):
        sqlserver.run("INSERT INTO dbo.skewed VALUES (?)", (i,))
    sqlserver.run("INSERT INTO dbo.skewed SELECT 100 + n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7)) v(n)")
    sqlserver.wait_for_changes(ci, 16)
    client = make_client({"connectionString": sqlserver.connection_string})
    try:
        lo, hi = client.min_lsn(ci), client.max_lsn()
        bounds = client.split_points(ci, lo, hi, 2)
        ranges, prev = [], lo
        for b in bounds:
            ranges.append((prev, b))
            prev = client.increment_lsn(b)
        sizes = [sqlserver.run(f"SELECT COUNT(*) FROM cdc.[{ci}_CT] WHERE __$start_lsn BETWEEN "
                               "CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)", r)[0][0] for r in ranges]
        assert sizes == [8, 8]
    finally:
        client.close()


def test_stream_facade_records_network_metrics_from_a_real_server(delta_spark, sqlserver, workdir):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("net_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.net_probe VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    q = stream(delta_spark, {"connectionString": sqlserver.connection_string, "captureInstance": ci}) \
        .to_delta(target, "net-v1", ckpt, facts, trigger={"availableNow": True})
    q.awaitTermination()
    [row] = delta_spark.read.format("delta").load(facts).collect()
    assert row["source_rtt_ms"] > 0 and row["read_mb"] > 0
    assert row["network_wait_ms"] is not None  # own session's ASYNC_NETWORK_IO, no extra grant
