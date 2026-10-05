"""The source against a real SQL Server 2022 with CDC (see conftest.py)."""

from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
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
        .options(**options)
        .load()
        .writeStream.trigger(availableNow=True)
    )
    if checkpoint:  # file sink next to the checkpoint: its metadata must live as long
        out = os.path.join(checkpoint, "out")
        q = (
            writer.format("parquet")
            .option("path", out)
            .option("checkpointLocation", os.path.join(checkpoint, "ckpt"))
            .start()
        )
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


def test_commit_times_are_utc_on_a_non_utc_server(spark, sqlserver, backend):
    ci = sqlserver.cdc_table("tz_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.tz_probe VALUES (1)")
    utc_now = sqlserver.run("SELECT SYSUTCDATETIME()")[0][0]
    sqlserver.wait_for_changes(ci, 1)

    df, q = _read(spark, sqlserver, ci, backend=backend)
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
    ("id", "int"),
    ("c_bit", "boolean"),
    ("c_tiny", "smallint"),
    ("c_small", "smallint"),
    ("c_big", "bigint"),
    ("c_real", "float"),
    ("c_float", "double"),
    ("c_dec", "decimal(18,2)"),
    ("c_num", "decimal(38,10)"),
    ("c_money", "decimal(19,4)"),
    ("c_smallmoney", "decimal(10,4)"),
    ("c_date", "date"),
    ("c_dt", "timestamp_ntz"),
    ("c_dt2", "timestamp_ntz"),
    ("c_sdt", "timestamp_ntz"),
    ("c_dto", "timestamp"),
    ("c_time", "string"),
    ("c_char", "string"),
    ("c_vc", "string"),
    ("c_nchar", "string"),
    ("c_nvc", "string"),
    ("c_vcmax", "string"),
    ("c_text", "string"),
    ("c_ntext", "string"),
    ("c_xml", "string"),
    ("c_guid", "string"),
    ("c_bin", "binary"),
    ("c_vbin", "binary"),
    ("c_image", "binary"),
    ("c_rv", "binary"),
    ("c_alias", "string"),
]


def test_inferred_columns_round_trip_every_mapped_type(spark, sqlserver, backend):
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

    df, _ = _read(spark, sqlserver, ci, backend=backend)  # no "columns" option
    assert [(n, t) for n, t in df.dtypes if not n.startswith("_")] == EXPECTED_TYPES

    row = df.first().asDict()
    assert {
        k: row[k] for k in ("id", "c_bit", "c_tiny", "c_small", "c_big", "c_real", "c_float")
    } == {
        "id": 1,
        "c_bit": True,
        "c_tiny": 200,
        "c_small": -3,
        "c_big": 9000000000,
        "c_real": 1.5,
        "c_float": 2.25,
    }
    assert (row["c_dec"], row["c_num"], row["c_money"], row["c_smallmoney"]) == (
        Decimal("12.34"),
        Decimal("1.0000000001"),
        Decimal("12.3456"),
        Decimal("1.2345"),
    )
    assert row["c_date"] == date(2026, 9, 28)
    assert row["c_dt"] == datetime(2026, 9, 28, 13, 50, 1, 123000)
    assert row["c_dt2"] == datetime(2026, 9, 28, 13, 50, 1, 123456)  # Spark keeps microseconds
    assert row["c_sdt"] == datetime(2026, 9, 28, 13, 50)
    # TIMESTAMP is an instant; render it in the session zone (UTC), not the local one
    assert df.selectExpr("CAST(c_dto AS STRING)").first()[0] == "2026-09-28 16:50:01.123456"
    assert row["c_time"] == "13:50:01.123456700"
    assert (row["c_char"], row["c_vc"], row["c_nchar"], row["c_nvc"]) == (
        "abc",
        "hello",
        "xyz",
        "olá",
    )
    assert (row["c_vcmax"], row["c_text"], row["c_ntext"], row["c_xml"]) == (
        "a" * 10,
        "txt",
        "ntxt",
        "<a>1</a>",
    )
    assert row["c_guid"] == "6F9619FF-8B86-D011-B42D-00C04FC964FF"
    assert (bytes(row["c_bin"]), bytes(row["c_vbin"]), bytes(row["c_image"])) == (
        b"\x01\x02\x03\x04",
        b"\x0a\x0b",
        b"\x0c",
    )
    assert len(row["c_rv"]) == 8 and row["c_alias"] == "alias"


def test_unsupported_type_fails_at_load_with_a_pointer_to_columns(spark, sqlserver):
    ci = sqlserver.cdc_table("geo_probe", "id INT NOT NULL PRIMARY KEY, g geography")
    with pytest.raises(Exception, match="geography.*'columns'"):
        _read(spark, sqlserver, ci)


def test_stream_resumes_from_checkpoint_with_transactions_in_order(
    spark, sqlserver, workdir, backend
):
    ci = sqlserver.cdc_table(
        "orders", "order_id INT NOT NULL PRIMARY KEY, status VARCHAR(20) NOT NULL"
    )
    sqlserver.run(
        "BEGIN TRAN; INSERT INTO dbo.orders VALUES (1, 'new'); "
        "UPDATE dbo.orders SET status = 'paid' WHERE order_id = 1; "
        "DELETE FROM dbo.orders WHERE order_id = 1; COMMIT"
    )
    sqlserver.wait_for_changes(ci, 4)  # insert, update before/after, delete

    first, _ = _read(spark, sqlserver, ci, checkpoint=workdir, arrowBatchSize="3", backend=backend)
    rows = first.orderBy("_start_lsn", "_command_id", "_seqval", "_operation").collect()
    assert [r["_operation"] for r in rows] == [2, 3, 4, 1]
    ids = [r["_command_id"] for r in rows]
    assert None not in ids and ids == sorted(ids)  # __$command_id orders the statements
    assert len({r["_start_lsn"] for r in rows}) == 1  # one commit

    sqlserver.run("INSERT INTO dbo.orders VALUES (2, 'new')")
    sqlserver.wait_for_changes(ci, 5)
    after, _ = _read(spark, sqlserver, ci, checkpoint=workdir, arrowBatchSize="3", backend=backend)
    after_rows = after.collect()  # same sink: first-run rows must not repeat
    new = [r for r in after_rows if r not in rows]
    assert len(after_rows) == 5 and [(r["order_id"], r["_operation"]) for r in new] == [(2, 2)]


def test_least_privilege_login_needs_one_grant_on_the_change_table(spark, sqlserver, backend):
    ci = sqlserver.cdc_table("priv_probe", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10)")
    sqlserver.run("INSERT INTO dbo.priv_probe VALUES (1, 'a')")
    sqlserver.wait_for_changes(ci, 1)
    # what the CDC query functions need: enough to plan and to infer the schema...
    conn = sqlserver.login("cdc_reader", "GRANT SELECT ON dbo.priv_probe TO cdc_reader")
    with pytest.raises(Exception, match=r"GRANT SELECT ON cdc\.\[dbo_priv_probe_CT\]"):
        _read(spark, sqlserver, ci, connectionString=conn, backend=backend)
    # ...plus SELECT on this one change table to read it
    sqlserver.run("GRANT SELECT ON cdc.dbo_priv_probe_CT TO cdc_reader")
    df, _ = _read(spark, sqlserver, ci, connectionString=conn, backend=backend)
    assert [(r["id"], r["v"]) for r in df.collect()] == [(1, "a")]
    client = make_client({"connectionString": conn, "backend": backend})
    try:  # own session's wait stats: no VIEW SERVER STATE for this login either
        assert isinstance(client.network_wait_ms(), int)
    finally:
        client.close()


def test_a_table_with_an_accented_name_streams_under_its_default_instance(
    spark, sqlserver, backend
):
    ci = sqlserver.cdc_table("Situação", "id INT NOT NULL PRIMARY KEY, nome NVARCHAR(20)")
    assert ci == "dbo_Situação"  # SQL Server's default name: <schema>_<table>
    sqlserver.run("INSERT INTO dbo.[Situação] VALUES (1, N'ativa')")
    sqlserver.wait_for_changes(ci, 1)
    df, _ = _read(spark, sqlserver, ci, backend=backend)
    assert [(r["id"], r["nome"], r["_capture_instance"]) for r in df.collect()] == [
        (1, "ativa", ci)
    ]


def test_purged_range_stops_the_stream(spark, sqlserver, workdir, backend):
    ci = sqlserver.cdc_table("purge_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.purge_probe VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    _read(spark, sqlserver, ci, checkpoint=workdir, backend=backend)  # at the first commit
    sqlserver.run("INSERT INTO dbo.purge_probe VALUES (2)")
    sqlserver.run("INSERT INTO dbo.purge_probe VALUES (3)")
    sqlserver.wait_for_changes(ci, 3)
    # the cleanup job's work, done now: rows below the new low watermark are deleted
    sqlserver.run(
        "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); "
        "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = ?, "
        "@low_water_mark = @lw, @threshold = 5000",
        (ci,),
    )
    with pytest.raises(Exception, match="re-snapshot is required"):
        _read(spark, sqlserver, ci, checkpoint=workdir, backend=backend)


def test_cleanup_deletes_the_time_mapping_below_every_instances_low_watermark(sqlserver):
    """Why seed() resolves a rerun before mapping its time (ADR 0025): once cleanup has passed
    a time, cdc.lsn_time_mapping no longer maps it. In a database of its own: the rows go only
    below the lowest low watermark of the database's instances."""
    import copy

    sqlserver.run("CREATE DATABASE cdc_it_cleanup")
    db = copy.copy(sqlserver)
    db._conn = sqlserver.connect("cdc_it_cleanup")
    db.run_enabling_cdc("EXEC sys.sp_cdc_enable_db")
    a = db.cdc_table("a", "id INT NOT NULL PRIMARY KEY")
    b = db.cdc_table("b", "id INT NOT NULL PRIMARY KEY")
    for i in range(3):
        db.run("INSERT INTO dbo.a VALUES (?)", (i,))
    db.wait_for_changes(a, 3)
    [(t,)] = db.run(
        "SELECT tran_end_time FROM cdc.lsn_time_mapping "
        f"WHERE start_lsn = (SELECT MAX(__$start_lsn) FROM cdc.[{a}_CT])"
    )
    db.run("INSERT INTO dbo.b VALUES (1)")
    db.wait_for_changes(b, 1)
    [(lw,)] = db.run("SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)")

    def below() -> int:
        return db.run(
            "SELECT COUNT(*) FROM cdc.lsn_time_mapping WHERE start_lsn < CONVERT(binary(10), ?, 1)",
            (lw,),
        )[0][0]

    def cleanup(ci: str) -> None:
        db.run(
            "DECLARE @lw binary(10) = CONVERT(binary(10), ?, 1); "
            "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = ?, "
            "@low_water_mark = @lw, @threshold = 5000",
            (lw, ci),
        )

    try:
        cleanup(a)
        assert below() > 0  # b's low watermark still holds them
        cleanup(b)
        assert below() == 0
        mapped = "SELECT sys.fn_cdc_map_time_to_lsn(N'largest less than or equal', ?)"
        assert db.run(mapped, (t,))[0][0] is None
    finally:
        db._conn.close()


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
        first, second = (
            end_offset_from_progress(q1.lastProgress),
            end_offset_from_progress(q2.lastProgress),
        )
        utc_now = sqlserver.run("SELECT SYSUTCDATETIME()")[0][0]
        assert second["lsn"] > first["lsn"]
        # idle, SQL Server alone moves max_lsn about every 5 minutes (lab t1)
        assert utc_now - datetime.fromisoformat(second["commit_ts"]) < timedelta(seconds=30)
    finally:
        sqlserver.run("EXEC msdb.dbo.sp_delete_job @job_name = ?", (job,))


def test_pre_2022_offset_fallback_matches_the_named_zone(sqlserver):
    from mssql_cdc.client import MssqlPythonBackend, SqlCdcClient

    class Pre2022(MssqlPythonBackend):  # SQL Server 2019 (15), which has no CURRENT_TIMEZONE_ID()
        def scalar(self, sql, params=()):
            assert "CURRENT_TIMEZONE_ID" not in sql  # it would not even compile there
            version = "SERVERPROPERTY('ProductMajorVersion')"
            return super().scalar(sql.replace(version, "15"), params)

    named = make_client({"connectionString": sqlserver.connection_string})
    fallback = SqlCdcClient(Pre2022(sqlserver.connection_string))
    try:
        lsn = named.max_lsn()
        assert fallback.timezone == "UTC-03:00"  # America/Sao_Paulo has no daylight saving now
        assert fallback.lsn_to_time(lsn) == named.lsn_to_time(lsn)
        assert named.clock() == (sqlserver.timezone_name, None)  # 2022: named, by its version
    finally:
        named.close()
        fallback.close()


def test_time_to_lsn_maps_a_fall_back_hour_to_a_time_no_later_commit_reads_before(sqlserver):
    from mssql_cdc.client import MssqlPythonBackend, SqlCdcClient

    client = SqlCdcClient(
        MssqlPythonBackend(sqlserver.connection_string), source_timezone="Eastern Standard Time"
    )
    at = client._server_clock("CONVERT(datetime2(0), ?, 126)")

    def clock(utc: str) -> str:
        sql = f"SELECT CONVERT(varchar(19), {at}, 126)"
        return client._b.scalar(sql, (utc,) * at.count("?"))

    try:  # 2026-11-01 06:00 UTC: 02:00 EDT becomes 01:00 EST, so 01:00-02:00 repeats
        # 01:45 EDT: commits up to 06:45 UTC read 01:00-01:45 EST, so an hour earlier
        assert clock("2026-11-01T05:45:00") == "2026-11-01T00:45:00"
        assert clock("2026-11-01T06:30:00") == "2026-11-01T01:30:00"  # the second 01:30
        assert clock("2026-03-08T06:30:00") == "2026-03-08T01:30:00"  # before spring forward
        assert clock("2026-07-01T12:00:00") == "2026-07-01T08:00:00"
    finally:
        client.close()


def test_round_trip_and_network_wait_on_a_real_server(sqlserver):
    client = make_client({"connectionString": sqlserver.connection_string})
    try:
        times = client.ping(3)
        assert len(times) == 3 and min(times) > 0
        assert isinstance(client.network_wait_ms(), int)  # own session: no VIEW SERVER STATE needed
    finally:
        client.close()


def test_split_points_balance_rows_across_uneven_commits(sqlserver, backend):
    ci = sqlserver.cdc_table("skewed", "id INT NOT NULL PRIMARY KEY")
    for i in range(8):
        sqlserver.run("INSERT INTO dbo.skewed VALUES (?)", (i,))
    sqlserver.run(
        "INSERT INTO dbo.skewed SELECT 100 + n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7)) v(n)"
    )
    sqlserver.wait_for_changes(ci, 16)
    client = make_client({"connectionString": sqlserver.connection_string, "backend": backend})
    try:
        lo, hi = client.min_lsn(ci), client.max_lsn()
        points = client.split_points(ci, lo, hi, 2)
        assert all(after == client.increment_lsn(b) for b, after in points)
        ranges, prev = [], lo
        for b, after in points:
            ranges.append((prev, b))
            prev = after
        sizes = [
            sqlserver.run(
                f"SELECT COUNT(*) FROM cdc.[{ci}_CT] WHERE __$start_lsn BETWEEN "
                "CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)",
                r,
            )[0][0]
            for r in ranges
        ]
        assert sizes == [8, 8]
    finally:
        client.close()


def test_stream_facade_records_network_metrics_from_a_real_server(delta_spark, sqlserver, workdir):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("net_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.net_probe VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    q = stream(
        delta_spark, {"connectionString": sqlserver.connection_string, "captureInstance": ci}
    ).to_delta(target, "net-v1", ckpt, facts, trigger={"availableNow": True})
    q.awaitTermination()
    [row] = delta_spark.read.format("delta").load(facts).collect()
    assert row["source_rtt_ms"] > 0 and row["read_mb"] > 0
    assert row["network_wait_ms"] is not None  # own session's ASYNC_NETWORK_IO, no extra grant
    # the end offset: at or after the batch's last change, and what headroom and lag start from
    assert row["end_lsn"] >= row["max_lsn"] and row["end_commit_ts"] >= row["max_commit_ts"]
    assert row["retention_watermark_ts"] <= row["end_commit_ts"] and row[
        "retention_headroom_hours"
    ] == round((row["end_commit_ts"] - row["retention_watermark_ts"]).total_seconds() / 3600, 2)
    # capture's newest commit (fn_cdc_get_max_lsn, no extra grant) is at or after the batch's
    assert row["source_max_commit_ts"] >= row["end_commit_ts"]
    assert row["ingestion_lag_seconds"] >= 0  # both ends from the server clock
    # Spark's clock minus the container's: allow a few seconds of skew (WSL2 VM drift)
    assert row["capture_lag_seconds"] is not None and row["capture_lag_seconds"] > -5


def test_bootstrap_snapshots_rows_older_than_cdc_with_a_least_privilege_login(
    delta_spark, sqlserver, workdir, latest, backend
):
    from mssql_cdc import stream

    # rows written before CDC was enabled exist only in the table: only a snapshot has them
    sqlserver.run("CREATE TABLE dbo.boot (id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL)")
    sqlserver.run("INSERT INTO dbo.boot SELECT n, 'old' FROM (VALUES (1),(2),(3),(4),(5),(6)) t(n)")
    sqlserver.run_enabling_cdc(
        "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'boot', "
        "@role_name = NULL, @supports_net_changes = 0"
    )
    ci = "dbo_boot"
    # No wait for capture: on a quiet database max_lsn stays below the new instance's first
    # LSN (and fn_cdc_get_min_lsn returns NULL) for up to ~5 minutes, and on a database capture
    # has never written to max_lsn is NULL too (run this test alone); the snapshot needs neither.
    conn = sqlserver.login(
        "boot_reader",
        "GRANT SELECT ON dbo.boot TO boot_reader",
        "GRANT SELECT ON cdc.dbo_boot_CT TO boot_reader",
    )
    options = {
        "connectionString": conn,
        "backend": backend,
        "captureInstance": ci,
        "numPartitions": "3",
        "arrowBatchSize": "1",
    }
    client = make_client(options)
    try:  # the documented API names the table and its key for a least-privilege login
        source = client.source_table(ci)
        assert source[:3] == ("dbo", "boot", ["id"]) and source.start_lsn.startswith("0x")
    finally:
        client.close()
    target, ckpt = os.path.join(workdir, "bronze"), os.path.join(workdir, "ckpt")

    def run(name=ci):
        q = stream(delta_spark, {**options, "captureInstance": name}).to_delta(
            target, "boot-v1", ckpt, trigger={"availableNow": True}, bootstrap=True
        )
        q.awaitTermination()
        return delta_spark.read.format("delta").load(target)

    first = run(ci.upper())  # the default collation matches the name ignoring case
    assert sorted((r["id"], r["v"], r["_operation"]) for r in first.collect()) == [
        (i, "old", 0) for i in range(1, 7)
    ]
    sqlserver.run("UPDATE dbo.boot SET v = 'new' WHERE id = 2")
    sqlserver.run("DELETE FROM dbo.boot WHERE id = 3")
    sqlserver.run("INSERT INTO dbo.boot VALUES (7, 'new')")
    sqlserver.wait_for_changes(ci, 4)
    second = run()
    assert second.where("_operation = 0").count() == 6 and second.count() == 10
    assert latest(second, "id", "v") == sorted(
        (r[0], r[1]) for r in sqlserver.run("SELECT id, v FROM dbo.boot")
    )


def test_seed_from_a_copy_maps_its_utc_start_on_the_server_clock_and_continues(
    delta_spark, sqlserver, workdir, latest
):
    from mssql_cdc import stream
    from mssql_cdc.lsn import normalize

    ci = sqlserver.cdc_table("seeded", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL")
    sqlserver.run("INSERT INTO dbo.seeded SELECT n, 'old' FROM (VALUES (1),(2),(3),(4)) t(n)")
    sqlserver.wait_for_changes(ci, 4)
    [(inserted,)] = sqlserver.run(
        f"SELECT CONVERT(varchar(22), MAX(__$start_lsn), 1) FROM cdc.{ci}_CT"
    )
    time.sleep(1.1)  # as_of counts to the second
    as_of = sqlserver.run("SELECT SYSUTCDATETIME()")[0][0]  # UTC; the server clock is UTC-3
    copy = delta_spark.createDataFrame(  # the copy, taken by SELECT before the changes below
        [tuple(r) for r in sqlserver.run("SELECT id, v FROM dbo.seeded")], "ID INT, V STRING"
    )
    sqlserver.run("UPDATE dbo.seeded SET v = 'new' WHERE id = 2")
    sqlserver.run("DELETE FROM dbo.seeded WHERE id = 3")
    sqlserver.run("INSERT INTO dbo.seeded VALUES (5, 'new')")
    sqlserver.wait_for_changes(ci, 8)
    s = stream(
        delta_spark, {"connectionString": sqlserver.connection_string, "captureInstance": ci}
    )
    target, ckpt = os.path.join(workdir, "bronze"), os.path.join(workdir, "ckpt")

    offset = s.seed(target, copy, as_of)
    # the last commit at or before as_of: the insert's or a later one of another table
    assert offset["lsn"] >= normalize(inserted)
    assert datetime.fromisoformat(offset["commit_ts"]) <= as_of
    q = s.to_delta(target, "seed-v1", ckpt, trigger={"availableNow": True}, bootstrap=True)
    q.awaitTermination()
    bronze = delta_spark.read.format("delta").load(target)
    assert bronze.where("_operation = 0").count() == 4  # the seed, and no snapshot after it
    assert latest(bronze, "id", "v") == sorted(
        (r[0], r[1]) for r in sqlserver.run("SELECT id, v FROM dbo.seeded")
    )


def test_resnapshot_recovers_a_stream_whose_changes_were_purged(
    delta_spark, sqlserver, workdir, latest
):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("resnap", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL")
    sqlserver.run("INSERT INTO dbo.resnap SELECT n, 'old' FROM (VALUES (1),(2),(3)) t(n)")
    sqlserver.wait_for_changes(ci, 3)
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))
    options = {"connectionString": sqlserver.connection_string, "captureInstance": ci}

    def run():
        q = stream(delta_spark, options).to_delta(
            target,
            "resnap-v1",
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            on_data_loss="resnapshot",
        )
        q.awaitTermination()
        return delta_spark.read.format("delta").load(target)

    run()  # the bootstrap snapshot
    sqlserver.run("INSERT INTO dbo.resnap VALUES (4, 'new')")
    sqlserver.wait_for_changes(ci, 4)
    run()  # the checkpoint is now at that commit
    sqlserver.run("UPDATE dbo.resnap SET v = 'new' WHERE id = 1")
    sqlserver.run("DELETE FROM dbo.resnap WHERE id = 2")
    sqlserver.wait_for_changes(ci, 7)
    sqlserver.run(
        "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); "
        "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = ?, "
        "@low_water_mark = @lw, @threshold = 5000",
        (ci,),
    )

    bronze = run()  # detects the purge, re-snapshots, starts generation 1
    assert bronze.where("_operation = 0").select("_start_lsn").distinct().count() == 2
    # id 2's delete was purged unread: only a rebuild from the newest snapshot drops it
    assert latest(bronze, "id", "v") == sorted(
        (r[0], r[1]) for r in sqlserver.run("SELECT id, v FROM dbo.resnap")
    )
    [event] = delta_spark.read.format("delta").load(facts).where("event = 'resnapshot'").collect()
    assert event["app_id"] == "resnap-v1.g1" and event["rows"] == 3
    assert event["lost_from_ts"] <= event["lost_to_ts"]


_N = "(VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) t(n)"
_CP1252 = "SQL_Latin1_General_CP1_CI_AS"  # the database default
SNAPSHOT_TILES = {  # table: (columns, rows, the keys' types for the CASTs)
    # a composite primary key led by a nvarchar, ending in a datetime2(7) whose bounds come
    # back truncated to microseconds (only the last key column may be)
    "snap_comp": (
        (
            "region NVARCHAR(10) NOT NULL, seq INT NOT NULL, at DATETIME2(7) NOT NULL, v INT, "
            "PRIMARY KEY (region, seq, at)"
        ),
        (
            "SELECT IIF(n < 5, N'n', N'ş'), n, DATEADD(second, n, "
            f"CAST('2026-09-28T10:00:00.1234567' AS datetime2(7))), n FROM {_N}"
        ),
        ["nvarchar(10)", "int", "datetime2(7)"],
    ),
    # one varchar key: an nvarchar parameter would convert the column
    "snap_code": (
        "code VARCHAR(12) NOT NULL PRIMARY KEY, v INT",
        f"SELECT CONCAT('k', n), n FROM {_N}",
        [f"varchar(12) COLLATE {_CP1252}"],
    ),
    # a varchar key in another code page than the database's: a bound CAST without the
    # column's collation turns every letter into '?' (partitions of 0, 0 and 10 rows). The
    # columns are named like the tiling query's helper columns once were
    "snap_greek": (
        "g VARCHAR(10) COLLATE Greek_CI_AS NOT NULL, p INT NOT NULL, v INT, PRIMARY KEY (g, p)",
        f"SELECT NCHAR(913 + n), n, n FROM {_N}",  # Α, Β, ... Κ
        ["varchar(10) COLLATE Greek_CI_AS", "int"],
    ),
    # 'n' and 'N' are one leading value in SQL and two in Python: tiles start at ('n', 4)
    # and ('N', 7), and the range between them must not read the whole value twice
    "snap_case": (
        "region VARCHAR(5) NOT NULL, id INT NOT NULL, v INT, PRIMARY KEY (region, id)",
        f"SELECT IIF(n % 2 = 0, 'n', 'N'), n, n FROM {_N}",
        [f"varchar(5) COLLATE {_CP1252}", "int"],
    ),
    # every other kind of bound, as text CAST back: equal leading values that only an exact
    # round trip matches (a datetime between ms, a zero decimal, binary, a GUID), the last
    # column a datetimeoffset range
    "snap_kinds": (
        (
            "d DATE NOT NULL, t DATETIME NOT NULL, m DECIMAL(18,10) NOT NULL, "
            "b VARBINARY(4) NOT NULL, f BIT NOT NULL, g UNIQUEIDENTIFIER NOT NULL, "
            "o DATETIMEOFFSET(3) NOT NULL, v INT, PRIMARY KEY (d, t, m, b, f, g, o)"
        ),
        (
            "SELECT '2026-09-28', '2026-09-28T10:00:00.007', 0, 0x0A0B, 1, "
            "'6F9619FF-8B86-D011-B42D-00C04FC964FF', DATEADD(second, n, "
            f"CAST('2026-09-28T10:00:00.123-03:00' AS datetimeoffset(3))), n FROM {_N}"
        ),
        [
            "date",
            "datetime",
            "decimal(18,10)",
            "varbinary(4)",
            "bit",
            "uniqueidentifier",
            "datetimeoffset(3)",
        ],
    ),
}


@pytest.mark.parametrize("name", SNAPSHOT_TILES)
def test_snapshot_tiles_composite_and_string_keys(spark, sqlserver, name, backend):
    ddl, rows, types = SNAPSHOT_TILES[name]
    ci = sqlserver.cdc_table(name, ddl)
    sqlserver.run(f"INSERT INTO dbo.{name} {rows}")
    client = make_client({"connectionString": sqlserver.connection_string, "backend": backend})
    try:  # lengths in characters, precisions as declared
        assert client.key_types(ci, client.source_table(ci).keys) == types
    finally:
        client.close()
    df = (
        spark.read.format("mssql_cdc_snapshot")
        .option("connectionString", sqlserver.connection_string)
        .option("captureInstance", ci)
        .option("backend", backend)
        .option("numPartitions", "3")
        .load()
    )
    assert df.rdd.glom().map(len).collect() == [4, 3, 3]  # NTILE(3) of 10 rows, each read once
    assert sorted(r["v"] for r in df.collect()) == sorted(
        r[0] for r in sqlserver.run(f"SELECT v FROM dbo.{name}")
    )


def _plan(sqlserver, sql, params=()):
    """Rows and actual plan (STATISTICS XML) of one query."""
    conn = sqlserver.connect()
    try:
        cur = conn.cursor()
        cur.execute("SET STATISTICS XML ON")
        cur.execute(sql, tuple(params))
        rows = len(cur.fetchall())
        cur.nextset()
        return rows, cur.fetchone()[0]
    finally:
        conn.close()


def _rows_read(plan):
    return sum(int(n) for n in re.findall(r'ActualRowsRead="(\d+)"', plan))


_ROWS = (  # n = 1..40000
    "(SELECT TOP 40000 ROW_NUMBER() OVER (ORDER BY (SELECT 1)) n "
    "FROM sys.all_columns a CROSS JOIN sys.all_columns b) t"
)


@pytest.mark.parametrize(
    "name, company, typ",
    [
        ("snap_one", "1", "INT"),  # (company, id) with one company: every range inside it
        ("snap_three", "n % 3", "INT"),  # ranges that cross from one company to the next
        ("snap_twenty", "CONCAT('r', n % 20)", "VARCHAR(10)"),
    ],
)
def test_each_snapshot_range_seeks_its_own_rows(sqlserver, name, company, typ):
    from mssql_cdc.client import _key_select

    ci = sqlserver.cdc_table(
        name, f"company {typ} NOT NULL, id INT NOT NULL, v INT, PRIMARY KEY (company, id)"
    )
    sqlserver.run(f"INSERT INTO dbo.{name} SELECT {company}, n, n FROM {_ROWS}")
    client = make_client({"connectionString": sqlserver.connection_string})
    try:
        keys = client.source_table(ci).keys
        types = client.key_types(ci, keys)
        bounds = [None, *client.key_tiles("dbo", name, keys, 8), None]
    finally:
        client.close()
    for lo, hi in pairwise(bounds):
        sql, params = _key_select(f"SELECT * FROM dbo.{name}", keys, types, lo, hi)
        rows, plan = _plan(sqlserver, sql, params)
        assert (rows, _rows_read(plan)) == (5000, 5000), (lo, hi)  # nothing read and dropped
        assert "CONVERT_IMPLICIT" not in plan  # varchar compared as varchar


def test_one_where_row_comparison_reads_the_whole_leading_value(sqlserver):
    """Why the ranges are UNION ALL pieces: in one WHERE, the row comparison seeks only on
    the leading column, and a range inside one leading value reads all of it."""
    sqlserver.run(
        "CREATE TABLE dbo.snap_where (company INT NOT NULL, id INT NOT NULL, code VARCHAR(10), "
        "PRIMARY KEY (company, id))"
    )
    sqlserver.run(
        f"INSERT INTO dbo.snap_where SELECT 1, n, CONCAT('r', n) FROM {_ROWS} WHERE n <= 10000"
    )
    i = "CAST(? AS int)"
    where = (  # (company, id) >= (1, 2501) AND (company, id) < (1, 5001), sargable bounds first
        f"[company] >= {i} AND ([company] > {i} OR ([company] = {i} AND [id] >= {i})) "
        f"AND [company] <= {i} AND ([company] < {i} OR ([company] = {i} AND [id] < {i}))"
    )
    params = [1, 1, 1, 2501, 1, 1, 1, 5001]
    rows, plan = _plan(sqlserver, f"SELECT * FROM dbo.snap_where WHERE {where}", params)
    assert rows == 2500 and _rows_read(plan) == 10000
    # a bare nvarchar parameter converts a varchar column instead of the other way round
    _, plan = _plan(sqlserver, "SELECT * FROM dbo.snap_where WHERE code >= ?", ["r1é"])
    assert "CONVERT_IMPLICIT(nvarchar(10)," in plan
    # why one integer key keeps MIN..MAX (two seeks): NTILE reads the whole key and spools it
    k = "[company], [id]"
    _, plan = _plan(sqlserver, f"SELECT {k}, NTILE(4) OVER (ORDER BY {k}) FROM dbo.snap_where")
    assert "Table Spool" in plan and _rows_read(plan) >= 10000


def test_key_buckets_floor_negative_keys_and_number_dates_from_1970(sqlserver):
    """reconcile()'s Tier 1: the server's buckets are the ones Spark computes with pmod."""
    sqlserver.run("CREATE TABLE dbo.rc_keys (id INT NOT NULL PRIMARY KEY, day DATE NOT NULL)")
    sqlserver.run(
        f"INSERT INTO dbo.rc_keys SELECT n - 26, DATEADD(day, n, '19691201') FROM {_ROWS} "
        "WHERE n <= 50"
    )
    client = make_client({"connectionString": sqlserver.connection_string})
    try:
        ints = client.key_buckets("dbo", "rc_keys", "id", "int", 10)
        days = client.key_buckets("dbo", "rc_keys", "day", "date", 7)
        whole = client.key_buckets("dbo", "rc_keys", None, None, 1)
    finally:
        client.close()

    def floored(values, width):
        out: dict = {}
        for v in values:
            n, s = out.get(v // width, (0, 0))
            out[v // width] = (n + 1, s + v)
        return [(b, n, Decimal(s)) for b, (n, s) in sorted(out.items())]

    assert sorted(ints) == floored(range(-25, 25), 10)  # -25..-21 in bucket -3, not -2
    first = (date(1969, 12, 2) - date(1970, 1, 1)).days
    assert sorted(days) == floored(range(first, first + 50), 7)
    assert whole == [(0, 50, None)]


def test_cdc_refuses_a_unique_index_over_nullable_columns(sqlserver):
    # so a snapshot's key columns are never NULL on SQL Server: the NULL handling of the key
    # ranges is defensive, and what the fake (whose keys can be NULL) does
    sqlserver.run("CREATE TABLE dbo.snap_nulls (a INT NULL, b VARCHAR(5) NULL)")
    sqlserver.run("CREATE UNIQUE INDEX ux_snap_nulls ON dbo.snap_nulls (a, b)")
    with pytest.raises(Exception, match="must be defined as NOT NULL"):
        sqlserver.run_enabling_cdc(
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'snap_nulls', "
            "@role_name = NULL, @index_name = N'ux_snap_nulls', @supports_net_changes = 0"
        )


def test_silver_reads_a_composite_key_and_converges_to_the_source_table(
    delta_spark, sqlserver, workdir
):
    from mssql_cdc import apply_changes, stream

    ci = sqlserver.cdc_table(
        "silver",
        "a INT NOT NULL, b VARCHAR(5) NOT NULL, v VARCHAR(10) NOT NULL, PRIMARY KEY (a, b)",
    )
    sqlserver.run("INSERT INTO dbo.silver VALUES (1, 'x', 'old'), (1, 'y', 'old'), (2, 'x', 'old')")
    sqlserver.wait_for_changes(ci, 3)
    options = {"connectionString": sqlserver.connection_string, "captureInstance": ci}
    bronze, silver, control, ckpt = (
        os.path.join(workdir, n) for n in ("bronze", "silver", "control", "ckpt")
    )

    def run():
        q = stream(delta_spark, options).to_delta(
            bronze, "silver-v1", ckpt, trigger={"availableNow": True}, bootstrap=True
        )
        q.awaitTermination()
        # no keys given: read from the capture instance's unique index, the primary key
        apply_changes(delta_spark, bronze, silver, ci, control_table=control, options=options)
        rows = delta_spark.read.format("delta").load(silver).collect()
        return sorted((r["a"], r["b"], r["v"]) for r in rows)

    def source():
        return sorted(tuple(r) for r in sqlserver.run("SELECT a, b, v FROM dbo.silver"))

    assert run() == source()
    sqlserver.run("UPDATE dbo.silver SET v = 'new' WHERE a = 1 AND b = 'y'")
    sqlserver.run("DELETE FROM dbo.silver WHERE a = 2")
    sqlserver.run("UPDATE dbo.silver SET b = 'z' WHERE a = 1 AND b = 'x'")  # the key changes
    sqlserver.wait_for_changes(ci, 8)
    after = run()
    changes = delta_spark.read.format("delta").load(bronze).where("_operation != 0").collect()
    ops = sorted((r["_operation"], r["b"]) for r in changes)
    assert after == source() == [(1, "y", "new"), (1, "z", "old")], ops
    # a primary-key update arrives as a delete of the old key and an insert of the new one,
    # which is why the before-images (operation 3) can be ignored
    assert {(1, "x"), (2, "z")} <= set(ops) and (3, "x") not in ops, ops


# -- schema changes and capture instance switches (ADR 0023) ---------------------------------
def _reader(sqlserver, table: str, *instances: str) -> str:
    """A least-privilege login for ``dbo.<table>``: SELECT on it and on each change table."""
    user = f"{table}_reader"
    grants = [f"GRANT SELECT ON dbo.{table} TO {user}"]
    grants += [f"GRANT SELECT ON cdc.[{ci}_CT] TO {user}" for ci in instances]
    return sqlserver.login(user, *grants)


def _writer(sqlserver, table: str, first_id: int):
    """Commit an insert about every 30 ms (and every third commit an update of an earlier row)
    on its own connection until the returned function is called; it returns the last id."""
    stop, state = threading.Event(), {"last": None, "error": None}

    def loop():
        conn = sqlserver.connect()
        cur = conn.cursor()
        i = first_id
        try:
            while not stop.is_set():
                try:
                    cur.execute(f"INSERT INTO dbo.{table} VALUES (?, 'new')", (i,))
                    if i % 3 == 0:
                        cur.execute(f"UPDATE dbo.{table} SET v = ? WHERE id = ?", (f"u{i}", i - 2))
                except Exception as exc:
                    if "deadlock victim" in str(exc):  # the enable won: commit it again
                        continue
                    raise
                state["last"], i = i, i + 1
                time.sleep(0.03)
        except Exception as exc:  # noqa: BLE001 - reported by finish()
            state["error"] = exc
        finally:
            conn.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def finish() -> int:
        stop.set()
        thread.join()
        assert state["error"] is None, state["error"]
        return state["last"]

    return finish


def _source(sqlserver, table: str, value: str = "v") -> list:
    return sorted(tuple(r) for r in sqlserver.run(f"SELECT id, {value} FROM dbo.{table}"))


def test_a_capture_instance_enabled_under_writes_takes_over_at_its_start_lsn(
    delta_spark, sqlserver, workdir, latest
):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("sw_live", "id INT NOT NULL PRIMARY KEY, v VARCHAR(20) NOT NULL")
    sqlserver.run(f"INSERT INTO dbo.sw_live SELECT n, 'old' FROM {_N}")
    options = {"connectionString": _reader(sqlserver, "sw_live", ci), "captureInstance": ci}
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run():
        q = stream(delta_spark, options).to_delta(
            target, "sw-live", ckpt, facts, trigger={"availableNow": True}, bootstrap=True
        )
        q.awaitTermination()

    finish = _writer(sqlserver, "sw_live", 100)
    try:
        run()  # the snapshot, then the old instance only
        v2 = sqlserver.enable_cdc("sw_live", "dbo_sw_live_v2")
        sqlserver.run(f"GRANT SELECT ON cdc.[{v2}_CT] TO sw_live_reader")
        sqlserver.wait_for_changes(v2, 20)  # capture is past the new start
        run()  # a batch across it, while the writer goes on
    finally:
        last = finish()
    sqlserver.wait_for(f"SELECT COUNT(*) FROM cdc.[{v2}_CT] WHERE id = ?", (last,))
    run()

    s = sqlserver.start_lsn(v2)
    bronze = delta_spark.read.format("delta").load(target)
    changes = bronze.where("_operation != 0")
    dups = changes.groupBy("_start_lsn", "_seqval", "_operation").count().where("count > 1")
    assert dups.count() == 0
    # below S from the old instance, from S on from the new one: every commit once
    sides = {(r[0] >= s, r[1]) for r in changes.select("_start_lsn", "_capture_instance").collect()}
    assert sides == {(False, ci), (True, v2)}
    facts_df = delta_spark.read.format("delta").load(facts)
    assert latest(bronze, "id", "v", facts_df) == _source(sqlserver, "sw_live")
    [switch] = facts_df.where("event = 'capture_instance_switched'").collect()
    assert (switch["detail"], switch["min_lsn"]) == (f"{ci} -> {v2}", s)


def test_an_older_instance_dropped_before_the_stream_reached_the_newer_start_is_data_loss(
    delta_spark, sqlserver, workdir, latest
):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("sw_gone", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL")
    sqlserver.run(f"INSERT INTO dbo.sw_gone SELECT n, 'old' FROM {_N}")
    sqlserver.wait_for_changes(ci, 10)
    options = {"connectionString": _reader(sqlserver, "sw_gone", ci), "captureInstance": ci}
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run(on_data_loss="fail"):
        q = stream(delta_spark, options).to_delta(
            target,
            "sw-gone",
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            on_data_loss=on_data_loss,
        )
        q.awaitTermination()
        return delta_spark.read.format("delta").load(target)

    run()
    sqlserver.run("DELETE FROM dbo.sw_gone WHERE id = 1")  # below the new start: only in v1
    sqlserver.wait_for_changes(ci, 11)
    v2 = sqlserver.enable_cdc("sw_gone", "dbo_sw_gone_v2")
    sqlserver.run(f"GRANT SELECT ON cdc.[{v2}_CT] TO sw_gone_reader")
    sqlserver.run("UPDATE dbo.sw_gone SET v = 'new' WHERE id = 2")
    sqlserver.wait_for_changes(v2, 2)
    sqlserver.run_enabling_cdc(
        "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'sw_gone', "
        "@capture_instance = ?",
        (ci,),
    )
    with pytest.raises(
        Exception, match=f"or held only by capture instance '{ci}', disabled before"
    ):
        run()
    bronze = run("resnapshot")  # the configured instance is gone: it follows v2
    facts_df = delta_spark.read.format("delta").load(facts)
    assert latest(bronze, "id", "v", facts_df) == _source(sqlserver, "sw_gone")
    [event] = facts_df.where("event = 'resnapshot'").collect()
    assert event["app_id"] == "sw-gone.g1" and event["rows"] == 9


def _first_batch(q, timeout: float = 120) -> None:
    """Wait until the running query ``q`` has finished a batch."""
    deadline = time.time() + timeout
    while q.lastProgress is None:
        if q.exception() or time.time() > deadline:
            raise AssertionError(f"no batch finished: {q.exception()}")
        time.sleep(0.5)


def test_a_type_change_stops_the_running_query_before_its_batch_is_written(
    delta_spark, sqlserver, workdir
):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("ddl_type", "id INT NOT NULL PRIMARY KEY, amount DECIMAL(9,2)")
    sqlserver.run("INSERT INTO dbo.ddl_type VALUES (1, 1.5)")
    sqlserver.wait_for_changes(ci, 1)
    options = {"connectionString": _reader(sqlserver, "ddl_type", ci), "captureInstance": ci}
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def start(**trigger):
        return stream(delta_spark, options).to_delta(
            target, "ddl-type", ckpt, facts, trigger=trigger
        )

    def bronze():
        return delta_spark.read.format("delta").load(target)

    q = start(processingTime="1 second")  # its schema has amount DECIMAL(9,2)
    try:
        _first_batch(q)
        # one capture scan then records the three together, so one batch holds them all
        sqlserver.capture_job(running=False)
        try:
            sqlserver.run("INSERT INTO dbo.ddl_type VALUES (2, 2.5)")
            sqlserver.run("ALTER TABLE dbo.ddl_type ALTER COLUMN amount DECIMAL(18,4)")
            sqlserver.run("INSERT INTO dbo.ddl_type VALUES (3, 123456789.1234)")  # no (9,2)
        finally:
            sqlserver.capture_job(running=True)
        with pytest.raises(Exception, match=r"amount DECIMAL\(18,4\) \(read as decimal\(9,2\)\)"):
            q.awaitTermination(120)
    finally:
        q.stop()
    assert [r["id"] for r in bronze().collect()] == [1]  # nothing of the batch with the DDL

    # the restart infers DECIMAL(18,4): Delta refuses it on bronze without type widening...
    q = start(availableNow=True)
    with pytest.raises(Exception, match="delta.enableTypeWidening"):
        q.awaitTermination()
    assert bronze().count() == 1
    # ...and widens the column with it
    delta_spark.sql(
        f"ALTER TABLE delta.`{target}` SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')"
    )
    start(availableNow=True).awaitTermination()
    assert dict(bronze().dtypes)["amount"] == "decimal(18,4)"
    assert sorted((r["id"], r["amount"]) for r in bronze().collect()) == [
        (1, Decimal("1.5")),
        (2, Decimal("2.5")),
        (3, Decimal("123456789.1234")),
    ]
    facts_df = delta_spark.read.format("delta").load(facts)
    [event] = facts_df.where("event = 'schema_change'").collect()
    assert "ALTER COLUMN amount" in event["detail"]


def test_after_a_dropped_column_bootstrap_and_resnapshot_read_it_as_null(
    delta_spark, sqlserver, workdir, latest
):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table(
        "ddl_drop", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL, extra VARCHAR(10)"
    )
    sqlserver.run(
        "INSERT INTO dbo.ddl_drop VALUES (1, 'old', 'x'), (2, 'old', 'x'), (3, 'old', 'x')"
    )
    sqlserver.wait_for_changes(ci, 3)
    options = {"connectionString": _reader(sqlserver, "ddl_drop", ci), "captureInstance": ci}

    def run(name):
        target, facts, ckpt = (os.path.join(workdir, name, n) for n in ("bronze", "facts", "ckpt"))
        q = stream(delta_spark, options).to_delta(
            target,
            f"ddl-drop-{name}",
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            on_data_loss="resnapshot",
        )
        q.awaitTermination()
        read = delta_spark.read.format("delta").load
        return read(target), read(facts)

    run("a")  # its snapshot selects extra
    sqlserver.run("ALTER TABLE dbo.ddl_drop DROP COLUMN extra")
    sqlserver.run("INSERT INTO dbo.ddl_drop VALUES (4, 'new')")
    sqlserver.wait_for_changes(ci, 4)
    bronze, facts = run("a")  # the stream goes on past the DROP, with an event
    assert [(r["id"], r["extra"]) for r in bronze.where("_operation = 2").collect()] == [(4, None)]
    [event] = facts.where("event = 'schema_change'").collect()
    assert "DROP COLUMN extra" in event["detail"]

    bronze, _ = run("b")  # a bootstrap after the DROP: the column still captured reads NULL
    assert sorted((r["id"], r["extra"]) for r in bronze.collect()) == [
        (i, None) for i in (1, 2, 3, 4)
    ]

    sqlserver.run("UPDATE dbo.ddl_drop SET v = 'new' WHERE id = 1")
    sqlserver.run("DELETE FROM dbo.ddl_drop WHERE id = 2")
    sqlserver.wait_for_changes(ci, 7)
    sqlserver.run(
        "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); "
        "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = ?, "
        "@low_water_mark = @lw, @threshold = 5000",
        (ci,),
    )
    bronze, facts = run("a")  # a purge: the re-snapshot reads NULL for extra too
    assert latest(bronze, "id", "v", facts) == _source(sqlserver, "ddl_drop")
    [event] = facts.where("event = 'resnapshot'").collect()
    resnapshot = bronze.where(f"_operation = 0 AND _start_lsn = '{event['max_lsn']}'")
    assert sorted((r["id"], r["extra"]) for r in resnapshot.collect()) == [
        (1, None),
        (3, None),
        (4, None),
    ]


def test_a_column_added_reaches_bronze_through_a_new_instance_with_its_values(
    delta_spark, sqlserver, workdir, latest, backend
):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("ddl_add", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL")
    sqlserver.run("INSERT INTO dbo.ddl_add VALUES (1, 'old'), (2, 'old'), (3, 'old')")
    sqlserver.wait_for_changes(ci, 3)
    options = {
        "connectionString": _reader(sqlserver, "ddl_add", ci),
        "captureInstance": ci,
        "backend": backend,
    }
    target, facts, ckpt = (os.path.join(workdir, n) for n in ("bronze", "facts", "ckpt"))

    def run():
        q = stream(delta_spark, options).to_delta(
            target,
            "ddl-add",
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            snapshot_on_switch=True,
        )
        q.awaitTermination()
        read = delta_spark.read.format("delta").load
        return read(target), read(facts)

    run()
    sqlserver.run("ALTER TABLE dbo.ddl_add ADD note VARCHAR(20) NULL")
    sqlserver.run("UPDATE dbo.ddl_add SET note = 'before v2' WHERE id = 1")
    sqlserver.run("UPDATE dbo.ddl_add SET v = 'new' WHERE id = 2")
    sqlserver.wait_for_changes(ci, 5)
    # the first instance does not capture note: the update of note alone wrote no change row
    assert sqlserver.run(f"SELECT COUNT(*) FROM cdc.[{ci}_CT] WHERE id = 1")[0][0] == 1
    bronze, facts_df = run()
    [added] = facts_df.where("event = 'schema_change'").collect()
    assert " ADD note " in added["detail"]
    assert "note" not in bronze.columns

    v2 = sqlserver.enable_cdc("ddl_add", "dbo_ddl_add_v2")
    sqlserver.run(f"GRANT SELECT ON cdc.[{v2}_CT] TO ddl_add_reader")
    sqlserver.run("UPDATE dbo.ddl_add SET note = 'after v2' WHERE id = 3")
    sqlserver.wait_for_changes(v2, 2)
    bronze, facts_df = run()  # load() infers note from v2; the batch crosses its start
    assert "note" in bronze.columns
    [switched] = facts_df.where("event = 'capture_instance_switched'").collect()
    assert switched["detail"] == f"{ci} -> {v2}"
    # id 1 changed before v2 existed: only the snapshot after the switch has its note
    source = _source(sqlserver, "ddl_add", "note")
    assert source == [(1, "before v2"), (2, None), (3, "after v2")]
    assert latest(bronze, "id", "note", facts_df) == source


def test_a_new_instance_without_its_grant_names_its_change_table(
    spark, sqlserver, workdir, backend
):
    ci = sqlserver.cdc_table("sw_grant", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.sw_grant VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    conn = _reader(sqlserver, "sw_grant", ci)
    _read(spark, sqlserver, ci, checkpoint=workdir, connectionString=conn, backend=backend)
    v2 = sqlserver.enable_cdc("sw_grant", "dbo_sw_grant_v2")
    sqlserver.run("INSERT INTO dbo.sw_grant VALUES (2)")
    sqlserver.wait_for_changes(v2, 1)
    with pytest.raises(Exception, match=r"PermissionError: .*cdc\.\[dbo_sw_grant_v2_CT\]"):
        _read(spark, sqlserver, ci, checkpoint=workdir, connectionString=conn, backend=backend)
    sqlserver.run(f"GRANT SELECT ON cdc.[{v2}_CT] TO sw_grant_reader")
    df, _ = _read(spark, sqlserver, ci, checkpoint=workdir, connectionString=conn, backend=backend)
    assert sorted((r["id"], r["_capture_instance"]) for r in df.collect()) == [(1, ci), (2, v2)]


def test_a_transaction_open_during_the_enable_is_read_whole_from_one_instance(spark, sqlserver):
    ci = sqlserver.cdc_table("sw_tx", "id INT NOT NULL PRIMARY KEY")
    conn = _reader(sqlserver, "sw_tx", ci)
    v2 = "dbo_sw_tx_v2"
    wrote, late = sqlserver.connect(), sqlserver.connect()
    try:
        wrote.cursor().execute("BEGIN TRAN; INSERT INTO dbo.sw_tx VALUES (1), (2), (3)")
        late_tx = late.cursor()
        late_tx.execute("BEGIN TRAN")  # begins before the enable, writes after it
        enable = threading.Thread(target=sqlserver.enable_cdc, args=("sw_tx", v2))
        enable.start()
        enable.join(3)
        assert enable.is_alive()  # the enable waits for the open transaction that wrote the table
        wrote.cursor().execute("COMMIT")
        enable.join(120)
        late_tx.execute("INSERT INTO dbo.sw_tx VALUES (11), (12), (13); COMMIT")
    finally:
        wrote.close()
        late.close()
    sqlserver.run(f"GRANT SELECT ON cdc.[{v2}_CT] TO sw_tx_reader")
    sqlserver.wait_for_changes(v2, 3)
    # the first transaction is only in the old instance, the second in both
    counts = (
        "SELECT (SELECT COUNT(*) FROM cdc.[{0}_CT] WHERE id < 10), "
        "(SELECT COUNT(*) FROM cdc.[{0}_CT] WHERE id > 10)"
    )
    assert tuple(sqlserver.run(counts.format(ci))[0]) == (3, 3)
    assert tuple(sqlserver.run(counts.format(v2))[0]) == (0, 3)

    df, _ = _read(spark, sqlserver, ci, connectionString=conn)
    rows = df.collect()
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 11, 12, 13]  # each change once
    first, second = (
        {(r["_capture_instance"], r["_start_lsn"]) for r in rows if (r["id"] > 10) == after}
        for after in (False, True)
    )
    [(first_ci, first_lsn)], [(second_ci, second_lsn)] = first, second  # one commit each
    assert (first_ci, second_ci) == (ci, v2)
    assert first_lsn < sqlserver.start_lsn(v2) <= second_lsn


# -- chunked snapshots (ADR 0028) ------------------------------------------------------------
def _churn(sqlserver, table: str, top: int, seed: int):
    """Commit one change to ``dbo.<table>`` (key ``id``, column ``v``) about every 20 ms on its
    own connection until the returned function is called, which returns how many it made: an
    update, a delete, a key update (``id`` to ``id + 1``) or an insert of a random id up to
    ``top``, or an insert above it. A key that exists already skips its insert or key update."""
    import random

    stop, state = threading.Event(), {"commits": 0, "error": None}

    def change(rnd: random.Random, i: int) -> tuple[str, tuple]:
        x, op, t = rnd.randrange(1, top + 1), rnd.random(), f"dbo.{table}"
        if op < 0.3:
            return f"UPDATE {t} SET v = ? WHERE id = ?", (f"u{i}", x)
        if op < 0.5:
            return f"DELETE FROM {t} WHERE id = ?", (x,)
        if op < 0.7:  # a key update: CDC records it as the old key's delete, the new one's insert
            return f"UPDATE {t} SET id = id + 1 WHERE id = ?", (x,)
        return f"INSERT INTO {t} VALUES (?, ?)", (x if op < 0.9 else top + i, f"i{i}")

    def loop():
        conn, rnd = sqlserver.connect(), random.Random(seed)
        cur = conn.cursor()
        try:
            while not stop.is_set():
                try:
                    cur.execute(*change(rnd, state["commits"]))
                except Exception as exc:
                    if "PRIMARY KEY" not in str(exc):  # else the key exists: skip it
                        raise
                state["commits"] += 1
                time.sleep(0.02)
        except Exception as exc:  # noqa: BLE001 - reported by finish()
            state["error"] = exc
        finally:
            conn.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def finish() -> int:
        stop.set()
        thread.join()
        assert state["error"] is None, state["error"]
        return state["commits"]

    return finish


def _marker(sqlserver, table: str, ci: str, column: str, value) -> None:
    """Insert one last row and wait for capture to reach it: the stream then reads up to it."""
    sqlserver.run(f"INSERT INTO dbo.{table} ({column}, v) VALUES (?, 'end')", (value,))
    sqlserver.wait_for(f"SELECT COUNT(*) FROM cdc.[{ci}_CT] WHERE {column} = ?", (value,))


def _chunked(delta_spark, options: dict, workdir: str, app_id: str):
    """Paths, the stream and ``run(**trigger)``: ``to_delta`` with a chunked bootstrap (which
    opens the snapshot the first time), started; returns the query."""
    from mssql_cdc import stream

    names = ("bronze", "silver", "facts", "control", "ckpt")
    paths = {n: os.path.join(workdir, n) for n in names}
    cdc = stream(delta_spark, options)

    def run(**trigger):
        return cdc.to_delta(
            paths["bronze"],
            app_id,
            paths["ckpt"],
            paths["facts"],
            trigger=trigger or {"availableNow": True},
            bootstrap=True,
            snapshot="chunked",
        )

    return paths, cdc, run


def _apply(delta_spark, paths: dict, ci: str, keys: list[str]) -> dict:
    from mssql_cdc import apply_changes

    return apply_changes(
        delta_spark,
        paths["bronze"],
        paths["silver"],
        ci,
        keys,
        control_table=paths["control"],
        facts_table=paths["facts"],
    )


def _image(delta_spark, path: str, *columns: str) -> set:
    return {
        tuple(r) for r in delta_spark.read.format("delta").load(path).select(*columns).collect()
    }


def _chunk_facts(delta_spark, facts: str) -> list[dict]:
    """The 'snapshot_chunk' facts rows: their detail with ``rows`` and ``lsn`` (the stamp L)."""
    rows = delta_spark.read.format("delta").load(facts).where("event = 'snapshot_chunk'").collect()
    found = [json.loads(r["detail"]) | {"rows": r["rows"], "lsn": r["min_lsn"]} for r in rows]
    return sorted(found, key=lambda d: d["chunk"])


def test_a_chunked_bootstrap_next_to_a_running_stream_and_a_writer_ends_equal_to_the_table(
    delta_spark, sqlserver, workdir
):
    # the rows written before CDC are only in the table: the chunks bring them, while the
    # stream, started at S, brings every change after it, deletes and key updates included
    sqlserver.run("CREATE TABLE dbo.ck_live (id INT NOT NULL PRIMARY KEY, v VARCHAR(20) NOT NULL)")
    sqlserver.run(f"INSERT INTO dbo.ck_live SELECT n, 'old' FROM {_ROWS} WHERE n <= 600")
    ci = sqlserver.enable_cdc("ck_live")
    options = {
        "connectionString": _reader(sqlserver, "ck_live", ci),
        "captureInstance": ci,
        "numPartitions": "2",  # two chunks per wave
    }
    paths, cdc, run = _chunked(delta_spark, options, workdir, "ck-live")
    finish = _churn(sqlserver, "ck_live", 600, seed=28)
    statuses, applied = [], []
    try:
        q = run(processingTime="1 second")  # opens S and streams from it, next to the waves
        while not (statuses and statuses[-1]["done"]):
            statuses.append(
                cdc.backfill(
                    paths["bronze"],
                    app_id="ck-live",
                    facts_table=paths["facts"],
                    chunk_rows=60,
                    max_waves=1,
                )
            )
            applied.append(_apply(delta_spark, paths, ci, ["id"]))  # wave by wave
            assert q.exception() is None and len(statuses) < 30
        q.stop()
    finally:
        commits = finish()
    _marker(sqlserver, "ck_live", ci, "id", -1)
    run().awaitTermination()
    applied.append(_apply(delta_spark, paths, ci, ["id"]))

    table = {tuple(r) for r in sqlserver.run("SELECT id, v FROM dbo.ck_live")}
    image = _image(delta_spark, paths["silver"], "id", "v")
    assert image == table, (commits, len(table), sorted(image ^ table)[:10])
    assert commits > 100, commits
    assert [s["chunks_done"] for s in statuses[:-1]] == [
        2 * (i + 1) for i in range(len(statuses) - 1)
    ]
    assert any(a["rebuilt"] for a in applied[-2:])  # at completion
    facts = delta_spark.read.format("delta").load(paths["facts"])
    [s] = [r["min_lsn"] for r in facts.where("event = 'snapshot_open'").collect()]
    chunks = _chunk_facts(delta_spark, paths["facts"])
    assert len(chunks) == statuses[-1]["chunks_done"] and min(c["lsn"] for c in chunks) >= s
    snap = delta_spark.read.format("delta").load(paths["bronze"]).where("_operation = 0")
    assert snap.count() == snap.select("id").distinct().count()  # no key read twice
    assert snap.where(f"_snapshot != '{s}' OR _start_lsn < '{s}'").isEmpty()


# 'n' and 'N' are one leading value under the database's case-insensitive collation and two
# in Python, and 'ş' sorts after both: bounds inside one leading value and across them
_COMP = f"SELECT IIF(n % 3 = 0, N'n', IIF(n % 3 = 1, N'N', N'ş')), n, 'old' FROM {_ROWS}"
KEYSETS = {  # table: (columns, rows, keys, changes between waves, a last key)
    "ck_comp": (
        "region NVARCHAR(10) NOT NULL, seq INT NOT NULL, v VARCHAR(20), PRIMARY KEY (region, seq)",
        f"{_COMP} WHERE n <= 90",
        ["region", "seq"],
        [
            ("INSERT INTO dbo.ck_comp VALUES (?, ?, 'new')", ("N", 1000)),
            ("UPDATE dbo.ck_comp SET v = 'upd' WHERE region = ? AND seq = ?", ("ş", 89)),
            ("DELETE FROM dbo.ck_comp WHERE region = ? AND seq = ?", ("n", 87)),
            ("UPDATE dbo.ck_comp SET seq = 2000 WHERE region = ? AND seq = ?", ("N", 88)),
        ],
        ("ş", 99999),
    ),
    # one varchar key, ordered as text: 'k1' < 'k10' < 'k2'
    "ck_code": (
        "code VARCHAR(12) NOT NULL PRIMARY KEY, v VARCHAR(20)",
        f"SELECT CONCAT('k', n), 'old' FROM {_ROWS} WHERE n <= 90",
        ["code"],
        [
            ("INSERT INTO dbo.ck_code VALUES (?, 'new')", ("k45a",)),
            ("UPDATE dbo.ck_code SET v = 'upd' WHERE code = ?", ("k89",)),
            ("DELETE FROM dbo.ck_code WHERE code = ?", ("k88",)),
            ("UPDATE dbo.ck_code SET code = 'k5a' WHERE code = ?", ("k87",)),
        ],
        ("zz",),
    ),
}


@pytest.mark.parametrize("name", KEYSETS)
def test_keyset_chunks_tile_composite_and_varchar_keys_under_changes(
    delta_spark, sqlserver, workdir, name
):
    ddl, rows, keys, changes, last = KEYSETS[name]
    sqlserver.run(f"CREATE TABLE dbo.{name} ({ddl})")
    sqlserver.run(f"INSERT INTO dbo.{name} {rows}")
    ci = sqlserver.enable_cdc(name)
    options = {
        "connectionString": _reader(sqlserver, name, ci),
        "captureInstance": ci,
        "numPartitions": "2",
    }
    paths, cdc, run = _chunked(delta_spark, options, workdir, name)
    run().awaitTermination()
    facts = delta_spark.read.format("delta").load(paths["facts"])
    [opened] = facts.where("event = 'snapshot_open'").collect()
    assert json.loads(opened["detail"])["plan"]["kind"] == "keyset"
    statuses, changes = [], list(changes)
    while not (statuses and statuses[-1]["done"]):
        statuses.append(
            cdc.backfill(
                paths["bronze"], app_id=name, facts_table=paths["facts"], chunk_rows=7, max_waves=1
            )
        )
        if changes:  # in keys read already, and in keys not read yet
            sqlserver.run(*changes.pop(0))
        assert len(statuses) < 20
    total = statuses[0]["chunks_total"]  # planned by the first call, the plan's from then on
    assert total > 2 and all(s["chunks_total"] == total for s in statuses)
    marker = ", ".join(["?"] * len(last))
    sqlserver.run(f"INSERT INTO dbo.{name} ({', '.join(keys)}, v) VALUES ({marker}, 'end')", last)
    sqlserver.wait_for(f"SELECT COUNT(*) FROM cdc.[{ci}_CT] WHERE v = 'end'")
    run().awaitTermination()
    assert _apply(delta_spark, paths, ci, keys)["rebuilt"]
    table = {tuple(r) for r in sqlserver.run(f"SELECT {', '.join(keys)}, v FROM dbo.{name}")}
    assert _image(delta_spark, paths["silver"], *keys, "v") == table

    chunks = _chunk_facts(delta_spark, paths["facts"])
    assert [c["chunk"] for c in chunks] == list(range(total))
    assert chunks[0]["lo"] is None and [c["last"] for c in chunks].index(True) == len(chunks) - 1
    assert all(a["hi"] == b["lo"] for a, b in pairwise(chunks))  # each starts where one ended
    # planned up front with 7 rows each (the last with the rest): read later, a chunk holds
    # that give or take the changes made in its range since, at most two here
    assert all(5 <= c["rows"] <= 9 for c in chunks[:-1]) and 0 < chunks[-1]["rows"] <= 9
    snap = delta_spark.read.format("delta").load(paths["bronze"]).where("_operation = 0")
    assert snap.count() == snap.select(*keys).distinct().count() == sum(c["rows"] for c in chunks)


def test_a_chunk_waits_under_read_committed_for_a_transaction_holding_locks_in_its_range(
    delta_spark, sqlserver, workdir
):
    from mssql_cdc.lsn import normalize

    sqlserver.run("CREATE TABLE dbo.ck_lock (id INT NOT NULL PRIMARY KEY, v VARCHAR(20) NOT NULL)")
    sqlserver.run(f"INSERT INTO dbo.ck_lock SELECT n, 'old' FROM {_ROWS} WHERE n <= 40")
    ci = sqlserver.enable_cdc("ck_lock")
    options = {
        "connectionString": _reader(sqlserver, "ck_lock", ci),
        "captureInstance": ci,
        "numPartitions": "2",
    }
    paths, cdc, run = _chunked(delta_spark, options, workdir, "ck-lock")
    run().awaitTermination()  # chunks [-, 11) [11, 21) in wave 0, [21, 31) [31, 41) in wave 1
    backfill = {"app_id": "ck-lock", "facts_table": paths["facts"]}
    # the first call plans, counting every key (under READ COMMITTED it would wait too), and
    # reads wave 0
    assert cdc.backfill(paths["bronze"], chunk_rows=10, max_waves=1, **backfill)["chunks_done"] == 2
    holder = sqlserver.connect()
    held = holder.cursor()
    held.execute("SELECT @@SPID")
    spid = held.fetchone()[0]
    held.execute("BEGIN TRAN; UPDATE dbo.ck_lock SET v = 'held' WHERE id BETWEEN 21 AND 25")
    waits: dict = {}

    def release():  # once a chunk's read waits on the holder's locks, commit
        deadline = time.time() + 120
        while not waits and time.time() < deadline:
            blocked = sqlserver.run(
                "SELECT session_id, wait_type FROM sys.dm_exec_requests "
                "WHERE blocking_session_id = ?",
                (spid,),
            )
            waits.update(dict(blocked))
            time.sleep(0.5)
        time.sleep(2)
        held.execute("COMMIT")

    releaser = threading.Thread(target=release, daemon=True)
    releaser.start()
    try:
        status = cdc.backfill(paths["bronze"], **backfill)
    finally:
        releaser.join(130)
        holder.close()
    assert status["done"] and status["chunks_done"] == 4
    assert waits and all(w.startswith("LCK_M_S") for w in waits.values()), waits  # under RC
    sqlserver.wait_for(f"SELECT COUNT(*) FROM cdc.[{ci}_CT] WHERE v = 'held'")
    [(commit,)] = sqlserver.run(
        f"SELECT CONVERT(varchar(22), MAX(__$start_lsn), 1) FROM cdc.[{ci}_CT] WHERE v = 'held'"
    )
    snap = delta_spark.read.format("delta").load(paths["bronze"]).where("id BETWEEN 21 AND 25")
    # the chunk read the rows once committed, after its stamp L: an image newer than L, which
    # the stream's change at that commit equals
    assert {(r["v"], r["_chunk"]) for r in snap.collect()} == {("held", 2)}
    assert all(r["_start_lsn"] < normalize(commit) for r in snap.collect())
    run().awaitTermination()
    assert _apply(delta_spark, paths, ci, ["id"])["rebuilt"]
    table = {tuple(r) for r in sqlserver.run("SELECT id, v FROM dbo.ck_lock")}
    assert _image(delta_spark, paths["silver"], "id", "v") == table


def test_a_chunked_resnapshot_deletes_stale_datetime2_keys_but_not_one_in_a_bounds_microsecond(
    delta_spark, sqlserver, workdir, monkeypatch
):
    """A datetime2(7) key holds 100 ns, Spark microseconds: k = ...02.1234567 is below the bound
    b1 = ...02.1234568 on SQL Server, so chunk 0 holds it, and reads ...02.123456 in Spark, the
    microsecond b1 truncates to. Chunk 1, [b1, b2), must not delete it; the keys deleted in the
    purged gap strictly inside a range (x, and y = ...04.0000001) go with their wave."""
    from mssql_cdc import client as cdc_client
    from mssql_cdc import stream

    ci = sqlserver.cdc_table(
        "ck_dt2", "at DATETIME2(7) NOT NULL PRIMARY KEY, v VARCHAR(5) NOT NULL"
    )
    at = {"a": "00.5", "x": "01", "k": "02.1234567", "c": "03", "y": "04.0000001", "z": "07"}
    for v, s in at.items():
        sqlserver.run(
            "INSERT INTO dbo.ck_dt2 VALUES (CAST(? AS datetime2(7)), ?)",
            (f"2026-09-28T10:00:{s}", v),
        )
    sqlserver.wait_for_changes(ci, len(at))
    options = {
        "connectionString": _reader(sqlserver, "ck_dt2", ci),
        "captureInstance": ci,
        "numPartitions": "1",  # one chunk per wave
    }
    paths = {n: os.path.join(workdir, n) for n in ("bronze", "silver", "facts", "control", "ckpt")}
    cdc = stream(delta_spark, options)

    def run(**kw):
        cdc.to_delta(
            paths["bronze"],
            "ck-dt2",
            paths["ckpt"],
            paths["facts"],
            trigger={"availableNow": True},
            bootstrap=True,
            on_data_loss="resnapshot",
            **kw,
        ).awaitTermination()

    def values() -> set:
        return {v for _, v in _image(delta_spark, paths["silver"], "at", "v")}

    run()  # generation 0, a whole snapshot
    sqlserver.run("INSERT INTO dbo.ck_dt2 VALUES ('2026-09-28T10:00:09', 'm')")
    sqlserver.wait_for_changes(ci, len(at) + 1)
    run()  # the checkpoint at that commit
    _apply(delta_spark, paths, ci, ["at"])
    assert values() == {*at, "m"}
    sqlserver.run("DELETE FROM dbo.ck_dt2 WHERE v IN ('x', 'y', 'z')")
    sqlserver.wait_for_changes(ci, len(at) + 4)
    sqlserver.run(
        "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); "
        "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = ?, "
        "@low_water_mark = @lw, @threshold = 5000",
        (ci,),
    )
    run(snapshot="chunked")  # generation 1 opens a chunked re-snapshot

    # bounds with 100 ns digits, as the snapshot reader CASTs them to datetime2(7)
    b1, b2 = "2026-09-28T10:00:02.1234568", "2026-09-28T10:00:05.0000000"
    monkeypatch.setattr(cdc_client, "plan_chunks", lambda *_: [[None, b1], [b1, b2], [b2, None]])

    def wave() -> set:
        cdc.backfill(paths["bronze"], app_id="ck-dt2", facts_table=paths["facts"], max_waves=1)
        _apply(delta_spark, paths, ci, ["at"])
        return values()

    assert wave() == {"a", "k", "c", "y", "z", "m"}  # x, inside chunk 0
    assert wave() == {"a", "k", "c", "z", "m"}  # y, inside chunk 1; k, in b1's microsecond, kept
    assert wave() == {"a", "k", "c", "m"}  # z, at the completion's rebuild
    table = {tuple(r) for r in sqlserver.run("SELECT at, v FROM dbo.ck_dt2")}
    assert _image(delta_spark, paths["silver"], "at", "v") == table
    assert (datetime(2026, 9, 28, 10, 0, 2, 123456), "k") in table  # the driver truncates


def test_a_snapshot_isolation_read_takes_the_committed_rows_without_waiting(sqlserver, backend):
    ci = sqlserver.cdc_table("ck_si", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL")
    sqlserver.run(f"INSERT INTO dbo.ck_si SELECT n, 'old' FROM {_ROWS} WHERE n <= 10")
    options = {"connectionString": _reader(sqlserver, "ck_si", ci), "backend": backend}

    def read(isolation):
        with closing(make_client(options)) as client:
            batches = client.iter_table(
                "dbo", "ck_si", ["id", "v"], ["id"], None, None, None, 100, isolation
            )
            return sorted((r["id"], r["v"]) for b in batches for r in b.to_pylist())

    with pytest.raises(Exception, match="[Ss]napshot isolation .*not allowed"):
        read("snapshot")  # until the DBA allows it
    sqlserver.run("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON")
    holder = sqlserver.connect()
    try:
        holder.cursor().execute("BEGIN TRAN; UPDATE dbo.ck_si SET v = 'held' WHERE id = 5")
        started = time.monotonic()
        # the versions committed when the read began, and no wait for the writer's locks
        assert read("snapshot") == [(i, "old") for i in range(1, 11)]
        assert time.monotonic() - started < 10
        holder.cursor().execute("COMMIT")
        assert read("snapshot")[4] == (5, "held")
    finally:
        holder.close()
        sqlserver.run("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION OFF")


@pytest.mark.parametrize("keys", ["a", "a, b"])  # rows counted per slice; keyset seeks
def test_a_snapshot_isolation_plan_does_not_wait_for_a_writers_locks(sqlserver, backend, keys):
    from mssql_cdc.client import plan_chunks, snapshot_plan

    table = "ck_plan_si_int" if keys == "a" else "ck_plan_si_keyset"
    ci = sqlserver.cdc_table(
        table, f"a INT NOT NULL, b VARCHAR(5) NOT NULL, v VARCHAR(10), PRIMARY KEY ({keys})"
    )
    sqlserver.run(
        f"INSERT INTO dbo.{table} SELECT n, CONCAT('b', n), 'old' FROM {_ROWS} WHERE n <= 40"
    )
    options = {"connectionString": _reader(sqlserver, table, ci), "backend": backend}
    with closing(make_client(options)) as client:  # recorded at the open, before the lock
        source = client.source_table(ci)
        extent = snapshot_plan(client, ci, source)

    def plan(isolation, out: dict) -> None:
        try:
            with closing(make_client(options)) as client:
                out["plan"] = plan_chunks(client, ci, source, extent, 10, isolation)
                out["level"] = client._b.scalar(
                    "SELECT transaction_isolation_level FROM sys.dm_exec_sessions "
                    "WHERE session_id = @@SPID"
                )
        except Exception as exc:  # noqa: BLE001 - the assertion shows it
            out["plan"] = exc

    def start(isolation) -> tuple[threading.Thread, dict]:
        out: dict = {}
        thread = threading.Thread(target=plan, args=(isolation, out), daemon=True)
        thread.start()
        return thread, out

    expected: dict = {}
    plan(None, expected)
    sqlserver.run("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON")
    holder = sqlserver.connect()
    try:
        held = holder.cursor()
        held.execute("SELECT @@SPID")
        spid = held.fetchone()[0]
        held.execute(f"BEGIN TRAN; UPDATE dbo.{table} SET v = 'held' WHERE a BETWEEN 21 AND 25")
        snap, snapped = start("snapshot")
        snap.join(10)
        fast = not snap.is_alive()
        committed, waited = start(None)
        # READ COMMITTED: the GROUP BY or the seeks wait on the holder's locks
        sqlserver.wait_for(
            "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id = ?",
            (spid,),
            timeout=60,
        )
        assert committed.is_alive()
        held.execute("COMMIT")
        committed.join(60)
        snap.join(60)
    finally:
        holder.close()
        sqlserver.run("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION OFF")
    assert fast and snapped["plan"] == expected["plan"]  # the committed versions, no wait
    assert waited["plan"] == expected["plan"]
    assert len(expected["plan"]) == 4
    # the integer plan's last count, a batch without parameters, leaves its session at SNAPSHOT
    # (sys.dm_exec_sessions: 2 is READ COMMITTED, 5 SNAPSHOT); a client of its own each time
    assert (expected["level"], waited["level"]) == (2, 2)
    if keys == "a":
        assert snapped["level"] == 5


@pytest.mark.parametrize("grant", ["table", "columns"])
def test_chunk_planning_needs_only_select_on_the_table(sqlserver, grant, backend):
    from mssql_cdc.client import plan_chunks, snapshot_plan

    table = f"ck_plan_{grant}"
    ci = sqlserver.cdc_table(
        table, "a INT NOT NULL, b VARCHAR(5) NOT NULL, v INT, PRIMARY KEY (a, b)"
    )
    sqlserver.run(
        f"INSERT INTO dbo.{table} SELECT n % 4, CONCAT('b', n), n FROM {_ROWS} WHERE n <= 30"
    )
    on = f"dbo.{table}" if grant == "table" else f"dbo.{table} (a, b, v)"
    user = f"{table}_reader"
    conn = sqlserver.login(
        user, f"GRANT SELECT ON {on} TO {user}", f"GRANT SELECT ON cdc.[{ci}_CT] TO {user}"
    )
    keys = sorted((r[0], r[1]) for r in sqlserver.run(f"SELECT a, b FROM dbo.{table}"))
    with closing(make_client({"connectionString": conn, "backend": backend})) as client:
        source = client.source_table(ci)
        # sys.sp_spaceused is public; the key's seeks need SELECT on its columns, nothing more
        assert client.row_estimate("dbo", table) == 30
        assert client.key_max("dbo", table, source.keys) == keys[-1]
        types = client.key_types(ci, source.keys)
        assert client.key_bound("dbo", table, source.keys, types, None, None, 10) == keys[10]
        assert client.key_bound("dbo", table, source.keys, types, keys[10], None, 25) is None
        extent = snapshot_plan(client, ci, source)
        assert extent == {"kind": "keyset", "max": list(keys[-1])}
        chunks = plan_chunks(client, ci, source, extent, 8)
    assert chunks == [
        [None, list(keys[8])],
        [list(keys[8]), list(keys[16])],
        [list(keys[16]), list(keys[24])],
        [list(keys[24]), None],
    ]


def test_a_datetime2_plan_reads_the_keys_in_maxs_microsecond(sqlserver, backend):
    """The driver truncates datetime2(7) to the microsecond: MAX ...00.1234707 and the key
    before it, ...00.1234700, both read back as ...00.123470. A last chunk ending at the key
    after MAX, as its truncated text, would read neither: it is open instead."""
    from mssql_cdc.client import _key_tuple, plan_chunks, snapshot_plan

    ci = sqlserver.cdc_table("ck_dt2_top", "at DATETIME2(7) NOT NULL PRIMARY KEY, v INT")
    sqlserver.run(  # 20 keys 700 ns apart, from ...00.1234574 to ...00.1234707
        "INSERT INTO dbo.ck_dt2_top SELECT DATEADD(ns, 700 * n, "
        f"CAST('2026-09-28T10:00:00.1234567' AS datetime2(7))), n FROM {_ROWS} WHERE n <= 20"
    )
    options = {"connectionString": _reader(sqlserver, "ck_dt2_top", ci), "backend": backend}
    with closing(make_client(options)) as client:
        source = client.source_table(ci)
        types = client.key_types(ci, source.keys)
        plan = plan_chunks(client, ci, source, snapshot_plan(client, ci, source), 6)
        read = [
            r["v"]
            for lo, hi in plan
            for b in client.iter_table(
                "dbo", "ck_dt2_top", ["at", "v"], ["at"], types, _key_tuple(lo), _key_tuple(hi), 100
            )
            for r in b.to_pylist()
        ]
    assert sorted(read) == list(range(1, 21))
    assert plan[-1][1] is None


def test_an_integer_plan_counts_a_skewed_key_on_the_server_and_backfills_it(
    delta_spark, sqlserver, workdir
):
    from mssql_cdc.client import plan_chunks, snapshot_plan

    # a dense cluster, a sparse region and a sentinel near the bigint max
    sqlserver.run(
        "CREATE TABLE dbo.ck_skew (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(20) NOT NULL)"
    )
    sqlserver.run(f"INSERT INTO dbo.ck_skew SELECT n, 'old' FROM {_ROWS} WHERE n <= 1000")
    sqlserver.run(f"INSERT INTO dbo.ck_skew SELECT n * 10000, 'old' FROM {_ROWS} WHERE n <= 50")
    top = 2**63 - 1  # the last chunk ends at MAX + 1, past bigint: compared as numeric
    sqlserver.run(f"INSERT INTO dbo.ck_skew VALUES ({top}, 'old')")
    ci = sqlserver.enable_cdc("ck_skew")
    ids = sorted(r[0] for r in sqlserver.run("SELECT id FROM dbo.ck_skew"))
    options = {
        "connectionString": _reader(sqlserver, "ck_skew", ci),  # SELECT on the table only
        "captureInstance": ci,
        "numPartitions": "4",
    }
    with closing(make_client(options)) as client:
        source = client.source_table(ci)
        extent = snapshot_plan(client, ci, source)
        assert extent == {"kind": "int", "lo": 1, "hi": top, "rows": len(ids)}
        plan = plan_chunks(client, ci, source, extent, 100)
    assert plan[0][0] is None and plan[-1][1] == top + 1
    assert all(a[1] == b[0] for a, b in pairwise(plan))
    sizes = [sum((lo is None or i >= lo) and i < hi for i in ids) for lo, hi in plan]
    # counted per slice on the server: none empty, none above chunk_rows, no two neighbours
    # that would fit in one; a step from MIN to MAX would put all but one row in the first
    assert sum(sizes) == len(ids) and all(0 < n <= 100 for n in sizes)
    assert all(a + b > 100 for a, b in pairwise(sizes)) and len(plan) <= 12

    paths, cdc, run = _chunked(delta_spark, options, workdir, "ck-skew")
    run().awaitTermination()  # opens S
    status = cdc.backfill(
        paths["bronze"], app_id="ck-skew", facts_table=paths["facts"], chunk_rows=100
    )
    assert status["done"] and status["chunks_done"] == status["chunks_total"] == len(plan)
    facts = delta_spark.read.format("delta").load(paths["facts"])
    [planned] = facts.where("event = 'snapshot_plan'").collect()
    assert json.loads(planned["detail"])["chunks"] == plan
    assert [c["rows"] for c in _chunk_facts(delta_spark, paths["facts"])] == sizes
    _marker(sqlserver, "ck_skew", ci, "id", -1)
    run().awaitTermination()
    assert _apply(delta_spark, paths, ci, ["id"])["rebuilt"]
    table = {tuple(r) for r in sqlserver.run("SELECT id, v FROM dbo.ck_skew")}
    assert _image(delta_spark, paths["silver"], "id", "v") == table


def test_key_bound_seeks_each_piece_for_its_first_keys_only(sqlserver):
    ci = sqlserver.cdc_table(
        "ck_bound", "company INT NOT NULL, id INT NOT NULL, v INT, PRIMARY KEY (company, id)"
    )
    sqlserver.run(f"INSERT INTO dbo.ck_bound SELECT n % 3, n, n FROM {_ROWS}")
    keys = sorted(tuple(r) for r in sqlserver.run("SELECT company, id FROM dbo.ck_bound"))
    lo, hi, n = (0, 3000), (2, 30002), 500
    inside = [k for k in keys if lo <= k < hi]
    client = make_client({"connectionString": sqlserver.connection_string})
    sent = []
    real = client._b.batches

    def record(sql, params, batch_size):
        sent.append((sql, params))
        return real(sql, params, batch_size)

    client._b.batches = record
    try:
        types = client.key_types(ci, ["company", "id"])
        sent.clear()
        assert client.key_bound("dbo", "ck_bound", ["company", "id"], types, lo, hi, n) == inside[n]
    finally:
        client.close()
    [(sql, params)] = sent
    rows, plan = _plan(sqlserver, sql, params)
    # three pieces (company 0 from id 3000, company 1, company 2 below id 30002), each a seek
    # that stops after its first n + 1 keys: never the ~33,000 keys of the range
    assert rows == 1 and sql.count(f"TOP ({n + 1})") == 3
    assert _rows_read(plan) <= 3 * (n + 1), _rows_read(plan)


def test_reconcile_matches_a_quiet_table_and_classifies_differences_injected_in_silver(
    delta_spark, sqlserver, workdir
):
    from delta.tables import DeltaTable

    from mssql_cdc import reconcile

    sqlserver.run("CREATE TABLE dbo.rc_live (id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL)")
    sqlserver.run(f"INSERT INTO dbo.rc_live SELECT n, 'old' FROM {_ROWS} WHERE n <= 300")
    ci = sqlserver.enable_cdc("rc_live")
    options = {
        "connectionString": _reader(sqlserver, "rc_live", ci),
        "captureInstance": ci,
        "numPartitions": "2",
    }
    paths, cdc, run = _chunked(delta_spark, options, workdir, "rc-live")
    run().awaitTermination()
    assert cdc.backfill(
        paths["bronze"], app_id="rc-live", facts_table=paths["facts"], chunk_rows=40
    )["done"]
    sqlserver.run("UPDATE dbo.rc_live SET v = 'new' WHERE id = 120")
    sqlserver.run("DELETE FROM dbo.rc_live WHERE id = 250")
    sqlserver.wait_for(f"SELECT COUNT(*) FROM cdc.[{ci}_CT] WHERE id = 250")
    run().awaitTermination()
    assert _apply(delta_spark, paths, ci, ["id"])["rebuilt"]

    def check(**kw):
        return reconcile(
            delta_spark,
            options,
            paths["silver"],
            bronze=paths["bronze"],
            control_table=paths["control"],
            facts_table=paths["facts"],
            bucket_rows=50,
            seed=3,
            **kw,
        )

    quiet = check(sample=1.0)  # every bucket counted and compared row by row
    assert (quiet["mismatch"], quiet["in_flight"], quiet["failures"]) == (0, 0, {})
    assert quiet["match"] == quiet["buckets"] == quiet["hashed"] >= 5
    silver = DeltaTable.forPath(delta_spark, paths["silver"])
    silver.delete("id = 7")  # an insert silver never got
    silver.update("id = 120", {"v": "'old'"})  # an update it missed
    stale = delta_spark.read.format("delta").load(paths["silver"]).where("id = 8")
    stale.selectExpr("5000 AS id", "v", "_start_lsn", "_commit_ts").write.format("delta").mode(
        "append"
    ).save(paths["silver"])  # a delete it missed
    found = check(sample=1.0)
    failures = {
        json.loads(r["key"])["id"]: r["failure_type"]
        for r in found["report"].where("key IS NOT NULL").collect()
    }
    assert failures == {7: "MISSING_TARGET", 120: "RECORD_DIFF", 5000: "MISSING_SOURCE"}
    assert found["mismatch"] == 2 and found["in_flight"] == 0  # 120 keeps the counts equal


def test_reconcile_buckets_a_date_key_as_sql_server_does(delta_spark, sqlserver, workdir):
    """Tier 1 on a date key: the server's DATEDIFF from 1970-01-01 against Spark's datediff
    with pmod flooring, dates before 1970, and the 9999-12-31 sentinel a calendar table has,
    whose bucket ends past the last date."""
    from delta.tables import DeltaTable

    from mssql_cdc import reconcile

    sqlserver.run("CREATE TABLE dbo.rc_date (d DATE NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL)")
    sqlserver.run(
        "INSERT INTO dbo.rc_date "
        f"SELECT DATEADD(day, n * 97, '1900-01-01'), 'old' FROM {_ROWS} WHERE n <= 300"
    )  # from 1900-04-08, every 97 days, across 1970
    sqlserver.run("INSERT INTO dbo.rc_date VALUES ('9999-12-31', 'end')")
    ci = sqlserver.enable_cdc("rc_date")
    options = {
        "connectionString": _reader(sqlserver, "rc_date", ci),
        "captureInstance": ci,
        "numPartitions": "2",
    }
    paths, cdc, run = _chunked(delta_spark, options, workdir, "rc-date")
    run().awaitTermination()
    assert cdc.backfill(
        paths["bronze"], app_id="rc-date", facts_table=paths["facts"], chunk_rows=40
    )["done"]
    run().awaitTermination()
    assert _apply(delta_spark, paths, ci, ["d"])["rebuilt"]

    def check():
        return reconcile(
            delta_spark,
            options,
            paths["silver"],
            bronze=paths["bronze"],
            control_table=paths["control"],
            bucket_rows=50,
            sample=1.0,
        )

    quiet = check()
    assert (quiet["mismatch"], quiet["in_flight"], quiet["failures"]) == (0, 0, {})
    assert quiet["match"] == quiet["buckets"] == quiet["hashed"] >= 5
    silver = DeltaTable.forPath(delta_spark, paths["silver"])
    silver.delete("d = DATE'1900-04-08'")  # the first key: an insert silver never got
    silver.update("d = DATE'9999-12-31'", {"v": "'lost'"})  # the sentinel's update it missed
    found = check()
    failures = {
        json.loads(r["key"])["d"]: r["failure_type"]
        for r in found["report"].where("key IS NOT NULL").collect()
    }
    assert failures == {"1900-04-08": "MISSING_TARGET", "9999-12-31": "RECORD_DIFF"}
    assert found["mismatch"] == 1 and found["in_flight"] == 0
