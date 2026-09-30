"""The source against a real SQL Server 2022 with CDC (see conftest.py)."""

from __future__ import annotations

import os
import re
import threading
import time
import uuid
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


def test_stream_resumes_from_checkpoint_with_transactions_in_order(spark, sqlserver, workdir):
    ci = sqlserver.cdc_table(
        "orders", "order_id INT NOT NULL PRIMARY KEY, status VARCHAR(20) NOT NULL"
    )
    sqlserver.run(
        "BEGIN TRAN; INSERT INTO dbo.orders VALUES (1, 'new'); "
        "UPDATE dbo.orders SET status = 'paid' WHERE order_id = 1; "
        "DELETE FROM dbo.orders WHERE order_id = 1; COMMIT"
    )
    sqlserver.wait_for_changes(ci, 4)  # insert, update before/after, delete

    first, _ = _read(spark, sqlserver, ci, checkpoint=workdir, arrowBatchSize="3")
    rows = first.orderBy("_start_lsn", "_command_id", "_seqval", "_operation").collect()
    assert [r["_operation"] for r in rows] == [2, 3, 4, 1]
    ids = [r["_command_id"] for r in rows]
    assert None not in ids and ids == sorted(ids)  # __$command_id orders the statements
    assert len({r["_start_lsn"] for r in rows}) == 1  # one commit

    sqlserver.run("INSERT INTO dbo.orders VALUES (2, 'new')")
    sqlserver.wait_for_changes(ci, 5)
    after, _ = _read(spark, sqlserver, ci, checkpoint=workdir, arrowBatchSize="3")
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
    client = make_client({"connectionString": conn})
    try:  # own session's wait stats: no VIEW SERVER STATE for this login either
        assert isinstance(client.network_wait_ms(), int)
    finally:
        client.close()


def test_purged_range_stops_the_stream(spark, sqlserver, workdir):
    ci = sqlserver.cdc_table("purge_probe", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.purge_probe VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    _read(spark, sqlserver, ci, checkpoint=workdir)  # checkpoint now at the first commit
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

    class Pre2022(MssqlPythonBackend):  # SQL Server 2016-2019 have no CURRENT_TIMEZONE_ID()
        def scalar(self, sql, params=()):
            if "CURRENT_TIMEZONE_ID" in sql:
                raise RuntimeError(
                    "'CURRENT_TIMEZONE_ID' is not a recognized built-in function name."
                )
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
    sqlserver.run(
        "INSERT INTO dbo.skewed SELECT 100 + n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7)) v(n)"
    )
    sqlserver.wait_for_changes(ci, 16)
    client = make_client({"connectionString": sqlserver.connection_string})
    try:
        lo, hi = client.min_lsn(ci), client.max_lsn()
        bounds = client.split_points(ci, lo, hi, 2)
        ranges, prev = [], lo
        for b in bounds:
            ranges.append((prev, b))
            prev = client.increment_lsn(b)
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
    delta_spark, sqlserver, workdir, latest
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
}


@pytest.mark.parametrize("name", SNAPSHOT_TILES)
def test_snapshot_tiles_composite_and_string_keys(spark, sqlserver, name):
    ddl, rows, types = SNAPSHOT_TILES[name]
    ci = sqlserver.cdc_table(name, ddl)
    sqlserver.run(f"INSERT INTO dbo.{name} {rows}")
    client = make_client({"connectionString": sqlserver.connection_string})
    try:  # lengths in characters, precisions as declared
        assert client.key_types(ci, client.source_table(ci).keys) == types
    finally:
        client.close()
    df = (
        spark.read.format("mssql_cdc_snapshot")
        .option("connectionString", sqlserver.connection_string)
        .option("captureInstance", ci)
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
    with pytest.raises(Exception, match=f"only in capture instance '{ci}', disabled before"):
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
    assert event["detail"].startswith("amount: ") and "ALTER COLUMN" in event["detail"]


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
    assert event["detail"].startswith("extra: ") and "DROP COLUMN" in event["detail"]

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
    delta_spark, sqlserver, workdir, latest
):
    from mssql_cdc import stream

    ci = sqlserver.cdc_table("ddl_add", "id INT NOT NULL PRIMARY KEY, v VARCHAR(10) NOT NULL")
    sqlserver.run("INSERT INTO dbo.ddl_add VALUES (1, 'old'), (2, 'old'), (3, 'old')")
    sqlserver.wait_for_changes(ci, 3)
    options = {"connectionString": _reader(sqlserver, "ddl_add", ci), "captureInstance": ci}
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
    assert added["detail"].startswith("note: ") and " ADD " in added["detail"].upper()
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


def test_a_new_instance_without_its_grant_names_its_change_table(spark, sqlserver, workdir):
    ci = sqlserver.cdc_table("sw_grant", "id INT NOT NULL PRIMARY KEY")
    sqlserver.run("INSERT INTO dbo.sw_grant VALUES (1)")
    sqlserver.wait_for_changes(ci, 1)
    conn = _reader(sqlserver, "sw_grant", ci)
    _read(spark, sqlserver, ci, checkpoint=workdir, connectionString=conn)
    v2 = sqlserver.enable_cdc("sw_grant", "dbo_sw_grant_v2")
    sqlserver.run("INSERT INTO dbo.sw_grant VALUES (2)")
    sqlserver.wait_for_changes(v2, 1)
    with pytest.raises(Exception, match=r"PermissionError: .*cdc\.\[dbo_sw_grant_v2_CT\]"):
        _read(spark, sqlserver, ci, checkpoint=workdir, connectionString=conn)
    sqlserver.run(f"GRANT SELECT ON cdc.[{v2}_CT] TO sw_grant_reader")
    df, _ = _read(spark, sqlserver, ci, checkpoint=workdir, connectionString=conn)
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
