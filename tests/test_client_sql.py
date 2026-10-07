"""SqlCdcClient builds T-SQL; check the generated statements without a server."""

import re
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from mssql_cdc.client import Backend, CdcClient, SqlCdcClient, make_client


class Recorder(Backend):
    named = 1  # SQL Server 2022 or Azure SQL: CURRENT_TIMEZONE_ID() names the zone

    def __init__(self, scalar_value="0x0000002A000001000001", tz="UTC", range_offset=None):
        self.calls = []
        self.value = scalar_value
        self.tz = tz
        self.range_offset = range_offset  # what the per-range offset query answers

    def batches(self, sql, params, batch_size):
        self.calls.append((sql, tuple(params)))
        return iter(())

    def scalar(self, sql, params=()):
        self.calls.append((sql, tuple(params)))
        if "SERVERPROPERTY('ProductMajorVersion')" in sql:
            return self.named
        if "CURRENT_TIMEZONE_ID" in sql:
            return self.tz
        if sql.startswith("SELECT CASE WHEN DATEDIFF(day, MIN(tran_end_time)"):
            return self.range_offset
        return self.value


def test_changes_query_shape():
    rec = Recorder()
    client = SqlCdcClient(rec)
    list(client.iter_changes("dbo_orders", "0x01", "0x02", ["order_id", "status"], True, 100))
    sql, params = rec.calls[-1]
    assert "FROM cdc.[dbo_orders_CT] c " in sql and "fn_cdc_get_all_changes" not in sql
    assert (
        "WHERE c.[__$start_lsn] BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)"
        in sql
    )
    assert "JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.[__$start_lsn]" in sql
    assert sql.rstrip().endswith(
        "ORDER BY c.[__$start_lsn], c.[__$command_id], c.[__$seqval], c.[__$operation]"
    )
    assert "c.[order_id], c.[status]" in sql
    assert params == ("0x01", "0x02")


def test_changes_query_without_command_id():
    rec = Recorder()
    list(SqlCdcClient(rec).iter_changes("dbo_orders", "0x01", "0x02", ["id"], False, 100))
    sql, _ = rec.calls[-1]
    assert "__$command_id" not in sql


def test_timezone_conversion():
    rec = Recorder()
    client = SqlCdcClient(rec, source_timezone="E. South America Standard Time")
    list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert "AT TIME ZONE N'E. South America Standard Time') AT TIME ZONE 'UTC'" in rec.calls[-1][0]
    utc = Recorder()
    list(
        SqlCdcClient(utc, source_timezone="UTC").iter_changes(
            "dbo_orders", "0x01", "0x02", [], True, 10
        )
    )
    assert "AT TIME ZONE" not in utc.calls[-1][0]
    assert not any("CURRENT_TIMEZONE_ID" in sql for sql, _ in utc.calls)


def test_named_zone_converts_a_range_with_one_offset_by_dateadd():
    rec = Recorder(range_offset=-180)  # both ends of the range at UTC-3, under 7 days apart
    client = SqlCdcClient(rec, source_timezone="E. South America Standard Time")
    list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    offset_sql, offset_params = rec.calls[-2]
    assert "DATEDIFF(day, MIN(tran_end_time), MAX(tran_end_time)) < 7" in offset_sql
    assert offset_params == ("0x01", "0x02")
    changes_sql = rec.calls[-1][0]
    assert (
        "CAST(DATEADD(minute, 180, m.tran_end_time) AS datetime2(3)) AS _commit_ts" in changes_sql
    )
    assert "AT TIME ZONE" not in changes_sql  # no per-row conversion


def test_timezone_auto_is_detected_once_per_client():
    rec = Recorder("2026-09-28T16:50:00.123", tz="E. South America Standard Time")
    client = SqlCdcClient(rec)
    client.lsn_to_time("0x01")
    list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert [sql for sql, _ in rec.calls].count("SELECT CURRENT_TIMEZONE_ID()") == 1
    assert "AT TIME ZONE N'E. South America Standard Time') AT TIME ZONE 'UTC'" in rec.calls[-1][0]


class OldServer(Recorder):
    """SQL Server 2016-2019: no CURRENT_TIMEZONE_ID(); its current UTC offset is UTC-3."""

    named = 0

    def scalar(self, sql, params=()):
        if "SYSDATETIMEOFFSET" in sql:
            self.calls.append((sql, tuple(params)))
            return -180
        return super().scalar(sql, params)


def test_timezone_auto_falls_back_to_the_current_offset_before_2022(caplog):
    rec = OldServer("2026-09-28T16:50:00.123")
    client = SqlCdcClient(rec)
    with caplog.at_level("WARNING", logger="mssql_cdc.client"):
        client.lsn_to_time("0x01")
        list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert client.timezone == "UTC-03:00" and client.clock() == (None, -180)
    assert "CAST(DATEADD(minute, 180, m.tran_end_time) AS datetime2(3))" in rec.calls[-1][0]
    sqls = [sql for sql, _ in rec.calls]
    assert sqls.count("SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET())") == 1
    # decided by the version: the function name does not compile there, even unused
    assert sum("SERVERPROPERTY('ProductMajorVersion')" in s for s in sqls) == 1
    assert not any("CURRENT_TIMEZONE_ID" in s for s in sqls)
    warned = [r.getMessage() for r in caplog.records if "daylight saving" in r.getMessage()]
    assert len(warned) == 1 and "set sourceTimeZone" in warned[0]


def test_the_fallback_offset_is_read_again_when_the_driver_refreshes_its_clock(caplog):
    class Changing(OldServer):  # UTC-3, then UTC-2: daylight saving began meanwhile
        def __init__(self):
            super().__init__()
            self.offsets = [-180, -120]

        def scalar(self, sql, params=()):
            if "SYSDATETIMEOFFSET" in sql:
                return self.offsets.pop(0)
            return super().scalar(sql, params)

    rec = Changing()
    client = SqlCdcClient(rec)
    assert client.clock() == (None, -180)
    with caplog.at_level("WARNING", logger="mssql_cdc.client"):
        client.refresh_clock()  # the driver, for each new batch
    assert client.clock() == (None, -120) and client.timezone == "UTC-02:00"
    assert any("changed from -180 to -120 minutes" in r.getMessage() for r in caplog.records)
    list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert "CAST(DATEADD(minute, 120, m.tran_end_time) AS datetime2(3))" in rec.calls[-1][0]
    named = Recorder(tz="E. South America Standard Time")
    client = SqlCdcClient(named)
    client.clock()
    sent = len(named.calls)
    client.refresh_clock()  # a named zone converts each commit by its own rules: nothing to read
    assert len(named.calls) == sent


ZONE = "Eastern Standard Time"


class FallBack(Recorder):
    """A range near a fall-back: no single offset over it (``range_offset`` None), and one
    commit the server clock went back to, its LSN and time."""

    def batches(self, sql, params, batch_size):
        self.calls.append((sql, tuple(params)))
        if "LAG(tran_end_time)" in sql:
            yield pa.RecordBatch.from_pylist(
                [{"l": "0x0000002A000001000005", "t": "2025-11-02T01:02:00"}]
            )


def test_a_range_near_a_fall_back_reads_commits_after_the_clock_went_back_with_the_later_offset():
    rec = FallBack(tz=ZONE)
    list(SqlCdcClient(rec, ZONE).iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    (offset_sql, _), (drops_sql, drops_params), (changes_sql, params) = rec.calls[-3:]
    # one offset only with no change within 3 hours of the range: AT TIME ZONE reads a
    # repeated hour with the offset before the change, as it reads the hours before it
    assert (
        f"DATEPART(TZOFFSET, DATEADD(hour, -3, MIN(tran_end_time)) AT TIME ZONE N'{ZONE}') "
        f"= DATEPART(TZOFFSET, DATEADD(hour, 3, MAX(tran_end_time)) AT TIME ZONE N'{ZONE}')"
    ) in offset_sql
    # where the clock went back more than a minute, from 3 hours before the range up to its end
    assert "LAG(tran_end_time) OVER (ORDER BY start_lsn)" in drops_sql
    assert (
        "WHERE start_lsn > b.lo AND start_lsn <= CONVERT(binary(10), ?, 1)) d "
        "WHERE d.t < DATEADD(minute, -1, d.prev)"
    ) in drops_sql
    assert drops_params == ("0x01", "0x02", "0x02")
    later = f"DATEPART(TZOFFSET, DATEADD(hour, 3, m.tran_end_time) AT TIME ZONE N'{ZONE}')"
    assert (
        "CASE WHEN EXISTS (SELECT 1 FROM (SELECT * FROM (VALUES (CONVERT(binary(10), ?, 1), "
        "CONVERT(datetime, ?, 126))) v(start_lsn, t)) x WHERE x.start_lsn <= m.start_lsn "
        "AND x.t > DATEADD(hour, -3, m.tran_end_time)) "
        f"THEN CAST(DATEADD(minute, -{later}, m.tran_end_time) AS datetime2(3)) "
        f"ELSE CAST((m.tran_end_time AT TIME ZONE N'{ZONE}') AT TIME ZONE 'UTC' "
        "AS datetime2(3)) END AS _commit_ts"
    ) in changes_sql
    assert params == ("0x0000002A000001000005", "2025-11-02T01:02:00", "0x01", "0x02")


def test_lsn_to_time_looks_for_the_clock_going_back_only_at_a_repeated_time():
    rec = Recorder("2025-11-02T06:02:00", tz=ZONE)
    assert SqlCdcClient(rec, ZONE).lsn_to_time("0x05") == "2025-11-02T06:02:00.000"
    [(sql, params)] = rec.calls  # one query, as sys.fn_cdc_map_lsn_to_time's was
    assert params == ("0x05",)
    assert sql.endswith(
        "FROM cdc.lsn_time_mapping m WHERE m.start_lsn = CONVERT(binary(10), ?, 1) "
        "ORDER BY m.start_lsn DESC"
    )
    # its own 3 hours, or nothing (above its own LSN) when its time does not repeat
    before = (
        "COALESCE((SELECT TOP (1) start_lsn FROM cdc.lsn_time_mapping "
        "WHERE tran_end_time <= DATEADD(hour, -3, m.tran_end_time) "
        "ORDER BY tran_end_time DESC), 0x00000000000000000000)"
    )
    assert f"THEN {before} ELSE m.start_lsn END AS lo) b CROSS APPLY (" in sql
    assert "WHERE start_lsn > b.lo AND start_lsn <= m.start_lsn) d" in sql


def test_a_clock_set_from_the_drivers_client_is_not_detected_again():
    zone = "E. South America Standard Time"
    driver = SqlCdcClient(Recorder(tz=zone))
    assert driver.clock() == (zone, None)
    for clock, conversion in [
        (driver.clock(), f"AT TIME ZONE N'{zone}') AT TIME ZONE 'UTC'"),
        ((None, -180), "CAST(DATEADD(minute, 180, m.tran_end_time) AS datetime2(3))"),
    ]:
        rec = Recorder(tz="UTC")  # what detection would find: not used
        task = SqlCdcClient(rec)
        task.set_clock(*clock)
        list(task.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
        assert conversion in rec.calls[-1][0]
        assert not any("SERVERPROPERTY" in s or "CURRENT_TIMEZONE_ID" in s for s, _ in rec.calls)
    with pytest.raises(ValueError, match="Invalid sourceTimeZone"):  # inlined: validated again
        SqlCdcClient(Recorder()).set_clock("UTC'; DROP TABLE x --", None)


def test_time_to_lsn_maps_utc_to_the_server_clock_to_the_second():
    at = datetime(2026, 9, 28, 16, 50, 0, 999000)  # UTC; a later second would round up
    named = Recorder("0x2a000001000001", tz="E. South America Standard Time")
    assert SqlCdcClient(named).time_to_lsn(at) == "0x0000002A000001000001"
    sql, params = named.calls[-1]
    local = (
        "CONVERT(datetime2(0), ({} AT TIME ZONE 'UTC') "
        "AT TIME ZONE N'E. South America Standard Time')"
    )
    utc_at = "CONVERT(datetime2(0), ?, 126)"
    assert sql == (  # the earlier of as_of's local time and the next hour's less the hour
        "SELECT CONVERT(varchar(22), sys.fn_cdc_map_time_to_lsn(N'largest less than or equal', "
        f"(SELECT MIN(v) FROM (VALUES ({local.format(utc_at)}), "
        f"(DATEADD(hour, -1, {local.format(f'DATEADD(hour, 1, {utc_at})')}))) x(v))), 1)"
    )
    assert params == ("2026-09-28T16:50:00",) * 2
    utc = Recorder()
    SqlCdcClient(utc, source_timezone="UTC").time_to_lsn(at)
    assert "'largest less than or equal', CONVERT(datetime2(0), ?, 126)), 1)" in utc.calls[-1][0]

    old = OldServer()  # SQL Server 2016-2019: the current offset, UTC-3
    SqlCdcClient(old).time_to_lsn(at)
    assert "DATEADD(minute, -180, CONVERT(datetime2(0), ?, 126))" in old.calls[-1][0]
    for none in (None, "0x00000000000000000000"):  # no commit at or before it
        assert SqlCdcClient(Recorder(none), source_timezone="UTC").time_to_lsn(at) is None


def test_lsn_to_time_always_carries_milliseconds():
    # style 126 drops ".000" on whole seconds; the offset contract keeps them (ADR 0002)
    whole = SqlCdcClient(Recorder(scalar_value="2026-09-28T16:50:00"), source_timezone="UTC")
    assert whole.lsn_to_time("0x01") == "2026-09-28T16:50:00.000"
    ms = SqlCdcClient(Recorder(scalar_value="2026-09-28T16:50:00.123"), source_timezone="UTC")
    assert ms.lsn_to_time("0x01") == "2026-09-28T16:50:00.123"
    assert (
        SqlCdcClient(Recorder(scalar_value=None), source_timezone="UTC").lsn_to_time("0x01") is None
    )


def test_timezone_detected_names_are_validated():
    with pytest.raises(
        ValueError, match="Invalid sourceTimeZone"
    ):  # detected values are inlined too
        SqlCdcClient(Recorder(tz="UTC'; DROP TABLE x --")).lsn_to_time("0x01")


def test_injection_is_rejected():
    rec = Recorder()
    client = SqlCdcClient(rec)
    with pytest.raises(ValueError, match="Invalid capture instance"):
        list(client.iter_changes("dbo_orders]; DROP TABLE x --", "0x01", "0x02", [], True, 10))
    with pytest.raises(ValueError, match="Invalid column name"):
        list(client.iter_changes("dbo_orders", "0x01", "0x02", ["a]; --"], True, 10))
    with pytest.raises(ValueError, match="Invalid sourceTimeZone"):
        SqlCdcClient(rec, source_timezone="UTC'; DROP")
    assert not rec.calls  # refused before any query was sent


def test_a_capture_instance_takes_any_letter_but_no_bracket_or_control_character():
    from mssql_cdc.client import _check_ident, _check_type

    rec = Recorder()
    client = SqlCdcClient(rec, source_timezone="UTC")
    ci = "dbo_Situação"  # SQL Server's default name for dbo.Situação
    client.split_points(ci, "0x01", "0x02", 2)
    assert "FROM cdc.[dbo_Situação_CT]" in rec.calls[-1][0]
    list(client.iter_changes(ci, "0x01", "0x02", ["id"], True, 10))
    assert "FROM cdc.[dbo_Situação_CT] c " in rec.calls[-1][0]
    client.min_lsn(ci)
    assert rec.calls[-1][1] == (ci,)  # a parameter where T-SQL takes one
    client.min_lsn("a" * 100)  # a sysname holds 100
    for bad in ("dbo_x]; DROP TABLE y --", "dbo_orders\n", "dbo\x00orders", "", "a" * 101):
        with pytest.raises(ValueError, match="Invalid capture instance"):
            list(client.iter_changes(bad, "0x01", "0x02", [], True, 10))
        with pytest.raises(ValueError, match="Invalid capture instance"):
            client.min_lsn(bad)
    for bad in ("int]", "int\n"):
        with pytest.raises(ValueError, match="Invalid SQL type"):
            _check_type(bad)
    for bad in ("UTC]", "UTC\n"):
        with pytest.raises(ValueError, match="Invalid sourceTimeZone"):
            SqlCdcClient(rec, source_timezone=bad)
    with pytest.raises(ValueError, match="Invalid collation"):  # inlined unquoted: ASCII only
        _check_ident("Greek_CI_AS\n", "collation")


def test_min_lsn_zero_means_missing_instance():
    with pytest.raises(ValueError, match="not found"):
        SqlCdcClient(Recorder("0x00000000000000000000")).min_lsn("dbo_orders")


def test_nth_commit_and_normalization():
    rec = Recorder("0x0000002a000001000001")
    assert (
        SqlCdcClient(rec).nth_commit_after("0x0000002A000001000000", 5) == "0x0000002A000001000001"
    )
    assert "TOP (5)" in rec.calls[-1][0]


def test_mssql_python_batches_are_bounded_in_bytes_not_only_rows():
    from types import SimpleNamespace

    from mssql_cdc.client import MssqlPythonBackend

    class Cursor:  # each batch holds the rows asked for, of the next width (bytes per row)
        def __init__(self, widths):
            self.widths, self.asked = list(widths), []

        def execute(self, sql, params):
            pass

        def arrow_batch(self, n):
            self.asked.append(n)
            width = self.widths.pop(0) if self.widths else 0
            return SimpleNamespace(num_rows=n if width else 0, nbytes=n * width)

        def close(self):
            pass

    def asked(batch_size, *widths):
        cursor = Cursor(widths)
        backend = object.__new__(MssqlPythonBackend)
        backend._conn = SimpleNamespace(cursor=lambda: cursor)
        list(backend.batches("SELECT 1", (), batch_size))
        return cursor.asked

    mib = 1024 * 1024
    # a first batch of at most 64 rows; then as many as fit in 64 MiB, up to batch_size
    assert asked(10_000, mib, 100, 100) == [64, 64, 10_000, 10_000]
    assert asked(10, 100) == [10, 10]
    assert asked(10_000, 100 * mib) == [64, 1]  # one row wider than the bound: one a batch


def test_make_client_requires_connection_string():
    with pytest.raises(ValueError, match="connectionString"):
        make_client({"backend": "mssql-python"})
    with pytest.raises(ValueError, match="Unknown backend"):
        make_client({"backend": "jdbc", "connectionString": "x"})


def test_backend_and_client_are_protocols(tmp_path):
    """A backend of one's own needs no base class, and isinstance tells it by its methods;
    both clients are CdcClients; an LSN is a plain str at run time (ADR 0030)."""
    from mssql_cdc.fake import FakeCdcClient

    class Own:
        def batches(self, sql, params, batch_size):
            return iter(())

        def scalar(self, sql, params=()):
            return "0x0000002a000001000001"

        def close(self):
            pass

    assert isinstance(Own(), Backend) and isinstance(Recorder(), Backend)
    assert not isinstance(object(), Backend)
    client = SqlCdcClient(Own(), "UTC")
    lsn = client.max_lsn()
    assert type(lsn) is str and lsn == "0x0000002A000001000001"
    assert isinstance(client, CdcClient) and isinstance(FakeCdcClient(str(tmp_path)), CdcClient)
    assert not isinstance(Own(), CdcClient)
    with pytest.raises(TypeError):
        Backend()


class Rows(Recorder):
    """Returns ``rows`` as one Arrow batch from ``batches()``."""

    def __init__(self, rows):
        super().__init__()
        self.rows = rows

    def batches(self, sql, params, batch_size):
        self.calls.append((sql, tuple(params)))
        return iter([pa.RecordBatch.from_pylist(self.rows)] if self.rows else [])


def _col(ordinal, name, data_type, precision=None, scale=None, length=None, dt_precision=None):
    """One row of sys.sp_cdc_get_captured_columns (the columns the client reads)."""
    return {
        "source_schema": "sales",
        "source_table": "orders",
        "column_ordinal": ordinal,
        "column_id": ordinal,
        "column_name": name,
        "data_type": data_type,
        "character_maximum_length": length,
        "numeric_precision": precision,
        "numeric_scale": scale,
        "datetime_precision": dt_precision,
    }


def test_captured_columns_inferred_from_cdc_metadata():
    rec = Rows(
        [
            _col(2, "amount", "decimal", 18, 2),
            _col(1, "order_id", "int", 10, 0),
            _col(3, "status", "varchar"),
            _col(4, "created at", "datetime2"),
            _col(5, "flag", "bit"),
            _col(6, "qty", "tinyint", 3, 0),
        ]
    )
    ddl = SqlCdcClient(rec).captured_columns("dbo_orders")
    assert ddl == (
        "`order_id` INT, `amount` DECIMAL(18,2), `status` STRING, "
        "`created at` TIMESTAMP_NTZ, `flag` BOOLEAN, `qty` SMALLINT"
    )  # ordinal order
    assert rec.calls[-1] == (
        "EXEC sys.sp_cdc_get_captured_columns @capture_instance = ?",
        ("dbo_orders",),
    )


def test_captured_columns_errors_point_to_columns_option():
    with pytest.raises(ValueError, match="geography.*'columns'"):
        SqlCdcClient(Rows([_col(1, "shape", "geography")])).captured_columns("dbo_orders")
    with pytest.raises(ValueError, match="not found"):
        SqlCdcClient(Rows([])).captured_columns("dbo_orders")
    sent = Rows([])
    with pytest.raises(ValueError, match="Invalid capture instance"):
        SqlCdcClient(sent).captured_columns("dbo_orders]; DROP TABLE x --")
    assert not sent.calls

    class Denied(Recorder):
        def batches(self, sql, params, batch_size):
            raise RuntimeError("Object doesn't exist or access is denied.")  # Error 22981

    with pytest.raises(ValueError, match="gating role.*Cause: Object doesn't exist"):
        SqlCdcClient(Denied()).captured_columns("dbo_orders")

    class Dropped(Recorder):  # a transient error says so, first line only
        def batches(self, sql, params, batch_size):
            raise RuntimeError("Communication link failure (08S01)\n[ODBC] details")

    with pytest.raises(ValueError, match=r"Cause: Communication link failure \(08S01\)$"):
        SqlCdcClient(Dropped()).captured_columns("dbo_orders")


def test_change_table_permission_error_names_the_grant():
    class Denied(Recorder):
        """Every read fails with ``message``; HAS_PERMS_BY_NAME answers ``perms``."""

        def __init__(self, message, perms=0):
            super().__init__()
            self.message, self.perms = message, perms

        def batches(self, sql, params, batch_size):
            raise RuntimeError(self.message)

        def scalar(self, sql, params=()):
            if "HAS_PERMS_BY_NAME" not in sql:
                return super().scalar(sql, params)
            self.calls.append((sql, tuple(params)))
            if isinstance(self.perms, Exception):
                raise self.perms
            return self.perms

    english = (
        "[SQL Server]The SELECT permission was denied on the object "
        "'dbo_orders_CT', database 'db', schema 'cdc'."
    )
    # another language, as mssql-python words it: no error number in the text, so SQL
    # Server is asked
    localized = (
        "Driver Error: Syntax error or access violation; DDBC Error: [Microsoft][SQL Server]"
        "A permissão SELECT foi negada no objeto 'dbo_orders_CT', banco de dados 'db', "
        "esquema 'cdc'."
    )
    for message in (english, localized):
        backend = Denied(message)
        client = SqlCdcClient(backend, source_timezone="UTC")
        grant = r"GRANT SELECT ON cdc\.\[dbo_orders_CT\]"
        with pytest.raises(PermissionError, match=grant + ".*own grant"):
            list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
        with pytest.raises(PermissionError, match=grant):
            client.split_points("dbo_orders", "0x01", "0x02", 4)
        assert backend.calls[-1][1] == ("dbo_orders", "cdc.[dbo_orders_CT]")
    for other in (
        Denied("Invalid object name 'cdc.dbo_orders_CT'.", perms=None),  # the instance is gone
        Denied("The SELECT permission was denied on 'dbo_orders_CT'", RuntimeError("link")),
        Denied("Communication link failure (08S01)"),  # names no table: nothing asked
    ):
        with pytest.raises(RuntimeError, match=other.message[:20]) as raised:
            list(SqlCdcClient(other).iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
        assert not isinstance(raised.value, PermissionError)
    assert not [c for c in other.calls if "HAS_PERMS_BY_NAME" in c[0]]


def _instance(ci, table, start, created=None, schema="dbo"):
    """One row of sys.sp_cdc_help_change_data_capture."""
    return {
        "capture_instance": ci,
        "source_schema": schema,
        "source_table": table,
        "index_column_list": "[id]",
        "start_lsn": start,
        "create_date": created,
    }


class Cdc(Recorder):
    """A server with capture instances: the CDC procedures and sys.columns answer from
    ``instances`` (help rows), ``captured`` (instance -> captured column rows), ``ddl`` (ddl
    history rows) and ``live`` (the source table's sys.columns rows)."""

    def __init__(self, instances, captured, ddl=(), live=()):
        super().__init__(scalar_value="2026-09-30T13:25:00", tz="UTC")
        self.instances, self.captured, self.ddl, self.live = instances, captured, ddl, live

    def batches(self, sql, params, batch_size):
        self.calls.append((sql, tuple(params)))
        if "sp_cdc_help_change_data_capture" in sql:  # @source_schema, @source_name: one table
            rows = [
                r for r in self.instances if params in ((), (r["source_schema"], r["source_table"]))
            ]
        elif "sp_cdc_get_captured_columns" in sql:
            rows = self.captured.get(params[0], [])
        elif "sp_cdc_get_ddl_history" in sql:
            rows = [r for r in self.ddl if r["capture_instance"] == params[0]]
        else:
            rows = self.live if "sys.columns" in sql else []
        return iter([pa.RecordBatch.from_pylist(rows)] if rows else [])


V1, V2 = "0x0000002A000001000100", "0x0000002A000002000100"


def _bin(lsn):  # the help listing returns start_lsn as binary(10)
    return bytes.fromhex(lsn[2:])


ORDERS = [
    _instance("dbo_orders_v2", "orders", _bin(V2), datetime(2026, 9, 30, 13, 25)),
    _instance("dbo_other", "other", _bin(V1), datetime(2026, 9, 1)),
    _instance("dbo_orders", "orders", _bin(V1), datetime(2026, 9, 1, 10)),
]
CAPTURED = {
    "dbo_orders": [
        _col(1, "id", "int", 10, 0),
        _col(2, "amount", "decimal", 9, 2),
        _col(3, "g", "geography"),
    ],
    "dbo_orders_v2": [
        _col(1, "id", "int", 10, 0),
        _col(2, "amount", "decimal", 18, 4),
        _col(3, "note", "varchar", length=10),
    ],
    "dbo_other": [_col(1, "id", "int", 10, 0)],
}


def test_capture_instances_lists_the_table_s_instances_oldest_first():
    from mssql_cdc.client import CaptureInstance

    rec = Cdc(ORDERS, CAPTURED)
    both = [
        CaptureInstance(
            "dbo_orders",
            V1,
            ["id", "amount", "g"],
            ["INT", "DECIMAL(9,2)", None],  # geography: fine unless the query reads it
        ),
        CaptureInstance(
            "dbo_orders_v2",
            V2,
            ["id", "amount", "note"],
            ["INT", "DECIMAL(18,4)", "STRING"],
        ),
    ]
    assert SqlCdcClient(rec).capture_instances("DBO_ORDERS") == both
    # one listing (the documented API, invariant 11), then each instance's captured columns
    assert [c for c in rec.calls if "sp_cdc" in c[0]] == [
        ("EXEC sys.sp_cdc_help_change_data_capture", ()),
        ("EXEC sys.sp_cdc_get_captured_columns @capture_instance = ?", ("dbo_orders",)),
        ("EXEC sys.sp_cdc_get_captured_columns @capture_instance = ?", ("dbo_orders_v2",)),
    ]
    assert SqlCdcClient(Cdc(ORDERS, CAPTURED)).capture_instances("dbo_orders_v2") == both
    # a tie on start_lsn (cleanup moves every instance's): the later create_date is newer
    tied = [dict(r, start_lsn=_bin(V2)) for r in ORDERS]
    names = [i.name for i in SqlCdcClient(Cdc(tied, CAPTURED)).capture_instances("dbo_orders")]
    assert names == ["dbo_orders", "dbo_orders_v2"]
    sent = len(rec.calls)
    with pytest.raises(ValueError, match="Invalid capture instance"):
        SqlCdcClient(rec).capture_instances("dbo_orders]; DROP TABLE x --")
    assert len(rec.calls) == sent


def test_captured_columns_are_read_once_per_instance_until_forgotten():
    def fetched(rec):
        return [c[1][0] for c in rec.calls if "sp_cdc_get_captured_columns" in c[0]]

    rec = Cdc(ORDERS, CAPTURED)
    client = SqlCdcClient(rec)
    client.capture_instances("dbo_orders")
    client.capture_instances("dbo_orders")  # the next planning: the listing only
    assert fetched(rec) == ["dbo_orders", "dbo_orders_v2"]
    assert sum("sys.columns" in c[0] for c in rec.calls) == 1  # is_computed: cached with them
    # ALTER COLUMN changes a captured type: the reader forgets them when a batch holds DDL
    rec.captured = {**CAPTURED, "dbo_orders_v2": [_col(1, "id", "bigint", 19, 0)]}
    client.forget_columns()
    assert client.capture_instances("dbo_orders")[1].column_types == ["BIGINT"]
    # an instance dropped and created again under its name is another one: its own columns
    rec.instances = [dict(r, create_date=datetime(2026, 10, 1)) for r in ORDERS]
    client.capture_instances("dbo_orders")
    assert fetched(rec) == ["dbo_orders", "dbo_orders_v2"] * 3


def test_planning_lists_the_resolved_table_only_until_it_misses_the_name():
    full = ("EXEC sys.sp_cdc_help_change_data_capture", ())

    def one(table):
        sql = "EXEC sys.sp_cdc_help_change_data_capture @source_schema = ?, @source_name = ?"
        return sql, ("dbo", table)

    class Server(Cdc):  # a table that does not exist: the procedure raises (error 22981)
        def batches(self, sql, params, batch_size):
            tables = {(r["source_schema"], r["source_table"]) for r in self.instances}
            if "sp_cdc_help" in sql and params and tuple(params) not in tables:
                self.calls.append((sql, tuple(params)))
                raise RuntimeError("Object doesn't exist or access is denied.")
            return super().batches(sql, params, batch_size)

    rec = Server(ORDERS, CAPTURED)
    client = SqlCdcClient(rec)
    both = ["dbo_orders", "dbo_orders_v2"]
    assert [i.name for i in client.capture_instances("dbo_orders")] == both
    assert [i.name for i in client.capture_instances("DBO_ORDERS_V2")] == both  # ignoring case
    # renamed: the old name lists nothing, the database lists it under the new one
    rec.instances = [
        dict(r, source_table="orders2") if "orders" in r["source_table"] else r for r in ORDERS
    ]
    assert [i.name for i in client.capture_instances("dbo_orders")] == both
    assert client.source_table("dbo_orders").table == "orders2"
    assert [i.name for i in client.capture_instances("dbo_other")] == ["dbo_other"]  # not there
    assert [c for c in rec.calls if "sp_cdc_help" in c[0]] == [
        full,
        one("orders"),
        one("orders"),
        full,
        one("orders2"),
        one("orders2"),
        full,
    ]


def test_a_disabled_capture_instance_is_followed_to_its_table():
    listed = [r for r in ORDERS if r["capture_instance"] != "dbo_orders"]  # disabled
    client = SqlCdcClient(Cdc(listed, CAPTURED))
    # its default name (<schema>_<table>) still tells the table
    assert [i.name for i in client.capture_instances("dbo_orders")] == ["dbo_orders_v2"]
    assert client.source_table("dbo_orders") == ("dbo", "orders", ["id"], V2)
    # a custom name does not: the error names the instances that may be its table's
    with pytest.raises(ValueError, match=r"not found.*'dbo_orders_v2'.*set captureInstance"):
        client.capture_instances("dbo_orders_old")
    with pytest.raises(ValueError, match=r"not found[^']*\.$"):
        client.capture_instances("sales_items")


def test_ddl_history_keeps_the_batch_range():
    def ddl(lsn, command, rcu=False):
        return {
            "source_schema": "dbo",
            "source_table": "orders",
            "capture_instance": "dbo_orders",
            "required_column_update": rcu,
            "ddl_command": command,
            "ddl_lsn": bytes.fromhex(lsn),  # binary, like start_lsn in the help listing
            "ddl_time": datetime(2026, 9, 30, 13, 25),
        }

    rec = Cdc(
        ORDERS,
        CAPTURED,
        ddl=[
            ddl(
                "0000002A000001000300",
                "ALTER TABLE dbo.orders ALTER COLUMN [amount] dec(18,4)",
                True,
            ),
            ddl("0000002A000001000200", "ALTER TABLE dbo.orders ADD note varchar(10) NULL"),
            ddl("0000002A000001000100", "ALTER TABLE dbo.orders DROP COLUMN g"),  # = from
            ddl("0000002A000001000400", "ALTER TABLE dbo.orders ADD CONSTRAINT d DEFAULT 0 FOR x"),
            ddl("0000002A000001000500", "ALTER TABLE dbo.orders ALTER COLUMN id bigint", True),
        ],
    )
    client = SqlCdcClient(rec)
    changes = client.ddl_history("dbo_orders", "0x0000002A000001000100", "0x0000002A000001000400")
    assert [c.lsn for c in changes] == [
        "0x0000002A000001000200",
        "0x0000002A000001000300",
        "0x0000002A000001000400",  # (from, to]: from out, to in
    ]
    assert changes[0].command == "ALTER TABLE dbo.orders ADD note varchar(10) NULL"
    assert changes[0].commit_ts == "2026-09-30T13:25:00.000"  # the commit at or before it, UTC
    # not fn_cdc_map_lsn_to_time: a DDL's LSN is no commit's, and it returns NULL for it
    commit_time = (
        "SELECT TOP (1) CONVERT(varchar(23), CAST(m.tran_end_time AS datetime2(3)), 126) "
        "FROM cdc.lsn_time_mapping m WHERE m.start_lsn <= CONVERT(binary(10), ?, 1) "
        "ORDER BY m.start_lsn DESC"
    )
    assert (commit_time, ("0x0000002A000001000200",)) in rec.calls
    assert ("EXEC sys.sp_cdc_get_ddl_history @capture_instance = ?", ("dbo_orders",)) in rec.calls
    sent = len(rec.calls)
    with pytest.raises(ValueError, match="Invalid capture instance"):
        client.ddl_history("dbo_orders]; DROP TABLE x --", "0x01", "0x02")
    assert len(rec.calls) == sent


def test_present_columns_match_the_source_table_by_column_id():
    def col(ordinal, name, column_id):
        return {**_col(ordinal, name, "int", 10, 0), "column_id": column_id}

    captured = {
        "dbo_orders": [col(1, "id", 1), col(2, "b", 2), col(3, "c", 3)],
        "dbo_orders_v2": [col(1, "id", 1), col(2, "c", 3), col(3, "d", 5), col(4, "t", 8)],
    }
    live = [  # b dropped and added again: another column_id; e never captured, not read
        {"name": "id", "column_id": 1, "is_computed": False},
        {"name": "c", "column_id": 3, "is_computed": False},
        {"name": "d", "column_id": 5, "is_computed": False},
        {"name": "b", "column_id": 6, "is_computed": False},
        {"name": "e", "column_id": 7, "is_computed": False},
        {"name": "t", "column_id": 8, "is_computed": True},  # NULL in every change row
    ]
    rec = Cdc([r for r in ORDERS if r["source_table"] == "orders"], captured, live=live)
    got = SqlCdcClient(rec).present_columns("dbo_orders", ["ID", "b", "c", "d", "e", "t", "x"])
    assert got == ["ID", "c", "d"]
    assert rec.calls[-1] == (
        (
            "SELECT name, column_id, is_computed FROM sys.columns "
            "WHERE object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))"
        ),
        ("dbo", "orders"),
    )


def test_computed_columns_are_told_by_sys_columns_and_left_out_of_the_schema(monkeypatch, caplog):
    from mssql_cdc.client import CaptureInstance

    captured = {"dbo_other": [_col(1, "id", "int", 10, 0), _col(2, "total", "int", 10, 0)]}
    live = [  # by column_id: total, captured as column 2, is computed now
        {"name": "id", "column_id": 1, "is_computed": False},
        {"name": "total", "column_id": 2, "is_computed": True},
    ]
    rec = Cdc([ORDERS[1]], captured, live=live)
    assert SqlCdcClient(rec).capture_instances("dbo_other") == [
        CaptureInstance("dbo_other", V1, ["id"], ["INT"], ("total",))
    ]
    # the table's sys.columns, which a login that can SELECT it sees (invariant 11)
    assert [c for c in rec.calls if "sys.columns" in c[0]] == [
        (
            (
                "SELECT name, column_id, is_computed FROM sys.columns "
                "WHERE object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))"
            ),
            ("dbo", "other"),
        )
    ]
    with caplog.at_level("WARNING", logger="mssql_cdc.source"):
        schema = _load(monkeypatch, rec, captureInstance="dbo_other")
    assert schema.endswith("_commit_ts TIMESTAMP_NTZ, `id` INT")
    assert "dbo_other captures computed column(s) total" in caplog.text


def _load(monkeypatch, backend, **options):
    import mssql_cdc.client as client_mod
    from mssql_cdc.source import MssqlCdcDataSource

    client = SqlCdcClient(backend)
    closed = []
    client.close = lambda: closed.append(True)
    monkeypatch.setattr(client_mod, "make_client", lambda options: client)
    try:
        return MssqlCdcDataSource({"captureInstance": "dbo_orders", **options}).schema()
    finally:
        assert closed or "columns" in options  # an inferred schema closes its connection


def test_schema_infers_columns_when_option_missing(monkeypatch):
    backend = Cdc([ORDERS[1]], {"dbo_other": [_col(1, "order_id", "int", 10, 0)]})
    schema = _load(monkeypatch, backend, captureInstance="dbo_other")
    assert schema.endswith("_commit_ts TIMESTAMP_NTZ, `order_id` INT")
    explicit = _load(monkeypatch, backend, columns="id BIGINT")
    assert explicit.endswith(", id BIGINT")


def test_schema_is_the_union_of_the_capture_instances(monkeypatch):
    from mssql_cdc import SchemaChangedError

    captured = {**CAPTURED, "dbo_orders": CAPTURED["dbo_orders"][:2]}
    # by name in capture order, older first; a wider type in the newer instance wins
    assert _load(monkeypatch, Cdc(ORDERS, captured)).endswith(
        "_commit_ts TIMESTAMP_NTZ, `id` INT, `amount` DECIMAL(18,4), `note` STRING"
    )
    narrower = {**captured, "dbo_orders_v2": [_col(1, "amount", "decimal", 9, 3)]}
    with pytest.raises(SchemaChangedError, match=r"'amount' as DECIMAL\(9,2\) and DECIMAL\(9,3\)"):
        _load(monkeypatch, Cdc(ORDERS, narrower))
    with pytest.raises(ValueError, match="geography.*'columns'"):  # a column the query reads
        _load(monkeypatch, Cdc(ORDERS, CAPTURED))


def test_fake_backend_still_requires_columns(tmp_path):
    from mssql_cdc.fake import FakeCdcDatabase
    from mssql_cdc.source import MssqlCdcDataSource

    FakeCdcDatabase(str(tmp_path), ["dbo_orders"])  # no captured columns declared
    ds = MssqlCdcDataSource(
        {"backend": "fake", "fakePath": str(tmp_path), "captureInstance": "dbo_orders"}
    )
    with pytest.raises(ValueError, match="'columns' is required"):
        ds.schema()


def test_ping_and_network_wait():
    rec = Recorder(scalar_value=1234)
    client = SqlCdcClient(rec, source_timezone="UTC")
    assert len(client.ping(3)) == 3 and rec.calls[-1] == ("SELECT 1", ())
    assert client.network_wait_ms() == 1234
    assert (
        "sys.dm_exec_session_wait_stats WHERE session_id = @@SPID AND wait_type = 'ASYNC_NETWORK_IO'"
        in rec.calls[-1][0]
    )

    class NoView(Recorder):
        def scalar(self, sql, params=()):
            if "dm_exec_session_wait_stats" in sql:
                raise RuntimeError("VIEW SERVER STATE permission was denied")
            return super().scalar(sql, params)

    assert SqlCdcClient(NoView(), source_timezone="UTC").network_wait_ms() is None  # never raises


def test_split_points_tile_the_change_rows_of_the_capture_instance():
    rec = Rows(
        [
            {"b": "0x0000002a000001000001", "n": "0x0000002a000001000002", "r": 3},
            {"b": "0x0000002a000001000003", "n": "0x0000002a000001000004", "r": 2},
        ]
    )
    points = SqlCdcClient(rec, source_timezone="UTC").split_points("dbo_orders", "0x01", "0x02", 4)
    # each bound with the LSN after it, where the next range starts (no query per bound), and
    # its tile's rows, by which small tiles are merged
    assert points == [
        ("0x0000002A000001000001", "0x0000002A000001000002", 3),
        ("0x0000002A000001000003", "0x0000002A000001000004", 2),
    ]
    assert len(rec.calls) == 1
    sql, params = rec.calls[-1]
    assert sql == (
        "SELECT CONVERT(varchar(22), MAX(__$start_lsn), 1) AS b, "
        "CONVERT(varchar(22), sys.fn_cdc_increment_lsn(MAX(__$start_lsn)), 1) AS n, "
        "COUNT_BIG(*) AS r FROM ("
        "SELECT __$start_lsn, NTILE(4) OVER (ORDER BY __$start_lsn) AS g "
        "FROM cdc.[dbo_orders_CT] "
        "WHERE __$start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)"
        ") x GROUP BY g ORDER BY b"
    )
    assert params == ("0x01", "0x02")


def test_source_table_matches_the_capture_instance_ignoring_case():
    def row(ci, table):
        return {
            "capture_instance": ci,
            "source_schema": "dbo",
            "source_table": table,
            "index_column_list": "[id]",
            "start_lsn": "0x0000002a000001000001",
        }

    # stored upper-case, configured lower-case: the CDC functions resolve it, so does this
    client = SqlCdcClient(Rows([row("dbo_ORDER_ITEMS", "ORDER_ITEMS")]))
    assert client.source_table("dbo_order_items").table == "ORDER_ITEMS"
    # a case-sensitive database can hold both: the exact name wins, else neither does
    client = SqlCdcClient(Rows([row("dbo_Orders", "Orders"), row("dbo_orders", "orders")]))
    assert client.source_table("dbo_orders").table == "orders"
    with pytest.raises(ValueError, match="matches 'dbo_Orders', 'dbo_orders' ignoring case"):
        client.source_table("DBO_ORDERS")


def test_source_table_takes_the_key_and_first_lsn_from_the_help_listing():
    class Help(Recorder):
        def batches(self, sql, params, batch_size):
            self.calls.append((sql, tuple(params)))
            if "sp_cdc_help_change_data_capture" in sql:
                return iter(
                    [
                        pa.RecordBatch.from_pylist(
                            [
                                {
                                    "capture_instance": "dbo_other",
                                    "source_schema": "dbo",
                                    "source_table": "other",
                                    "index_column_list": None,
                                    "start_lsn": None,
                                },
                                {
                                    "capture_instance": "dbo_orders",
                                    "source_schema": "sales",
                                    "source_table": "orders",
                                    "index_column_list": "[order_id], [line]",
                                    "start_lsn": "0x0000002a000001000001",
                                },
                            ]
                        )
                    ]
                )
            return iter(())

    rec = Help()
    client = SqlCdcClient(rec)
    assert client.source_table("dbo_orders") == (
        "sales",
        "orders",
        ["order_id", "line"],
        "0x0000002A000001000001",
    )
    assert rec.calls[-1] == ("EXEC sys.sp_cdc_help_change_data_capture", ())
    with pytest.raises(ValueError, match="not found"):
        client.source_table("dbo_missing")


def test_key_range_is_two_seeks_and_none_on_an_empty_table():
    rec = Recorder()
    assert SqlCdcClient(rec).key_range("sales", "orders", "order_id") == (None, None)
    sql = (
        "SELECT (SELECT MIN([order_id]) FROM [sales].[orders]) AS lo, "
        "(SELECT MAX([order_id]) FROM [sales].[orders]) AS hi"
    )
    assert rec.calls == [(sql, ())]


def _where(lo, hi, keys=("order_id",), types=None):
    """The query iter_table sends for [lo, hi) past its SELECT, and its parameters."""
    rec = Recorder()
    cols = ["order_id", "status"]
    list(SqlCdcClient(rec).iter_table("sales", "orders", cols, keys, types, lo, hi, 100))
    [(sql, params)] = rec.calls
    return sql.removeprefix("SELECT [order_id], [status] FROM [sales].[orders]"), params


# a composite key: bounds bound as text and CAST to the declared types and collations, the
# range cut into seekable pieces (an equality prefix and one range each) joined by UNION ALL
_S = " UNION ALL SELECT [order_id], [status] FROM [sales].[orders]"
_KEYS, _TYPES = ("region", "id"), ("varchar(10) COLLATE Greek_CI_AS", "int")
_V, _I = "CAST(? COLLATE Greek_CI_AS AS varchar(10))", "CAST(? AS int)"
# (region, id) < (u, v), row by row
_BELOW = (
    f"(([region] < {_V} OR [region] IS NULL)"
    f" OR ([region] = {_V} AND ([id] < {_I} OR [id] IS NULL)))"
)
_AT = datetime(2026, 9, 28, 10, 0, 0, 6667)
_BELOW3 = (  # (a, b, c) < (4, 5, 6)
    "(([a] < 4 OR [a] IS NULL) OR ([a] = 4 AND ([b] < 5 OR [b] IS NULL)) "
    "OR ([a] = 4 AND [b] = 5 AND ([c] < 6 OR [c] IS NULL)))"
)


@pytest.mark.parametrize(
    ("lo", "hi", "keys", "types", "where", "params"),
    [
        # one integer key: inlined bounds, no parameters
        (None, None, ("order_id",), None, "", ()),
        (None, None, (), None, "", ()),
        (None, (10,), ("order_id",), None, " WHERE ([order_id] < 10 OR [order_id] IS NULL)", ()),
        (
            (10,),
            (20,),
            ("order_id",),
            None,
            " WHERE [order_id] >= 10 AND ([order_id] < 20 OR [order_id] IS NULL)",
            (),
        ),
        ((20,), None, ("order_id",), None, " WHERE [order_id] >= 20", ()),
        # inside one leading value: one piece, one seek
        (
            ("n", 5),
            ("n", 9),
            _KEYS,
            _TYPES,
            f" WHERE [region] = {_V} AND [id] >= {_I} AND ([id] < {_I} OR [id] IS NULL)",
            ("n", "5", "9"),
        ),
        # text CAST reads back exactly (arrow-odbc binds nothing else): datetime takes 3
        # digits, a zero decimal is no '0E-10', binary goes as hex like an LSN
        (
            (_AT, Decimal("0E-10"), b"\n\x0b"),
            None,
            ("t", "m", "b"),
            ("datetime", "decimal(18,10)", "varbinary(4)"),
            (
                " WHERE [t] = CAST(? AS datetime) AND [m] = CAST(? AS decimal(18,10)) "
                "AND [b] >= CONVERT(varbinary(4), ?, 1)"
                f"{_S} WHERE [t] = CAST(? AS datetime) AND [m] > CAST(? AS decimal(18,10))"
                f"{_S} WHERE [t] > CAST(? AS datetime)"
            ),
            ("2026-09-28T10:00:00.006", "0.0000000000", "0x0a0b")
            + ("2026-09-28T10:00:00.006", "0.0000000000", "2026-09-28T10:00:00.006"),
        ),
        # across leading values: the rest of 'n', what lies between, the start of 's'. The
        # first also checks < ('s', 1) and the last > 'n': both hold unless 'n' = 's' in SQL
        # (a case-insensitive 'n' and 'N'), and then the first piece alone reads the range
        (
            ("n", 5),
            ("s", 1),
            _KEYS,
            _TYPES,
            (
                f" WHERE [region] = {_V} AND [id] >= {_I} AND {_BELOW}"
                f"{_S} WHERE [region] > {_V} AND ([region] < {_V} OR [region] IS NULL)"
                f"{_S} WHERE [region] = {_V} AND ([id] < {_I} OR [id] IS NULL) AND [region] > {_V}"
            ),
            ("n", "5", "s", "s", "1", "n", "s", "s", "1", "n"),  # in the order of the ? marks
        ),
        (
            None,
            ("s", 1),
            _KEYS,
            _TYPES,
            (
                f" WHERE ([region] < {_V} OR [region] IS NULL)"
                f"{_S} WHERE [region] = {_V} AND ([id] < {_I} OR [id] IS NULL)"
            ),
            ("s", "s", "1"),
        ),
        (
            ("n", 5),
            None,
            _KEYS,
            _TYPES,
            f" WHERE [region] = {_V} AND [id] >= {_I}{_S} WHERE [region] > {_V}",
            ("n", "5", "n"),
        ),
        # three columns: the deepest piece first, integers inlined
        (
            (1, 2, 3),
            (4, 5, 6),
            ("a", "b", "c"),
            None,
            (
                f" WHERE [a] = 1 AND [b] = 2 AND [c] >= 3 AND {_BELOW3}"
                f"{_S} WHERE [a] = 1 AND [b] > 2 AND {_BELOW3}"
                f"{_S} WHERE [a] > 1 AND ([a] < 4 OR [a] IS NULL)"
                f"{_S} WHERE [a] = 4 AND ([b] < 5 OR [b] IS NULL) AND [a] > 1"
                f"{_S} WHERE [a] = 4 AND [b] = 5 AND ([c] < 6 OR [c] IS NULL) AND [a] > 1"
            ),
            (),
        ),
        # NULL bounds (defensive: SQL Server refuses a CDC index over nullable columns),
        # sorting first as in ORDER BY
        (
            (None, 5),
            (None, 9),
            _KEYS,
            _TYPES,
            f" WHERE [region] IS NULL AND [id] >= {_I} AND ([id] < {_I} OR [id] IS NULL)",
            ("5", "9"),
        ),
        (
            ("n", None),
            None,
            _KEYS,
            _TYPES,
            f" WHERE [region] = {_V} AND 1 = 1{_S} WHERE [region] > {_V}",
            ("n", "n"),
        ),
    ],
    ids=[
        "open",
        "no-key",
        "below",
        "between",
        "from",
        "one-leading-value",
        "text-cast",
        "across-leading-values",
        "composite-below",
        "composite-from",
        "three-columns",
        "null-leading",
        "null-trailing",
    ],
)
def test_a_snapshot_range_reads_seekable_pieces(lo, hi, keys, types, where, params):
    assert _where(lo, hi, keys, types) == (where, params)


@pytest.mark.parametrize(
    ("lo", "keys", "types", "error"),
    [
        (("1; DROP TABLE x",), ("order_id",), None, r"invalid literal for int\(\)"),  # inlined
        (None, ("a]; DROP TABLE x --",), None, "Invalid column name"),
        (("n",), ("region",), ("int) OR 1=1 --",), "Invalid SQL type"),
        (("n",), ("region",), ("varchar(5) COLLATE x AS int) --",), "Invalid SQL type"),
    ],
    ids=["integer-bound", "key", "type", "collation"],
)
def test_a_snapshot_range_refuses_injection_before_any_query(lo, keys, types, error):
    rec = Recorder()
    with pytest.raises(ValueError, match=error):
        list(SqlCdcClient(rec).iter_table("sales", "orders", ["id"], keys, types, lo, None, 100))
    assert not rec.calls


def test_key_tiles_and_types_for_composite_or_non_integer_keys():
    rec = Rows([{"region": "n", "id": 3}, {"region": "s", "id": 1}])
    client = SqlCdcClient(rec, source_timezone="UTC")
    assert client.key_tiles("sales", "orders", ["region", "id"], 3) == [("n", 3), ("s", 1)]
    sql, params = rec.calls[-1]
    assert (
        sql
        == (
            "SELECT [region], [id] FROM (SELECT [region], [id], [__$tile], "
            "LAG([__$tile]) OVER (ORDER BY [region], [id]) AS [__$prev] FROM ("
            "SELECT [region], [id], NTILE(3) OVER (ORDER BY [region], [id]) AS [__$tile] "
            "FROM [sales].[orders]) a) b WHERE [__$tile] <> [__$prev] ORDER BY [__$tile]"
        )
        and params == ()
    )  # only the first key of tiles 2..n crosses the network; no key column is named __$...
    sent = len(rec.calls)
    with pytest.raises(ValueError, match="Invalid column name"):
        client.key_tiles("sales", "orders", ["id]) a; DROP TABLE x --"], 3)
    with pytest.raises(ValueError, match=r"invalid literal for int\(\)"):  # inlined as one
        client.key_tiles("sales", "orders", ["id"], "3; DROP TABLE x")
    assert len(rec.calls) == sent

    class Meta(Recorder):  # captured columns, then sys.columns
        def __init__(self, collations):
            super().__init__()
            self.collations = collations

        def batches(self, sql, params, batch_size):
            self.calls.append((sql, tuple(params)))
            captured = [
                _col(1, "region", "varchar", length=10),
                _col(2, "code", "nvarchar", length=20),
                _col(3, "amount", "numeric", 18, 2),
                _col(4, "at", "datetime2", dt_precision=7),
                _col(5, "day", "date", dt_precision=0),
                _col(6, "id", "bigint", 19, 0),
                _col(7, "t", "time", dt_precision=7),
                _col(8, "g", "geography"),
                _col(9, "flag", "char", length=1),
            ]
            rows = captured if "sp_cdc_get_captured_columns" in sql else self.collations
            return iter([pa.RecordBatch.from_pylist(rows)])

    meta = Meta([{"name": "region", "collation_name": "Greek_CI_AS"}])
    cols = SqlCdcClient(meta)
    assert cols.key_types(
        "dbo_orders",
        ["region", "code", "amount", "at", "day", "id", "t", "g", "missing", "flag", "at"],
    ) == [
        "varchar(10) COLLATE Greek_CI_AS",  # converted in its own code page, not the default
        "nvarchar(20)",
        "numeric(18,2)",
        None,  # truncated to microseconds ahead of another key column: bounds could swap
        "date",
        "bigint",
        None,  # time(7) comes back as time64[ns]: no Python value to bind
        None,  # no Spark mapping
        None,
        None,  # a char column whose collation the login cannot see
        "datetime2(7)",  # the last key column may be truncated
    ]
    assert meta.calls[-1] == (
        (
            "SELECT name, collation_name FROM sys.columns "
            "WHERE object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))"
        ),
        ("sales", "orders"),
    )
    assert SqlCdcClient(meta).key_types("dbo_orders", ["id", "at"]) == ["bigint", "datetime2(7)"]
    assert "sys.columns" not in meta.calls[-1][0]  # no string key, no collation lookup
    # a collation is inlined unquoted, so the one sys.columns names is checked too
    with pytest.raises(ValueError, match="Invalid collation: 'x; DROP'"):
        SqlCdcClient(Meta([{"name": "region", "collation_name": "x; DROP"}])).key_types(
            "dbo_orders", ["region"]
        )


# -- chunked snapshots (ADR 0028) ----------------------------------------------------
def test_chunk_planning_queries():
    from mssql_cdc.client import _key_select

    keys, types = ["region", "id"], ["varchar(10) COLLATE Greek_CI_AS", "int"]
    rec = Rows([{"region": "n", "id": 3}])
    client = SqlCdcClient(rec, source_timezone="UTC")
    assert client.key_bound("sales", "orders", keys, types, ("a", 1), ("s", 9), 1000) == ("n", 3)
    sql, params = rec.calls[-1]
    # every seekable piece of [lo, hi) takes its own first n + 1 keys, then the (n + 1)-th of
    # their union is the bound: a TOP per seek, never a sort of the range
    k = "[region], [id]"
    assert sql.startswith(
        f"SELECT {k} FROM (SELECT * FROM (SELECT TOP (1001) {k} FROM [sales].[orders] WHERE "
    )
    assert sql.count("SELECT TOP (1001)") == sql.count(f" ORDER BY {k}) p") == 3
    assert sql.endswith(f") u ORDER BY {k} OFFSET 1000 ROWS FETCH NEXT 1 ROWS ONLY")
    assert params == tuple(_key_select("x", keys, types, ("a", 1), ("s", 9))[1])
    empty = Rows([])
    assert SqlCdcClient(empty).key_bound("sales", "orders", ["id"], None, None, (9,), 5) is None
    assert empty.calls[-1][0] == (
        "SELECT [id] FROM (SELECT * FROM (SELECT TOP (6) [id] FROM [sales].[orders] WHERE "
        "([id] < 9 OR [id] IS NULL) ORDER BY [id]) p) u ORDER BY [id] "
        "OFFSET 5 ROWS FETCH NEXT 1 ROWS ONLY"
    )

    top = Rows([{"region": "z", "id": 7}])
    assert SqlCdcClient(top).key_max("sales", "orders", keys) == ("z", 7)
    assert top.calls[-1] == (
        "SELECT TOP (1) [region], [id] FROM [sales].[orders] ORDER BY [region] DESC, [id] DESC",
        (),
    )
    spaced = Rows([{"name": "orders", "rows": "1234                ", "reserved": "80 KB"}])
    assert SqlCdcClient(spaced).row_estimate("sales", "orders") == 1234  # char(20), public
    assert spaced.calls[-1] == ("EXEC sys.sp_spaceused @objname = ?", ("[sales].[orders]",))
    sent = len(rec.calls)
    with pytest.raises(ValueError, match="Invalid column name"):
        client.key_bound("sales", "orders", ["id]) p; DROP TABLE x --"], None, None, None, 5)
    with pytest.raises(ValueError, match=r"invalid literal for int\(\)"):  # inlined as one
        client.key_bound("sales", "orders", ["id"], None, None, None, "5; DROP TABLE x")
    assert len(rec.calls) == sent


def test_a_chunk_reads_under_read_committed_or_snapshot_never_nolock():
    rec = Recorder()
    client = SqlCdcClient(rec, source_timezone="UTC")
    list(client.iter_table("sales", "orders", ["id"], ["id"], None, (1,), None, 10))
    assert rec.calls[-1][0] == "SELECT [id] FROM [sales].[orders] WHERE [id] >= 1"
    list(client.iter_table("sales", "orders", ["id"], ["id"], None, (1,), None, 10, "snapshot"))
    assert rec.calls[-1][0] == (
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT [id] FROM [sales].[orders] WHERE [id] >= 1"
    )
    with pytest.raises(ValueError, match="isolation"):
        list(client.iter_table("sales", "orders", ["id"], [], None, None, None, 10, "uncommitted"))
    assert not any("NOLOCK" in sql or "UNCOMMITTED" in sql for sql, _ in rec.calls)


def test_a_lock_timeout_goes_before_every_read_of_the_source_table_only():
    rec = Recorder()
    client = SqlCdcClient(rec, source_timezone="UTC", lock_timeout_ms=1500)
    reads = [  # the snapshot's, the plans' and reconcile's
        lambda: client.key_range("sales", "orders", "id"),
        lambda: client.key_buckets("sales", "orders", "id", "int", 10, 0, 10),
        lambda: client.key_bound("sales", "orders", ["id"], None, None, (9,), 5),
        lambda: list(client.iter_table("sales", "orders", ["id"], [], None, None, None, 10)),
        lambda: client.key_tiles("sales", "orders", ["a", "b"], 4),
        lambda: client.key_max("sales", "orders", ["a", "b"]),
    ]
    for read in reads:
        read()
        assert rec.calls[-1][0].startswith("SET LOCK_TIMEOUT 1500; SELECT ")
    list(client.iter_table("sales", "orders", ["id"], [], None, None, None, 10, "snapshot"))
    assert rec.calls[-1][0].startswith(
        "SET LOCK_TIMEOUT 1500; SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT "
    )
    rec.calls.clear()
    client.max_lsn()
    list(client.iter_changes("dbo_orders", "0x01", "0x02", ["id"], True, 100))
    assert rec.calls and not any("LOCK_TIMEOUT" in sql for sql, _ in rec.calls)
    plain = Recorder()  # without the option: no SET at all, as before
    SqlCdcClient(plain, source_timezone="UTC").key_max("sales", "orders", ["a"])
    assert plain.calls[-1][0] == "SELECT TOP (1) [a] FROM [sales].[orders] ORDER BY [a] DESC"


def test_lock_timeout_ms_is_a_non_negative_integer(monkeypatch):
    class Connected(Backend):
        def __init__(self, connection_string, timeout):
            self.timeout = timeout

        def batches(self, sql, params, batch_size):
            return iter(())

    monkeypatch.setattr("mssql_cdc.client.MssqlPythonBackend", Connected)
    client = make_client({"connectionString": "Server=x", "LOCKTIMEOUTMS": " 500 "})
    assert client._lock_timeout_ms == 500 and client._b.timeout == 30
    assert make_client({"connectionString": "Server=x", "lockTimeoutMs": "0"})._lock_timeout_ms == 0
    assert make_client({"connectionString": "Server=x"})._lock_timeout_ms is None
    for value in ("-1", "1.5", "soon", ""):
        message = f"lockTimeoutMs must be a non-negative integer (milliseconds), not {value!r}"
        with pytest.raises(ValueError, match=re.escape(message)):
            make_client({"connectionString": "Server=x", "lockTimeoutMs": value})


def test_chunk_planning_counts_and_seeks_under_the_backfills_isolation():
    from mssql_cdc.client import SourceTable, plan_chunks

    rec = Recorder()
    client = SqlCdcClient(rec, source_timezone="UTC")
    t = "[sales].[orders] WHERE [id] >= 0 AND [id] < 10"
    o, w = "CAST([id] AS bigint)", "CAST(10 AS bigint)"
    planning = [
        (
            lambda *i: client.key_range("sales", "orders", "id", 0, 10, *i),
            f"SELECT (SELECT MIN([id]) FROM {t}) AS lo, (SELECT MAX([id]) FROM {t}) AS hi",
        ),
        (
            lambda *i: client.key_buckets("sales", "orders", "id", "int", 10, 0, 10, *i),
            (
                "SELECT [__$b] AS b, COUNT_BIG(*) AS n, SUM(CAST([__$o] AS decimal(38,0))) AS s "
                f"FROM (SELECT {o} AS [__$o], CASE WHEN {o} >= 0 THEN {o} / {w} "
                f"ELSE ({o} + 1) / {w} - 1 END AS [__$b] FROM [sales].[orders] "
                "WHERE [id] IS NOT NULL AND [id] >= 0 AND [id] < 10) x GROUP BY [__$b]"
            ),
        ),
        (
            lambda *i: client.key_bound("sales", "orders", ["id"], None, None, (9,), 5, *i),
            (
                "SELECT [id] FROM (SELECT * FROM (SELECT TOP (6) [id] FROM [sales].[orders] WHERE "
                "([id] < 9 OR [id] IS NULL) ORDER BY [id]) p) u ORDER BY [id] "
                "OFFSET 5 ROWS FETCH NEXT 1 ROWS ONLY"
            ),
        ),
    ]
    for plan, sql in planning:
        plan()
        assert rec.calls[-1][0] == sql  # READ COMMITTED: as before
        plan(None)
        assert rec.calls[-1][0] == sql
        plan("snapshot")  # as the chunks read
        assert rec.calls[-1][0] == "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; " + sql
        with pytest.raises(ValueError, match="isolation"):
            plan("uncommitted")
    assert not any("NOLOCK" in sql or "UNCOMMITTED" in sql for sql, _ in rec.calls)
    # plan_chunks hands it down: an integer plan's count
    source = SourceTable("sales", "orders", ["id"], None)
    extent = {"kind": "int", "lo": 0, "hi": 9, "rows": 0}
    assert plan_chunks(client, "dbo_orders", source, extent, 10, "snapshot") == [[None, 10]]
    assert rec.calls[-1][0].startswith("SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT [__$b]")


def test_every_planning_read_takes_the_source_capture_instance_key_types_and_isolation():
    # the Recorder answers no rows, so a plan stops before most of its reads; these answer
    from unittest.mock import create_autospec

    from mssql_cdc.client import SourceTable, last_bound, plan_chunks, snapshot_plan

    ci, iso = "sales_orders", "snapshot"
    at, top = datetime(2026, 1, 1, 0, 0, 1, 123456), datetime(2026, 1, 1, 0, 0, 2, 654321)
    client = create_autospec(CdcClient, instance=True)
    types = {"at": "datetime", "n": "int", "v": None}  # v: a type no bound is bound as
    client.key_types.side_effect = lambda _, keys: [types[k] for k in keys]
    client.key_max.return_value = (top, 9)
    client.key_bound.side_effect = [(at, 3), None, (top.replace(second=3), 0)]
    source = SourceTable("sales", "orders", ["at", "n"], None)
    extent = snapshot_plan(client, ci, source)
    # bounds as datetime's text: CAST reads no more than 3 digits back
    assert extent == {"kind": "keyset", "max": ["2026-01-01T00:00:02.654", 9]}
    mid, end = ["2026-01-01T00:00:01.123", 3], ["2026-01-01T00:00:03.654", 0]
    assert plan_chunks(client, ci, source, extent, 5, iso) == [[None, mid], [mid, end]]
    assert {c.args[0] for c in client.key_types.call_args_list} == {ci}
    assert client.key_max.call_args.args == ("sales", "orders", ["at", "n"])
    seek = ("sales", "orders", ["at", "n"], ["datetime", "int"])
    assert all((c.args[:4], c.args[-1]) == (seek, iso) for c in client.key_bound.call_args_list)
    # the key after MAX reads back as MAX (a datetime2(7) in MAX's microsecond): left open
    client.key_bound.side_effect = [(top, 9)]
    assert last_bound(client, ci, source, extent["max"], iso) is None
    # no unique index, or a key no bound is bound as: one chunk, the whole table
    for keys in ([], ["at", "v"]):
        whole = snapshot_plan(client, ci, SourceTable("sales", "orders", keys, None))
        assert whole == {"kind": "keyset", "max": None}

    # an integer key whose one slice is counted again, on a finer grid over its own keys
    client = create_autospec(CdcClient, instance=True)
    client.key_range.side_effect = [(0, 99), (5, 9)]
    client.row_estimate.return_value = 0
    client.key_buckets.side_effect = [[(0, 2, None)], [(5, 1, None), (9, 1, None)]]
    source = SourceTable("sales", "orders", ["id"], None)
    extent = snapshot_plan(client, ci, source)
    assert extent == {"kind": "int", "lo": 0, "hi": 99, "rows": 0}
    assert plan_chunks(client, ci, source, extent, 1, iso) == [[None, 9], [9, 100]]
    client.row_estimate.assert_called_once_with("sales", "orders")
    assert [c.args for c in client.key_range.call_args_list] == [
        ("sales", "orders", "id"),
        ("sales", "orders", "id", 0, 100, iso),
    ]
    assert all(c.args[-1] == iso for c in client.key_buckets.call_args_list)


def test_chunk_bounds_cross_json_as_text_cast_reads_back():
    from mssql_cdc.client import _json_key, _key_select, _key_tuple

    at = datetime(2026, 9, 28, 10, 0, 0, 6667)
    assert _json_key((5,), None) == 5 and _json_key((None, "n"), ["int", "char(1)"]) == [None, "n"]
    assert _json_key((at, Decimal("0E-10"), b"\n\x0b"), ["datetime", "decimal(18,10)", "x"]) == [
        "2026-09-28T10:00:00.006",
        "0.0000000000",
        "0x0a0b",
    ]
    assert _json_key((at,), ["datetime2(7)"]) == "2026-09-28T10:00:00.006667"
    assert _key_tuple(None) is None and _key_tuple(5) == (5,) and _key_tuple([1, "a"]) == (1, "a")
    # binary arrives as its hex text and is converted, not CAST from the characters
    sql, params = _key_select("SELECT 1", ["b"], ["varbinary(4)"], ("0x0a0b",), None)
    assert sql == "SELECT 1 WHERE [b] >= CONVERT(varbinary(4), ?, 1)" and params == ["0x0a0b"]


def test_key_buckets_count_and_sum_the_key_per_floored_bucket_in_one_query():
    rec = Rows([{"b": -1, "n": 3, "s": Decimal(-6)}, {"b": 0, "n": 2, "s": Decimal(15)}])
    client = SqlCdcClient(rec, source_timezone="UTC")
    assert client.key_buckets("sales", "orders", "id", "int", 10) == [
        (-1, 3, Decimal(-6)),
        (0, 2, Decimal(15)),
    ]
    sql, params = rec.calls[-1]
    o, w = "CAST([id] AS bigint)", "CAST(10 AS bigint)"
    assert (
        sql
        == (
            "SELECT [__$b] AS b, COUNT_BIG(*) AS n, SUM(CAST([__$o] AS decimal(38,0))) AS s "
            f"FROM (SELECT {o} AS [__$o], CASE WHEN {o} >= 0 THEN {o} / {w} "
            f"ELSE ({o} + 1) / {w} - 1 END AS [__$b] FROM [sales].[orders] WHERE [id] IS NOT NULL) x "
            "GROUP BY [__$b]"
        )
        and params == ()
    )  # floored, as Spark's pmod: -1 is in bucket -1, not 0
    client.key_buckets("sales", "orders", "day", "date", 7)
    assert "CAST(DATEDIFF(day, CAST('19700101' AS date), [day]) AS bigint)" in rec.calls[-1][0]
    client.key_buckets("sales", "orders", None, None, 1)
    assert rec.calls[-1][0] == (
        "SELECT CAST(0 AS bigint) AS b, COUNT_BIG(*) AS n, CAST(NULL AS decimal(38,0)) AS s "
        "FROM [sales].[orders]"
    )
    sent = len(rec.calls)
    with pytest.raises(ValueError, match="Invalid column name"):
        client.key_buckets("sales", "orders", "id]; DROP TABLE x --", "int", 10)
    with pytest.raises(ValueError, match=r"invalid literal for int\(\)"):  # inlined as one
        client.key_buckets("sales", "orders", "id", "int", "10; DROP TABLE x")
    assert len(rec.calls) == sent


def test_an_integer_plan_counts_and_seeks_one_slice_of_the_key():
    rec = Rows([{"b": 0, "n": 2, "s": Decimal(15)}])
    client = SqlCdcClient(rec, source_timezone="UTC")
    client.key_buckets("sales", "orders", "id", "int", 10, -5, 30)  # a seek, not a scan
    assert rec.calls[-1][0].endswith(
        "FROM [sales].[orders] WHERE [id] IS NOT NULL AND [id] >= -5 AND [id] < 30) x "
        "GROUP BY [__$b]"
    )
    client.key_buckets("sales", "orders", "id", "int", 10, None, 30)
    assert "WHERE [id] IS NOT NULL AND [id] < 30) x" in rec.calls[-1][0]
    ranged = Rows([{"lo": 3, "hi": 9}])
    assert SqlCdcClient(ranged).key_range("sales", "orders", "id", 0, 10) == (3, 9)
    t = "[sales].[orders] WHERE [id] >= 0 AND [id] < 10"
    assert ranged.calls[-1] == (
        f"SELECT (SELECT MIN([id]) FROM {t}) AS lo, (SELECT MAX([id]) FROM {t}) AS hi",
        (),
    )
    sent = len(rec.calls)
    with pytest.raises(ValueError, match=r"invalid literal for int\(\)"):  # inlined as one
        client.key_buckets("sales", "orders", "id", "int", 10, "0; DROP TABLE x")
    assert len(rec.calls) == sent
