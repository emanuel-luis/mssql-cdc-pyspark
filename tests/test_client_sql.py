"""SqlCdcClient builds T-SQL; check the generated statements without a server."""

import pyarrow as pa
import pytest

from mssql_cdc.client import Backend, SqlCdcClient, make_client


class Recorder(Backend):
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
        if "CURRENT_TIMEZONE_ID" in sql:
            return self.tz
        if "FROM cdc.lsn_time_mapping" in sql and "TZOFFSET" in sql:
            return self.range_offset
        return self.value


def test_changes_query_shape():
    rec = Recorder()
    client = SqlCdcClient(rec)
    list(client.iter_changes("dbo_orders", "0x01", "0x02", ["order_id", "status"], True, 100))
    sql, params = rec.calls[-1]
    assert "FROM cdc.[dbo_orders_CT] c " in sql and "fn_cdc_get_all_changes" not in sql
    assert "WHERE c.[__$start_lsn] BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)" in sql
    assert "JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.[__$start_lsn]" in sql
    assert sql.rstrip().endswith("ORDER BY c.[__$start_lsn], c.[__$command_id], c.[__$seqval], c.[__$operation]")
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
    list(SqlCdcClient(utc, source_timezone="UTC").iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
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
    assert "CAST(DATEADD(minute, 180, m.tran_end_time) AS datetime2(3)) AS _commit_ts" in changes_sql
    assert "AT TIME ZONE" not in changes_sql  # no per-row conversion


def test_timezone_auto_is_detected_once_per_client():
    rec = Recorder("2026-09-28T16:50:00.123", tz="E. South America Standard Time")
    client = SqlCdcClient(rec)
    client.lsn_to_time("0x01")
    list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert [sql for sql, _ in rec.calls].count("SELECT CURRENT_TIMEZONE_ID()") == 1
    assert "AT TIME ZONE N'E. South America Standard Time') AT TIME ZONE 'UTC'" in rec.calls[-1][0]


def test_timezone_auto_falls_back_to_the_current_offset_before_2022():
    class OldServer(Recorder):  # SQL Server 2016-2019
        def scalar(self, sql, params=()):
            if "CURRENT_TIMEZONE_ID" in sql:
                self.calls.append((sql, tuple(params)))
                raise RuntimeError("'CURRENT_TIMEZONE_ID' is not a recognized built-in function name.")
            if "TZOFFSET" in sql:
                self.calls.append((sql, tuple(params)))
                return -180
            return super().scalar(sql, params)

    rec = OldServer("2026-09-28T16:50:00.123")
    client = SqlCdcClient(rec)
    client.lsn_to_time("0x01")
    list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert client.timezone == "UTC-03:00"
    assert "CAST(DATEADD(minute, 180, m.tran_end_time) AS datetime2(3))" in rec.calls[-1][0]
    assert [sql for sql, _ in rec.calls].count("SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET())") == 1


def test_lsn_to_time_always_carries_milliseconds():
    # style 126 drops ".000" on whole seconds; the offset contract keeps them (ADR 0002)
    whole = SqlCdcClient(Recorder(scalar_value="2026-09-28T16:50:00"), source_timezone="UTC")
    assert whole.lsn_to_time("0x01") == "2026-09-28T16:50:00.000"
    ms = SqlCdcClient(Recorder(scalar_value="2026-09-28T16:50:00.123"), source_timezone="UTC")
    assert ms.lsn_to_time("0x01") == "2026-09-28T16:50:00.123"
    assert SqlCdcClient(Recorder(scalar_value=None), source_timezone="UTC").lsn_to_time("0x01") is None


def test_timezone_detected_names_are_validated():
    with pytest.raises(ValueError, match="Invalid sourceTimeZone"):  # detected values are inlined too
        SqlCdcClient(Recorder(tz="UTC'; DROP TABLE x --")).lsn_to_time("0x01")


def test_injection_is_rejected():
    client = SqlCdcClient(Recorder())
    with pytest.raises(ValueError):
        list(client.iter_changes("dbo_orders; DROP TABLE x", "0x01", "0x02", [], True, 10))
    with pytest.raises(ValueError):
        list(client.iter_changes("dbo_orders", "0x01", "0x02", ["a]; --"], True, 10))
    with pytest.raises(ValueError):
        SqlCdcClient(Recorder(), source_timezone="UTC'; DROP")


def test_min_lsn_zero_means_missing_instance():
    with pytest.raises(ValueError, match="not found"):
        SqlCdcClient(Recorder("0x00000000000000000000")).min_lsn("dbo_orders")


def test_nth_commit_and_normalization():
    rec = Recorder("0x0000002a000001000001")
    assert SqlCdcClient(rec).nth_commit_after("0x0000002A000001000000", 5) == "0x0000002A000001000001"
    assert "TOP (5)" in rec.calls[-1][0]


def test_make_client_requires_connection_string():
    with pytest.raises(ValueError, match="connectionString"):
        make_client({"backend": "mssql-python"})
    with pytest.raises(ValueError, match="Unknown backend"):
        make_client({"backend": "jdbc", "connectionString": "x"})


class Rows(Recorder):
    """Returns ``rows`` as one Arrow batch from ``batches()``."""

    def __init__(self, rows):
        super().__init__()
        self.rows = rows

    def batches(self, sql, params, batch_size):
        self.calls.append((sql, tuple(params)))
        return iter([pa.RecordBatch.from_pylist(self.rows)] if self.rows else [])


def _col(ordinal, name, data_type, precision=None, scale=None):
    """One row of sys.sp_cdc_get_captured_columns (the columns the client reads)."""
    return {"column_ordinal": ordinal, "column_name": name, "data_type": data_type,
            "numeric_precision": precision, "numeric_scale": scale}


def test_captured_columns_inferred_from_cdc_metadata():
    rec = Rows([_col(2, "amount", "decimal", 18, 2), _col(1, "order_id", "int", 10, 0),
                _col(3, "status", "varchar"), _col(4, "created at", "datetime2"),
                _col(5, "flag", "bit"), _col(6, "qty", "tinyint", 3, 0)])
    ddl = SqlCdcClient(rec).captured_columns("dbo_orders")
    assert ddl == ("`order_id` INT, `amount` DECIMAL(18,2), `status` STRING, "
                   "`created at` TIMESTAMP_NTZ, `flag` BOOLEAN, `qty` SMALLINT")  # ordinal order
    assert rec.calls[-1] == ("EXEC sys.sp_cdc_get_captured_columns @capture_instance = ?", ("dbo_orders",))


def test_captured_columns_errors_point_to_columns_option():
    with pytest.raises(ValueError, match="geography.*'columns'"):
        SqlCdcClient(Rows([_col(1, "shape", "geography")])).captured_columns("dbo_orders")
    with pytest.raises(ValueError, match="not found"):
        SqlCdcClient(Rows([])).captured_columns("dbo_orders")
    with pytest.raises(ValueError):
        SqlCdcClient(Rows([])).captured_columns("dbo_orders; DROP TABLE x")

    class Denied(Recorder):
        def batches(self, sql, params, batch_size):
            raise RuntimeError("Object doesn't exist or access is denied.")  # Error 22981

    with pytest.raises(ValueError, match="gating role"):
        SqlCdcClient(Denied()).captured_columns("dbo_orders")


def test_change_table_permission_error_names_the_grant():
    class Denied(Recorder):
        def batches(self, sql, params, batch_size):
            raise RuntimeError("[SQL Server]The SELECT permission was denied on the object "
                               "'dbo_orders_CT', database 'db', schema 'cdc'.")

    with pytest.raises(PermissionError, match=r"GRANT SELECT ON cdc\.\[dbo_orders_CT\]"):
        list(SqlCdcClient(Denied()).iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    with pytest.raises(PermissionError, match=r"GRANT SELECT ON cdc\.\[dbo_orders_CT\]"):
        SqlCdcClient(Denied(), source_timezone="UTC").split_points("dbo_orders", "0x01", "0x02", 4)


def test_schema_infers_columns_when_option_missing(monkeypatch):
    import mssql_cdc.client as client_mod
    from mssql_cdc.source import MssqlCdcDataSource

    client = SqlCdcClient(Rows([_col(1, "order_id", "int", 10, 0)]))
    closed = []
    client.close = lambda: closed.append(True)
    monkeypatch.setattr(client_mod, "make_client", lambda options: client)
    schema = MssqlCdcDataSource({"captureInstance": "dbo_orders"}).schema()
    assert schema.endswith("_commit_ts TIMESTAMP_NTZ, `order_id` INT") and closed
    explicit = MssqlCdcDataSource({"captureInstance": "dbo_orders", "columns": "id BIGINT"})
    assert explicit.schema().endswith(", id BIGINT")


def test_fake_backend_still_requires_columns(tmp_path):
    from mssql_cdc.source import MssqlCdcDataSource

    ds = MssqlCdcDataSource({"backend": "fake", "fakePath": str(tmp_path), "captureInstance": "dbo_orders"})
    with pytest.raises(ValueError, match="'columns' is required"):
        ds.schema()


def test_ping_and_network_wait():
    rec = Recorder(scalar_value=1234)
    client = SqlCdcClient(rec, source_timezone="UTC")
    assert len(client.ping(3)) == 3 and rec.calls[-1] == ("SELECT 1", ())
    assert client.network_wait_ms() == 1234
    assert "sys.dm_exec_session_wait_stats WHERE session_id = @@SPID AND wait_type = 'ASYNC_NETWORK_IO'" in rec.calls[-1][0]

    class NoView(Recorder):
        def scalar(self, sql, params=()):
            if "dm_exec_session_wait_stats" in sql:
                raise RuntimeError("VIEW SERVER STATE permission was denied")
            return super().scalar(sql, params)

    assert SqlCdcClient(NoView(), source_timezone="UTC").network_wait_ms() is None  # never raises


def test_split_points_tile_the_change_rows_of_the_capture_instance():
    rec = Rows([{"b": "0x0000002a000001000001"}, {"b": "0x0000002a000001000002"}])
    points = SqlCdcClient(rec, source_timezone="UTC").split_points("dbo_orders", "0x01", "0x02", 4)
    assert points == ["0x0000002A000001000001", "0x0000002A000001000002"]
    sql, params = rec.calls[-1]
    assert "NTILE(4) OVER (ORDER BY __$start_lsn)" in sql and "FROM cdc.[dbo_orders_CT]" in sql
    assert "lsn_time_mapping" not in sql and params == ("0x01", "0x02")


def test_snapshot_queries():
    class Help(Recorder):
        def batches(self, sql, params, batch_size):
            self.calls.append((sql, tuple(params)))
            if "sp_cdc_help_change_data_capture" in sql:
                return iter([pa.RecordBatch.from_pylist([
                    {"capture_instance": "dbo_other", "source_schema": "dbo", "source_table": "other",
                     "index_column_list": None, "start_lsn": None},
                    {"capture_instance": "dbo_orders", "source_schema": "sales", "source_table": "orders",
                     "index_column_list": "[order_id], [line]", "start_lsn": "0x0000002a000001000001"}])])
            return iter(())

    rec = Help()
    client = SqlCdcClient(rec)
    assert client.source_table("dbo_orders") == (
        "sales", "orders", ["order_id", "line"], "0x0000002A000001000001")
    assert rec.calls[-1] == ("EXEC sys.sp_cdc_help_change_data_capture", ())
    with pytest.raises(ValueError, match="not found"):
        client.source_table("dbo_missing")

    assert client.key_range("sales", "orders", "order_id") == (None, None)  # empty table
    assert rec.calls[-1][0] == ("SELECT (SELECT MIN([order_id]) FROM [sales].[orders]) AS lo, "
                                "(SELECT MAX([order_id]) FROM [sales].[orders]) AS hi")

    def where(lo, hi, key="order_id"):
        list(client.iter_table("sales", "orders", ["order_id", "status"], key, lo, hi, 100))
        return rec.calls[-1][0].removeprefix("SELECT [order_id], [status] FROM [sales].[orders]")

    assert where(None, None) == "" and where(1, 2, key=None) == ""
    assert where(None, 10) == " WHERE ([order_id] < 10 OR [order_id] IS NULL)"
    assert where(10, 20) == " WHERE [order_id] >= 10 AND [order_id] < 20"
    assert where(20, None) == " WHERE [order_id] >= 20"
    with pytest.raises(ValueError):
        where("1; DROP TABLE x", None)
    with pytest.raises(ValueError):
        where(None, None, key="a]; DROP TABLE x --")
