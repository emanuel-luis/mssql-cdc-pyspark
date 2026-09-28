"""SqlCdcClient builds T-SQL; check the generated statements without a server."""

import pyarrow as pa
import pytest

from mssql_cdc.client import Backend, SqlCdcClient, make_client


class Recorder(Backend):
    def __init__(self, scalar_value="0x0000002A000001000001", tz="UTC"):
        self.calls = []
        self.value = scalar_value
        self.tz = tz

    def batches(self, sql, params, batch_size):
        self.calls.append((sql, tuple(params)))
        return iter(())

    def scalar(self, sql, params=()):
        self.calls.append((sql, tuple(params)))
        return self.tz if "CURRENT_TIMEZONE_ID" in sql else self.value


def test_changes_query_shape():
    rec = Recorder()
    client = SqlCdcClient(rec)
    list(client.iter_changes("dbo_orders", "0x01", "0x02", ["order_id", "status"], True, 100))
    sql, params = rec.calls[-1]
    assert "cdc.[fn_cdc_get_all_changes_dbo_orders](CONVERT(binary(10), ?, 1), CONVERT(binary(10), ?, 1), N'all update old')" in sql
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


def test_timezone_auto_is_detected_once_per_client():
    rec = Recorder(tz="E. South America Standard Time")
    client = SqlCdcClient(rec)
    client.lsn_to_time("0x01")
    list(client.iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert [sql for sql, _ in rec.calls].count("SELECT CURRENT_TIMEZONE_ID()") == 1
    assert "AT TIME ZONE N'E. South America Standard Time') AT TIME ZONE 'UTC'" in rec.calls[-1][0]


def test_timezone_auto_fails_loudly():
    class OldServer(Recorder):
        def scalar(self, sql, params=()):
            if "CURRENT_TIMEZONE_ID" in sql:
                raise RuntimeError("'CURRENT_TIMEZONE_ID' is not a recognized built-in function name.")
            return super().scalar(sql, params)

    with pytest.raises(ValueError, match="Set sourceTimeZone"):
        SqlCdcClient(OldServer()).lsn_to_time("0x01")
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


def _col(name, type_name, precision=0, scale=0):
    return {"column_name": name, "type_name": type_name, "precision": precision, "scale": scale}


def test_captured_columns_inferred_from_cdc_metadata():
    rec = Rows([_col("order_id", "int", 10), _col("amount", "decimal", 18, 2),
                _col("status", "varchar"), _col("created at", "datetime2", 23, 3),
                _col("flag", "bit"), _col("qty", "tinyint", 3)])
    ddl = SqlCdcClient(rec).captured_columns("dbo_orders")
    assert ddl == ("`order_id` INT, `amount` DECIMAL(18,2), `status` STRING, "
                   "`created at` TIMESTAMP_NTZ, `flag` BOOLEAN, `qty` SMALLINT")
    sql, params = rec.calls[-1]
    assert "JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id" in sql
    assert "WHERE ct.capture_instance = ?" in sql and sql.endswith("ORDER BY cc.column_ordinal")
    assert params == ("dbo_orders",)


def test_captured_columns_errors_point_to_columns_option():
    with pytest.raises(ValueError, match="geography.*'columns'"):
        SqlCdcClient(Rows([_col("shape", "geography")])).captured_columns("dbo_orders")
    with pytest.raises(ValueError, match="not found"):
        SqlCdcClient(Rows([])).captured_columns("dbo_orders")
    with pytest.raises(ValueError):
        SqlCdcClient(Rows([])).captured_columns("dbo_orders; DROP TABLE x")


def test_schema_infers_columns_when_option_missing(monkeypatch):
    import mssql_cdc.client as client_mod
    from mssql_cdc.source import MssqlCdcDataSource

    client = SqlCdcClient(Rows([_col("order_id", "int", 10)]))
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
