"""SqlCdcClient builds T-SQL; check the generated statements without a server."""

import pytest

from mssql_cdc.client import Backend, SqlCdcClient, make_client


class Recorder(Backend):
    def __init__(self, scalar_value="0x0000002A000001000001"):
        self.calls = []
        self.value = scalar_value

    def batches(self, sql, params, batch_size):
        self.calls.append((sql, tuple(params)))
        return iter(())

    def scalar(self, sql, params=()):
        self.calls.append((sql, tuple(params)))
        return self.value


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
    list(SqlCdcClient(utc).iter_changes("dbo_orders", "0x01", "0x02", [], True, 10))
    assert "AT TIME ZONE" not in utc.calls[-1][0]


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
