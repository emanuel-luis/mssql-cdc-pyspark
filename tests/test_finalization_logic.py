from datetime import datetime

from mssql_cdc.finalization import candidate, end_offset_from_progress, table_ref, truncate


def test_truncate():
    ts = datetime(2026, 9, 28, 14, 37, 12, 345000)
    assert truncate(ts, "minute") == datetime(2026, 9, 28, 14, 37)
    assert truncate(ts, "hour") == datetime(2026, 9, 28, 14, 0)
    assert truncate(ts, "day") == datetime(2026, 9, 28)


def test_candidate_excludes_period_containing_end():
    # batch ended at 14:37 -> everything before 14:00 is final, 14:00-15:00 is not
    assert candidate({"lsn": "0x1", "commit_ts": "2026-09-28T14:37:12.345"}) == datetime(2026, 9, 28, 14)
    assert candidate({"lsn": "0x1", "commit_ts": ""}) is None
    assert candidate(None) is None


def test_end_offset_from_progress_accepts_str_or_dict():
    p1 = {"sources": [{"endOffset": '{"lsn": "0xA", "commit_ts": "2026-09-28T01:00:00.000"}'}]}
    p2 = {"sources": [{"endOffset": {"lsn": "0xA", "commit_ts": "2026-09-28T01:00:00.000"}}]}
    assert end_offset_from_progress(p1) == end_offset_from_progress(p2)
    assert end_offset_from_progress({"sources": []}) is None


def test_table_ref():
    assert table_ref("lab.cdc.orders") == "lab.cdc.orders"
    assert table_ref("/tmp/x") == "delta.`/tmp/x`"
    assert table_ref("abfss://c@a.dfs.core.windows.net/x") == "delta.`abfss://c@a.dfs.core.windows.net/x`"
