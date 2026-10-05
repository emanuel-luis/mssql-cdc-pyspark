"""The sink's pure helpers, without a Spark session."""

import json
import os
from datetime import datetime

import pytest

from mssql_cdc.sink import _event_row, _fold_metrics, _headroom, _lag, write_facts

T = datetime(2026, 9, 28, 14, 0)


def _write(path, name, **metrics):
    with open(os.path.join(path, name), "w", encoding="utf-8") as fh:
        json.dump(metrics, fh)


def test_fold_metrics_skips_unreadable_files_and_ends_at_the_largest_to_lsn(tmp_path):
    path = str(tmp_path)
    assert _fold_metrics(path) == {}
    end = "0x00000000000000000020"
    _write(
        path,
        "0x00000000000000000011-0x00000000000000000020.json",
        to_lsn=end,
        to_commit_ts="2026-09-28T14:00:00",
        seconds=1.5,
        bytes=2e6,
        network_wait_ms=3,
        rtt_ms=2.0,
        retention_watermark_ts="2026-09-25T14:00:00",
        source_max_commit_ts="2026-09-28T14:01:00",
        capture_lag_seconds=4.0,
    )
    _write(
        path,
        "0x00000000000000000001-0x00000000000000000010.json",
        to_lsn="0x00000000000000000010",
        to_commit_ts="2026-09-28T13:00:00",
        seconds=0.5,
        bytes=1e6,
        network_wait_ms=5,
        rtt_ms=4.0,
    )
    with open(os.path.join(path, "torn.json"), "w", encoding="utf-8") as fh:
        fh.write('{"to_lsn": "0xFF')  # a dead attempt's, cut short
    # the reader's event: not a partition's metrics
    _write(path, "event-schema_change-0x00000000000000000099.json", to_lsn="0x" + "F" * 20)
    assert _fold_metrics(path) == {
        "end_lsn": end,
        "end_commit_ts": T,
        "retention_watermark_ts": datetime(2026, 9, 25, 14),
        "source_max_commit_ts": datetime(2026, 9, 28, 14, 1),
        "capture_lag_seconds": 4.0,
        "source_rtt_ms": 3.0,
        "read_seconds": 2.0,
        "read_mb": 3.0,
        "network_wait_ms": 8,
    }
    # one partition did not measure its wait: a sum without it would understate the batch's
    _write(path, "early.json", to_lsn="0x00000000000000000001", seconds=0, bytes=0)
    assert _fold_metrics(path)["network_wait_ms"] is None


def test_headroom_and_lag_are_null_without_either_end():
    assert _headroom(None, T) == {"retention_watermark_ts": None, "retention_headroom_hours": None}
    assert _headroom(T, None)["retention_headroom_hours"] is None
    assert _headroom(datetime(2026, 9, 28, 12, 30), T)["retention_headroom_hours"] == 1.5
    assert _lag(None, T) is None and _lag(T, None) is None
    assert _lag(datetime(2026, 9, 28, 14, 0, 30), T) == 30.0


def test_a_data_skipped_event_row_carries_the_gap():
    lsn = "0x00000000000000000020"
    event = {"event": "data_skipped", "lsn": lsn, "commit_ts": "2026-09-28T14:00:00.000"}
    gap = {"lost_from_ts": "2026-09-28T13:00:00.000", "lost_to_ts": "2026-09-28T14:00:00.000"}
    row = _event_row({**event, "detail": "0x01..0x20", **gap}, app_id="a", batch_id=3)
    assert (row["lost_from_ts"], row["lost_to_ts"]) == (datetime(2026, 9, 28, 13), T)
    assert (row["batch_id"], row["rows"], row["min_lsn"], row["max_lsn"]) == (3, 0, lsn, lsn)
    assert _event_row({**event, "event": "schema_change"})["lost_from_ts"] is None  # no gap


def test_a_facts_key_that_is_no_column_raises_before_anything_is_written():
    # spark is never reached: projecting would have written NULL read_mb
    with pytest.raises(ValueError, match=r"unknown facts columns: \['read_MB'\]"):
        write_facts(None, "facts", [{"app_id": "x", "read_MB": 1.0}], None, 0)
