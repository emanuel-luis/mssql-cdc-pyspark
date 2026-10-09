"""The sink's pure helpers, without a Spark session."""

import json
import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from mssql_cdc.events import event_row
from mssql_cdc.sink import _fold_metrics, _headroom, _lag, write_facts

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
        warnings=["option 'maxCommitPerBatch' is not one this source reads"],
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
        "data_skipped": [],  # no partition found cleanup had run while it read
        # the driver's, which the batch's last range carried
        "warnings": ["option 'maxCommitPerBatch' is not one this source reads"],
    }
    # one partition did not measure its wait: a sum without it would understate the batch's
    _write(path, "early.json", to_lsn="0x00000000000000000001", seconds=0, bytes=0)
    assert _fold_metrics(path)["network_wait_ms"] is None


def _round_trip(uri, local):
    """The reader's files written to ``uri``, folded and removed through it; ``local``: where
    they land. Returns the file names the sink listed."""
    from mssql_cdc._metricsfs import list_json, remove
    from mssql_cdc.sink import _read_events
    from mssql_cdc.source import _write_event, _write_metrics

    _write_metrics(uri, "0x01-0x02", {"to_lsn": "0x02", "seconds": 1.0, "bytes": 1e6})
    _write_event(uri, "schema_change", "dbo_t", "0x01", None, "{}")
    # written whole: no temporary file left beside them
    assert sorted(os.listdir(local)) == ["0x01-0x02.json", "event-schema_change-0x01.json"]
    assert _fold_metrics(uri)["read_mb"] == 1.0
    names, events = _read_events(uri)
    assert [e["event"] for e in events] == ["schema_change"]
    listed = list_json(uri) + names
    remove(listed)
    assert os.listdir(local) == []
    return listed


def test_a_uri_metrics_path_goes_through_pyarrow_fs(tmp_path):
    # file://: pyarrow.fs's local filesystem, which moves a temporary file over the name
    listed = _round_trip((tmp_path / "m").as_uri(), tmp_path / "m")
    assert listed == [
        (tmp_path / "m" / n).as_uri() for n in ("0x01-0x02.json", "event-schema_change-0x01.json")
    ]


class _NoRename:
    """A filesystem that cannot move a file (pyarrow raises NotImplementedError), as some
    object stores; a local filesystem otherwise."""

    def __init__(self):
        from pyarrow.fs import LocalFileSystem

        self.fs, self.moves = LocalFileSystem(), 0

    def __getattr__(self, name):
        return getattr(self.fs, name)

    def move(self, src, dest):
        self.moves += 1
        raise NotImplementedError("Move is not supported")


def test_a_filesystem_without_rename_gets_each_file_in_one_write(tmp_path, monkeypatch):
    from mssql_cdc import _metricsfs

    store = _NoRename()
    # s3://b/<key>?<options> is tmp_path/<key>; the query is pyarrow's, not part of the key
    monkeypatch.setattr(
        _metricsfs,
        "_open",
        lambda uri: (store, str(tmp_path / uri.partition("?")[0].removeprefix("s3://b/"))),
    )
    listed = _round_trip("s3://b/m?region=x", tmp_path / "m")
    assert store.moves == 2  # tried once per file, then written under its own name
    assert listed == [
        "s3://b/m/0x01-0x02.json?region=x",
        "s3://b/m/event-schema_change-0x01.json?region=x",
    ]
    # a stream's directory under an explicit one keeps the options last
    assert _metricsfs.join("s3://b/m/?region=x", "app") == "s3://b/m/app?region=x"


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
    row = event_row({**event, "detail": "0x01..0x20", **gap}, app_id="a", batch_id=3)
    assert (row["lost_from_ts"], row["lost_to_ts"]) == (datetime(2026, 9, 28, 13), T)
    assert (row["batch_id"], row["rows"], row["min_lsn"], row["max_lsn"]) == (3, 0, lsn, lsn)
    assert event_row({**event, "event": "schema_change"})["lost_from_ts"] is None  # no gap


def test_a_facts_key_that_is_no_column_raises_before_anything_is_written():
    from pyspark.sql import SparkSession

    spark = MagicMock(spec_set=SparkSession)
    with pytest.raises(ValueError, match=r"unknown facts columns: \['read_MB'\]"):
        write_facts(spark, "facts", [{"app_id": "x", "read_MB": 1.0}], None, 0)
    assert not spark.method_calls  # never reached: projecting would have written NULL read_mb
