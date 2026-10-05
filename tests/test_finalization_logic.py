import json
import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from mssql_cdc import finalization, tables
from mssql_cdc.finalization import candidate, end_offset_from_progress, table_ref, truncate


def test_truncate():
    ts = datetime(2026, 9, 28, 14, 37, 12, 345000)
    assert truncate(ts, "minute") == datetime(2026, 9, 28, 14, 37)
    assert truncate(ts, "hour") == datetime(2026, 9, 28, 14, 0)
    assert truncate(ts, "day") == datetime(2026, 9, 28)


def test_candidate_excludes_period_containing_end():
    # batch ended at 14:37 -> everything before 14:00 is final, 14:00-15:00 is not
    assert candidate({"lsn": "0x1", "commit_ts": "2026-09-28T14:37:12.345"}) == datetime(
        2026, 9, 28, 14
    )
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
    assert (
        table_ref("abfss://c@a.dfs.core.windows.net/x")
        == "delta.`abfss://c@a.dfs.core.windows.net/x`"
    )


def _tracker(monkeypatch, advance):
    monkeypatch.setattr(finalization, "advance", advance)
    session = SimpleNamespace(streams=SimpleNamespace(removeListener=lambda listener: None))
    tracker = finalization.FinalizationListener(session, "run-1", "ctl", "bronze_orders")
    tracker._worker.start()
    return tracker


def _progress(ts, run="run-1"):
    p = {"sources": [{"endOffset": {"lsn": "0x1", "commit_ts": f"2026-09-28T{ts}:00.000"}}]}
    return SimpleNamespace(runId=run, json=json.dumps(p))


def _until(done):
    deadline = time.monotonic() + 30
    while not done():
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_tracker_worker_skips_other_runs_and_same_period(monkeypatch):
    calls = []
    tracker = _tracker(monkeypatch, lambda s, c, t, end, g: calls.append(end["commit_ts"]))

    def progress(ts, run="run-1"):
        tracker.onQueryProgress(SimpleNamespace(progress=_progress(ts, run)))
        _until(lambda: tracker._progress is None)  # taken by the worker, not merged with the next

    progress("13:10")
    progress("13:40")  # the same hour: no MERGE
    progress("20:00", run="run-0")  # an earlier run of the query
    tracker.onQueryTerminated(SimpleNamespace(runId="run-0"))  # does not stop this tracker
    progress("14:05")
    tracker.onQueryTerminated(SimpleNamespace(runId="run-1"))
    assert tracker.join(timeout=30)
    assert calls == ["2026-09-28T13:10:00.000", "2026-09-28T14:05:00.000"]


def test_a_failing_tracker_keeps_its_error_and_logs_one_error_per_streak(monkeypatch, caplog):
    caplog.set_level(logging.WARNING, logger="mssql_cdc.finalization")
    failing = [True]

    def advance(spark, control, table, end, granularity):
        if failing[0]:
            raise PermissionError("no MODIFY on the control table")

    tracker = _tracker(monkeypatch, advance)

    def logged():
        return [r for r in caplog.records if r.name == "mssql_cdc.finalization"]

    def offer(ts, failures):
        tracker._offer(_progress(ts))
        _until(lambda: tracker.failures == failures)
        return [(r.levelname, r.exc_info is not None) for r in logged()]

    assert offer("13:10", 1) == [("ERROR", True)]  # the first of a streak, with its traceback
    assert offer("13:20", 2) == [("ERROR", True)]  # tried again, and quiet for 10 minutes
    monkeypatch.setattr(finalization, "_REPEAT_SECONDS", 0)
    assert offer("13:30", 3) == [("ERROR", True), ("WARNING", False)]
    assert "3 failures in a row" in logged()[-1].getMessage()
    assert isinstance(tracker.last_error, PermissionError)
    failing[0] = False
    offer("13:40", 0)
    assert tracker.last_error is None
    tracker._stop()
    assert tracker.join(timeout=30) and len(logged()) == 2


class ConcurrentAppendException(Exception):  # named as delta.exceptions' is
    pass


def test_a_control_merge_retries_only_concurrent_commits_until_its_deadline(monkeypatch):
    sleeps, calls = [], []
    monkeypatch.setattr(time, "sleep", sleeps.append)

    def merge(*errors):
        def run():
            calls.append(1)
            if len(calls) <= len(errors):
                raise errors[len(calls) - 1]

        return run

    lost = ConcurrentAppendException("[DELTA_CONCURRENT_APPEND] files were added")
    tables.retrying(merge(lost, ConcurrentAppendException("again")))
    assert len(calls) == 3 and len(sleeps) == 2 and all(0 <= s <= 10 for s in sleeps)
    # Spark Connect raises its own types: the error class in the message counts too
    assert tables.is_conflict(RuntimeError("[DELTA_METADATA_CHANGED] metadata changed"))

    calls.clear()
    with pytest.raises(ValueError):
        tables.retrying(merge(ValueError("not a conflict")))
    assert calls == [1] and len(sleeps) == 2  # at once

    calls.clear()
    monkeypatch.setattr(tables, "_RETRY_SECONDS", 0)
    with pytest.raises(ConcurrentAppendException):
        tables.retrying(merge(lost, lost))
    assert calls == [1]  # past the deadline: raised


def test_is_final_takes_an_aware_period_end_in_utc(monkeypatch):
    monkeypatch.setattr(finalization, "finalized_until", lambda *a: datetime(2026, 9, 28, 14))
    brt = timezone(timedelta(hours=-3))
    assert finalization.is_final(None, "ctl", "t", datetime(2026, 9, 28, 11, tzinfo=brt))
    assert not finalization.is_final(None, "ctl", "t", datetime(2026, 9, 28, 12, tzinfo=brt))
    assert finalization.is_final(None, "ctl", "t", datetime(2026, 9, 28, 14))
