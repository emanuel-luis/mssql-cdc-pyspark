from datetime import datetime

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


def test_tracker_worker_skips_other_runs_and_same_period_and_retries_after_termination(
    monkeypatch,
):
    import json
    import time
    from types import SimpleNamespace

    from mssql_cdc import finalization

    calls, failures = [], []

    def advance(spark, control, table, end, granularity):
        calls.append(end["commit_ts"])
        if failures:  # a MERGE of another tracker on the same control table won
            raise RuntimeError(failures.pop())

    monkeypatch.setattr(finalization, "advance", advance)
    session = SimpleNamespace(streams=SimpleNamespace(removeListener=lambda listener: None))
    tracker = finalization.FinalizationListener(session, "run-1", "ctl", "bronze_orders")
    tracker._worker.start()

    def progress(ts, run="run-1"):
        p = {"sources": [{"endOffset": {"lsn": "0x1", "commit_ts": f"2026-09-28T{ts}:00.000"}}]}
        tracker.onQueryProgress(
            SimpleNamespace(progress=SimpleNamespace(runId=run, json=json.dumps(p)))
        )
        deadline = time.monotonic() + 30
        while tracker._progress is not None:  # taken by the worker, not merged with the next
            assert time.monotonic() < deadline
            time.sleep(0.01)

    progress("13:10")
    progress("13:40")  # the same hour: no MERGE
    progress("20:00", run="run-0")  # an earlier run of the query
    tracker.onQueryTerminated(SimpleNamespace(runId="run-0"))  # does not stop this tracker
    failures.append("ConcurrentAppendException")
    progress("14:05")
    tracker.onQueryTerminated(SimpleNamespace(runId="run-1"))
    assert tracker.join(timeout=30)
    assert calls == ["2026-09-28T13:10:00.000"] + ["2026-09-28T14:05:00.000"] * 2
