"""Completeness signal ("partition finalization") for CDC-fed tables.

Two layers, in the spirit of Pinterest's partition finalization:

* **Facts**, per micro-batch: what was written (row counts, LSN and commit-time
  ranges). See ``mssql_cdc.sink``.
* **Verdict**, per table: ``finalized_until``. Every period strictly before it is
  complete in the target table and will not receive more source commits.

Why the verdict is sound for SQL Server CDC: a batch's end LSN never exceeds
``sys.fn_cdc_get_max_lsn()``, the last LSN the capture process has processed, and
the capture process writes change rows in commit order. So once a batch ending at
LSN ``L`` is committed to the target, every source transaction committed at or
before the commit time of ``L`` is in the target. Transactions sharing that exact
commit time may still be pending, which is why the period *containing* it is not
finalized; truncating to the period start handles that.

Ordering rather than atomicity keeps it safe: write data first, advance the verdict
after, and never let it move backwards. If the job dies in between, the verdict
lags (consumers wait a little longer); it can never run ahead of the data.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from pyspark.sql.streaming import StreamingQueryListener

from . import migrations
from .migrations.control import APPLIED_COLUMNS
from .tables import delta_table, table_ref  # noqa: F401 - table_ref re-exported

if TYPE_CHECKING:
    from pyspark.sql.streaming.listener import (
        QueryProgressEvent,
        QueryStartedEvent,
        QueryTerminatedEvent,
    )

_log = logging.getLogger(__name__)
_GRANULARITIES = ("minute", "hour", "day")


def truncate(ts: datetime, granularity: str = "hour") -> datetime:
    g = granularity.lower()
    if g not in _GRANULARITIES:
        raise ValueError(f"granularity must be one of {_GRANULARITIES}")
    ts = ts.replace(second=0, microsecond=0)
    if g in ("hour", "day"):
        ts = ts.replace(minute=0)
    if g == "day":
        ts = ts.replace(hour=0)
    return ts


def candidate(end_offset: dict | None, granularity: str = "hour") -> datetime | None:
    """``finalized_until`` implied by a committed batch's end offset."""
    if not end_offset or not end_offset.get("commit_ts"):
        return None
    return truncate(datetime.fromisoformat(end_offset["commit_ts"]), granularity)


def end_offset_from_progress(progress, source_index: int = 0) -> dict | None:
    """Extract the end offset from a StreamingQueryProgress (object or dict)."""
    if progress is None:
        return None
    data = json.loads(progress.json) if hasattr(progress, "json") else progress
    sources = data.get("sources") or []
    if len(sources) <= source_index:
        return None
    end = sources[source_index].get("endOffset")
    if isinstance(end, str):
        end = json.loads(end)
    return end


CONTROL_COMMENT = (
    "Completeness verdict per CDC-fed table, kept by mssql-cdc-pyspark: one row per table. "
    "Gate downstream work on finalized_until, e.g. with finalization.is_final()."
)
CONTROL_COLUMNS = [
    (
        "table_name",
        "STRING",
        (
            "The table this verdict is about: the name passed to "
            "finalization.advance(), usually the target table. One row per table."
        ),
    ),
    (
        "finalized_until",
        "TIMESTAMP_NTZ",
        (
            "The verdict, UTC. Every period that ends at or before "
            "this instant is complete in the table: no source commit at or before it can still arrive. "
            "It only moves forward. A consumer of the period [start, end) waits for finalized_until >= end."
        ),
    ),
    (
        "end_lsn",
        "STRING",
        (
            "Source commit LSN (0x + 20 hex) of the batch end that last moved the "
            "verdict: how far the source had been read and committed to the table."
        ),
    ),
    (
        "end_commit_ts",
        "TIMESTAMP_NTZ",
        (
            "Commit time of end_lsn, UTC. finalized_until is this instant "
            "truncated to the period (an hour by default), because transactions sharing this exact "
            "commit time may still be arriving."
        ),
    ),
    ("updated_at", "TIMESTAMP_NTZ", "When the verdict last moved, UTC."),
    *APPLIED_COLUMNS,
]


def ensure_control_table(spark, control_table: str) -> None:
    migrations.ensure(spark, control_table, "control", CONTROL_COLUMNS, CONTROL_COMMENT)


def advance(
    spark, control_table: str, table_name: str, end_offset: dict | None, granularity: str = "hour"
) -> datetime | None:
    """Monotonically advance ``finalized_until`` for ``table_name``.

    Call only after the batch that produced ``end_offset`` is committed.
    """
    cand = candidate(end_offset, granularity)
    ensure_control_table(spark, control_table)
    if cand is not None:
        assert end_offset is not None  # candidate() is None without it
        src = spark.createDataFrame(
            [
                (
                    table_name,
                    cand,
                    end_offset["lsn"],
                    datetime.fromisoformat(end_offset["commit_ts"]),
                    datetime.now(timezone.utc).replace(tzinfo=None),
                )
            ],
            "table_name STRING, cand TIMESTAMP_NTZ, end_lsn STRING, end_ts TIMESTAMP_NTZ, now TIMESTAMP_NTZ",
        )
        changes = {
            "finalized_until": "s.cand",
            "end_lsn": "s.end_lsn",
            "end_commit_ts": "s.end_ts",
            "updated_at": "s.now",
        }
        (
            delta_table(spark, control_table)
            .alias("t")
            .merge(src.alias("s"), "t.table_name = s.table_name")
            # never backwards; NULL on a row apply_changes created before any verdict
            .whenMatchedUpdate(
                condition="t.finalized_until IS NULL OR s.cand > t.finalized_until", set=changes
            )
            .whenNotMatchedInsert(values={"table_name": "s.table_name", **changes})
            .execute()
        )
    return finalized_until(spark, control_table, table_name)


def finalized_until(spark, control_table: str, table_name: str) -> datetime | None:
    df = delta_table(spark, control_table).toDF()
    rows = df.where(df.table_name == table_name).select("finalized_until").collect()
    return rows[0][0] if rows else None


def is_final(spark, control_table: str, table_name: str, period_end: datetime) -> bool:
    """True when the period ending at ``period_end`` (exclusive) is complete."""
    fu = finalized_until(spark, control_table, table_name)
    return fu is not None and period_end <= fu


class FinalizationListener(StreamingQueryListener):
    """Advances ``finalized_until`` after the batches of one query run. Create it with
    ``track``, which registers it and returns it.

    Spark posts a progress event after the batch's data and checkpoint commits, so the verdict
    still follows the data (ADR 0005). The listener bus calls back on a thread every listener
    of the session shares, so the callbacks only hand the newest progress to one worker
    thread, which calls ``advance``. A progress that arrives while an advance runs replaces
    the one still waiting: the newest end offset implies the older ones. The worker skips
    an end offset that does not move the verdict, so the control table gets a commit per
    period, not per batch. A failed advance is logged; the next progress retries it.
    When the run terminates, the worker applies what is left, stops and removes the listener.
    """

    def __init__(
        self, spark, run_id: str, control_table: str, table_name: str, granularity: str = "hour"
    ):
        if granularity.lower() not in _GRANULARITIES:
            raise ValueError(f"granularity must be one of {_GRANULARITIES}")
        self._session, self._run_id = spark, str(run_id)
        self._control, self._table, self._granularity = control_table, table_name, granularity
        self._cond = threading.Condition()
        self._progress: Any = None  # the newest progress not yet applied
        self._stopped = False
        self._worker = threading.Thread(
            target=self._run, name=f"mssql-cdc-finalization {table_name}", daemon=True
        )

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        pass

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        if str(event.progress.runId) == self._run_id:
            self._offer(event.progress)

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        if str(event.runId) == self._run_id:
            self._stop()

    def _offer(self, progress) -> None:
        if progress is not None:
            with self._cond:
                self._progress = progress
                self._cond.notify()

    def _stop(self) -> None:
        with self._cond:
            self._stopped = True
            self._cond.notify()

    def _run(self) -> None:
        last = None  # the verdict this worker last wrote
        while True:
            with self._cond:
                self._cond.wait_for(lambda: self._progress is not None or self._stopped)
                progress, self._progress = self._progress, None
            if progress is None:  # stopped, nothing left to apply
                break
            try:
                end = end_offset_from_progress(progress)
                cand = candidate(end, self._granularity)
                if cand is not None and (last is None or cand > last):
                    advance(self._session, self._control, self._table, end, self._granularity)
                    last = cand
            except Exception:  # noqa: BLE001 - the worker must survive; the next progress retries
                _log.warning(
                    "finalized_until of %s not advanced; the next progress retries",
                    self._table,
                    exc_info=True,
                )
        try:
            self._session.streams.removeListener(self)
        except Exception:  # noqa: BLE001 - a stopped session has no listeners left to remove
            _log.warning("could not remove the finalization listener of %s", self._table)

    def join(self, timeout: float | None = None) -> bool:
        """Wait until the query has terminated and its last verdict is written (or failed,
        and logged). False when ``timeout`` seconds pass first."""
        self._worker.join(timeout)
        return not self._worker.is_alive()


def track(
    spark, query, control_table: str, table_name: str, granularity: str = "hour"
) -> FinalizationListener:
    """Advance ``finalized_until`` of ``table_name`` after every batch of ``query``, a started
    StreamingQuery that writes it, until the query terminates; returns the listener.

    For a stream that keeps running (a ``processingTime`` trigger, or the default). Call it
    right after starting the query; ``join()`` waits for the last verdict once the query
    stops. With ``availableNow``, ``query.awaitTermination()`` then ``join()``. The control
    table is created here, so a wrong name fails now rather than in the worker's log.
    """
    listener = FinalizationListener(spark, query.runId, control_table, table_name, granularity)
    ensure_control_table(spark, control_table)
    spark.streams.addListener(listener)
    # progress posted before the listener was added is missed: the latest one implies it
    listener._offer(query.lastProgress)
    if not query.isActive:  # terminated before the listener could hear it
        listener._stop()
    listener._worker.start()
    return listener
