"""Completeness signal ("partition finalization") for CDC-fed tables.

Two layers, in the spirit of Pinterest's partition finalization:

* **Facts**, per micro-batch: what was written (row counts, LSN and commit-time
  ranges). See ``mssql_cdc.sink``.
* **Verdict**, per table: ``finalized_until``. Every period strictly before it is
  complete in the target table and will not receive more source commits. Over a loss gap
  a re-snapshot recorded (``lost_from_ts``..``lost_to_ts`` of the facts' 'resnapshot'
  rows), a change-log table's verdict means only that nothing more will arrive, not that
  the gap's changes are in it (ADR 0018).

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
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, get_args

from pyspark.sql.streaming.listener import StreamingQueryListener

from . import migrations
from .migrations.control import APPLIED_COLUMNS, OPEN_COMMENTS, VERDICT_COMMENTS, WAVE_COLUMNS
from .tables import delta_table, retrying, table_ref  # noqa: F401 - table_ref re-exported
from .types import Granularity, Offset

if TYPE_CHECKING:
    from pyspark.sql import Column
    from pyspark.sql.streaming.listener import (
        QueryProgressEvent,
        QueryStartedEvent,
        QueryTerminatedEvent,
    )

    from .types import SparkSessionLike, StreamingQueryLike

_log = logging.getLogger(__name__)
_GRANULARITIES = get_args(Granularity)
_REPEAT_SECONDS = 600  # a tracker's failure streak: one WARNING at most this often


def _check_granularity(granularity: str) -> None:
    if granularity.lower() not in _GRANULARITIES:
        raise ValueError(f"granularity must be one of {_GRANULARITIES}")


def truncate(ts: datetime, granularity: str = "hour") -> datetime:
    _check_granularity(granularity)
    g = granularity.lower()
    ts = ts.replace(second=0, microsecond=0)
    if g in ("hour", "day"):
        ts = ts.replace(minute=0)
    if g == "day":
        ts = ts.replace(hour=0)
    return ts


def candidate(
    end_offset: Mapping[str, Any] | None, *, granularity: Granularity = "hour"
) -> datetime | None:
    """``finalized_until`` implied by a committed batch's end offset."""
    if not end_offset or not end_offset.get("commit_ts"):
        return None
    return truncate(datetime.fromisoformat(end_offset["commit_ts"]), granularity)


def end_offset_from_progress(
    progress: Mapping[str, Any] | None, *, source_index: int = 0
) -> Offset | None:
    """Extract the end offset from a StreamingQueryProgress (object or dict); ``source_index``
    is keyword-only."""
    if progress is None:
        return None
    data: Mapping[str, Any] = json.loads(progress.json) if hasattr(progress, "json") else progress
    sources = data.get("sources") or []
    if len(sources) <= source_index:
        return None
    end = sources[source_index].get("endOffset")
    offset: Offset | None = json.loads(end) if isinstance(end, str) else end  # the source's JSON
    return offset


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
    ("finalized_until", "TIMESTAMP_NTZ", VERDICT_COMMENTS["finalized_until"]),
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
    *((n, t, OPEN_COMMENTS.get(n, c)) for n, t, c in WAVE_COLUMNS),
]


def ensure_control_table(spark: SparkSessionLike, control_table: str) -> None:
    migrations.ensure(spark, control_table, "control", CONTROL_COLUMNS, CONTROL_COMMENT)


def advance(
    spark: SparkSessionLike,
    control_table: str,
    table_name: str,
    end_offset: Mapping[str, Any] | None,
    *,
    granularity: Granularity = "hour",
) -> datetime | None:
    """Monotonically advance ``finalized_until`` for ``table_name``; ``granularity`` is
    keyword-only.

    Call only after the batch that produced ``end_offset`` is committed. A MERGE that loses
    to a concurrent commit on the control table is retried for up to a minute.
    """
    cand = candidate(end_offset, granularity=granularity)
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
        changes: dict[str, str | Column] = {  # as DeltaMergeBuilder takes it
            "finalized_until": "s.cand",
            "end_lsn": "s.end_lsn",
            "end_commit_ts": "s.end_ts",
            "updated_at": "s.now",
        }
        retrying(  # every tracker and silver job shares the control table
            lambda: (
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
        )
    return finalized_until(spark, control_table, table_name)


def finalized_until(
    spark: SparkSessionLike, control_table: str, table_name: str
) -> datetime | None:
    df = delta_table(spark, control_table).toDF()
    rows = df.where(df.table_name == table_name).select("finalized_until").collect()
    return rows[0][0] if rows else None


def is_final(
    spark: SparkSessionLike, control_table: str, table_name: str, period_end: datetime
) -> bool:
    """True when the period ending at ``period_end`` (exclusive) is complete: no source
    commit in it can still arrive. Over a loss gap a re-snapshot recorded (the facts'
    'resnapshot' rows), a change-log table's period is final without the gap's changes.
    An aware ``period_end`` is taken in UTC, as the verdict is."""
    if period_end.tzinfo:
        period_end = period_end.astimezone(timezone.utc).replace(tzinfo=None)
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
    period, not per batch. ``advance`` itself retries a MERGE that loses to another
    tracker's on the same control table; a verdict that still fails is logged, and the next
    progress tries again. ``last_error`` is the error of the last attempt (None once one
    succeeds) and ``failures`` how many failed in a row: the first of a streak is logged as
    an ERROR, then a WARNING at most every 10 minutes until one succeeds. When the run
    terminates, the worker applies what is left, stops and removes the listener (on Spark
    Connect it leaves it registered: see ``_run``).
    """

    def __init__(
        self,
        spark: SparkSessionLike,
        run_id: str,
        control_table: str,
        table_name: str,
        *,
        granularity: Granularity = "hour",
    ):
        _check_granularity(granularity)
        self._session, self._run_id = spark, str(run_id)
        self._control, self._table, self._granularity = control_table, table_name, granularity
        self._cond = threading.Condition()
        self._progress: Mapping[str, Any] | None = None  # the newest progress not yet applied
        self._stopped = False
        self.last_error: Exception | None = None
        self.failures: int = 0
        self._warned = 0.0  # time.monotonic() of the streak's last log
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

    def _offer(self, progress: Mapping[str, Any] | None) -> None:
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
                cand = candidate(end, granularity=self._granularity)
                if cand is not None and (last is None or cand > last):
                    advance(
                        self._session,
                        self._control,
                        self._table,
                        end,
                        granularity=self._granularity,
                    )
                    last = cand  # not on a failure: the next progress tries again
                    self.last_error, self.failures = None, 0
            except Exception as exc:  # noqa: BLE001 - the worker must survive
                self._failed(exc)
        if type(self._session).__module__.startswith("pyspark.sql.connect"):
            # PySpark 4.2.0's Connect client removes its last listener under the lock its
            # event thread posts under, then joins that thread: an event in between hangs
            # both. Left registered, the listener ignores every other run's events.
            return
        try:
            self._session.streams.removeListener(self)
        except Exception:  # noqa: BLE001 - a stopped session has no listeners left to remove
            _log.warning("could not remove the finalization listener of %s", self._table)

    def _failed(self, exc: Exception) -> None:
        """Log a failed verdict: the first of a streak with its traceback, as an ERROR (a
        permanent one, such as no MODIFY on the control table, fails on every progress, about
        every 10 s when idle), then one WARNING at most every ``_REPEAT_SECONDS``."""
        n, now = self.failures + 1, time.monotonic()
        if n == 1:
            _log.error("finalized_until of %s not advanced", self._table, exc_info=True)
            self._warned = now
        elif now - self._warned >= _REPEAT_SECONDS:
            _log.warning(
                "finalized_until of %s still not advanced (%d failures in a row): %s",
                self._table,
                n,
                exc,
            )
            self._warned = now
        self.last_error, self.failures = exc, n

    def join(self, *, timeout: float | None = None) -> bool:
        """Wait until the query has terminated and its last verdict is written (or failed,
        and logged: see ``last_error``). False when ``timeout`` seconds pass first
        (keyword-only)."""
        self._worker.join(timeout)
        return not self._worker.is_alive()


def track(
    spark: SparkSessionLike,
    query: StreamingQueryLike,
    control_table: str,
    table_name: str,
    *,
    granularity: Granularity = "hour",
) -> FinalizationListener:
    """Advance ``finalized_until`` of ``table_name`` after every batch of ``query``, a started
    StreamingQuery that writes it, until the query terminates; returns the listener.

    For a stream that keeps running (a ``processingTime`` trigger, or the default). Call it
    right after starting the query; ``join()`` waits for the last verdict once the query
    stops. With ``availableNow``, ``query.awaitTermination()`` then ``join()``. The control
    table is created here, so a wrong name fails now rather than in the worker's log.
    ``granularity`` is keyword-only.
    """
    listener = FinalizationListener(
        spark, query.runId, control_table, table_name, granularity=granularity
    )
    ensure_control_table(spark, control_table)
    spark.streams.addListener(listener)
    # progress posted before the listener was added is missed: the latest one implies it
    listener._offer(query.lastProgress)
    if not query.isActive:  # terminated before the listener could hear it
        listener._stop()
    listener._worker.start()
    return listener
