"""Chunked snapshots read next to the stream (ADR 0028): ``backfill()`` plans a snapshot's
chunks once, reads them in waves, appends each wave to the target in one commit and then
records its chunks in the facts; after a crash between the two, the facts come from the
commit. A wave is read while the one before it commits, and sized toward a duration; where
the platform refuses to cache it, its append reads it (ADR 0032)."""

from __future__ import annotations

import json
import math
import os
import time
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from .. import events
from .._metricsfs import join, list_json, read_json, remove
from ..types import BackfillState, BackfillStatus, Isolation
from ._common import _family, _iso, _log, _opt, _ts, _version
from ._lock import _ModeLock

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

    from ..client import CdcClient, SourceTable
    from ..payloads import (
        SnapshotCompletionDetail,
        SnapshotOpenDetail,
        SnapshotPlanDetail,
        WaveChunk,
        WaveMetadata,
    )
    from ..types import SparkSessionLike

# what backfill() reads of a chunked snapshot's facts rows (ADR 0028)
_SNAPSHOT_FACTS = (
    "app_id",
    "event",
    "rows",
    "min_lsn",
    "max_lsn",
    "min_commit_ts",
    "detail",
    "lost_from_ts",
    "lost_to_ts",
    "written_at",
    "duration_ms",
)

_WAVE_SECONDS = 300.0
"""``backfill(target_wave_seconds=None)``: on a production source, a wave of 4 chunks of
1,000,000 rows took 1 to 3 minutes to read, plus about 25 s of fixed cost (a Spark job, the
commit, the facts rows): a few minutes per wave amortises that (ADR 0028)."""


@dataclass(frozen=True, slots=True)
class _ChunkRead:
    """A chunk of a chunked snapshot in its target: what ``backfill()`` counts."""

    chunk: int
    wave: int
    last: bool
    """The plan's final chunk."""
    rows: int | None
    lsn: str
    """The stamp it was read under."""


@dataclass(frozen=True, slots=True)
class _Wave:
    """A wave read and cached, waiting for its commit (``_commit_wave``), which unpersists it;
    or, ``lazy`` where the platform refuses to cache (ADR 0032), not read yet: its append
    reads it, and its tag's counts and read metrics are None until then."""

    wave: int
    rows: DataFrame
    tag: WaveMetadata
    empty: bool
    started_at: datetime
    t0: float
    """``time.monotonic()`` when its read started."""
    lazy: bool = False
    metrics: str | None = None
    """A lazy wave's chunk metrics directory."""

    def release(self) -> None:
        if not self.lazy:  # serverless refuses unpersist too
            self.rows.unpersist()


@dataclass(frozen=True, slots=True)
class _Pending:
    """A wave committing in ``backfill()``'s background thread, and the chunks it read."""

    future: Future[list[_ChunkRead]]
    wave: int
    chunks: list[int]


def _wave_chunks(
    planned: list[list[Any]],
    every: list[list[Any]],
    counts: Mapping[int, int] | None,
    read: Mapping[int, Mapping[str, Any]],
    high: str | None,
) -> list[WaveChunk]:
    """The ``planned`` chunks of the plan ``every`` in a wave's userMetadata: ``counts`` (None
    before the wave is read), each chunk's metrics file in ``read``, and ``high``, ``max_lsn``
    after the read, for the chunks without one."""
    return [
        {
            "chunk": i,
            "lo": lo,
            "hi": hi,
            "last": i == len(every) - 1,
            "rows": None if counts is None else counts.get(i, 0),
            "high_lsn": read.get(i, {}).get("high_lsn") or high,
            "read_seconds": read[i]["seconds"] if i in read else None,
            "read_mb": round(read[i]["bytes"] / 1e6, 6) if i in read else None,
        }
        for i, lo, hi in planned
    ]


def _chunk_metrics(metrics: str | None) -> dict[int, dict[str, Any]]:
    """The wave's chunk metrics files in ``metrics``, by chunk; removed once read."""
    read: dict[int, dict[str, Any]] = {}
    names = list_json(metrics) if metrics else []
    for name in names:
        try:
            m = read_json(name)
            read[m["chunk"]] = m
        except (OSError, ValueError, KeyError):
            continue
    remove(names)
    return read


def _earlier_wave(spark: SparkSessionLike, target: str, key: str, wave: int) -> WaveMetadata | None:
    """The userMetadata of the commit that appended wave ``wave`` of ``key`` to ``target``,
    from its history; None once log cleanup has dropped it."""
    from pyspark.sql import functions as F

    from ..tables import delta_table

    found = delta_table(spark, target).history().where(F.col("userMetadata").contains(key))
    for (meta,) in found.select("userMetadata").collect():
        earlier: WaveMetadata = json.loads(meta)
        if earlier.get("backfill") == key and earlier.get("wave") == wave:
            return earlier
    return None


class _Chunked(_ModeLock):
    """``backfill()`` and its steps."""

    def backfill(
        self,
        target: str,
        *,
        app_id: str,
        facts_table: str,
        chunk_rows: int | None = None,
        max_waves: int | None = None,
        max_seconds: float | None = None,
        target_wave_seconds: float | None = None,
        min_headroom_hours: float | None = None,
        isolation: Isolation | None = None,
    ) -> BackfillStatus:
        """Read the newest open chunked snapshot of ``target`` (``to_delta(...,
        snapshot="chunked")`` opens one) in waves of chunks of at most about ``chunk_rows``
        rows, ``numPartitions`` at a time, next to the running stream; returns how far it
        got. Run it apart from the stream (its own task) and call it again until ``done``.

        The first call plans every chunk (``client.plan_chunks``: one integer key from row
        counts per slice of its range, the slices packed into chunks of at most ``chunk_rows``
        rows whatever the skew; other keys ``chunk_rows`` keys at a time) and records the plan
        in a 'snapshot_plan' facts row; later calls read it, so the chunks never change while
        the snapshot is open (but for the end of a keyset plan's last chunk, the first key after
        MAX: when there was none at planning, it is sought again before each wave).
        ``chunk_rows``: 1,000,000 when None; a later call's other value is ignored, with a
        warning. A full snapshot of the stream still being read raises (one mode while a
        snapshot is open).

        Each wave is stamped with ``max_lsn`` before it is read, at or after the snapshot's
        LSN S, appended to ``target`` in one commit (operation 0, ``_snapshot`` S,
        ``_chunk``) and then recorded as one 'snapshot_chunk' facts row per chunk; after the
        last chunk comes the snapshot's 'bootstrap' or 'resnapshot' row, which tells
        downstream to rebuild from S. After a crash between a wave's append and its facts
        rows, the rerun's append is skipped by Delta and its rows are rebuilt from the one
        committed (from its commit's userMetadata or, once log cleanup dropped that, from its
        rows in ``target``): nothing is appended twice. A wave is read while the one before
        it is appended and recorded, in a background thread, one wave at a time. Where the
        platform refuses to cache a wave (Databricks serverless, ADR 0032), its append reads
        it, once the one before is recorded, and its commit's userMetadata has no counts.

        A wave takes whole rounds of ``numPartitions`` chunks, partition p reading chunks p,
        p + numPartitions... one after the other: one round at first, then as many as
        ``target_wave_seconds`` (300 when None; 0 for one round) holds at the pace of the last
        wave read (an earlier call's from its facts row), and no more than the rest of
        ``max_seconds``. The plan's chunks and their order never change.

        ``app_id``: the stream's, as given to ``to_delta``. ``max_waves`` and ``max_seconds``
        bound one call. ``min_headroom_hours``: pause before a wave while the stream's
        retention headroom (of its newest facts row, less that row's age) is below it, or
        the stream has written none: the snapshot shares the link with the stream, and a
        stream the retention passes loses the snapshot too. ``isolation``: ``"snapshot"``
        reads the chunks and plans them (the counts and seeks of ``plan_chunks``, the first
        key after MAX of ``last_bound``) under SNAPSHOT isolation, which the database must
        allow; ``"readCommitted"`` under READ COMMITTED. None: the stream's ``isolationLevel``
        option, READ COMMITTED without one. Either is matched ignoring case.

        Returns a ``BackfillStatus``, ``{"snapshot", "chunks_done", "chunks_total", "done",
        "paused", "state", "reason"}``: ``chunks_total`` is the plan's, None until a call has
        planned it.
        ``state`` says why the call returned, ``reason`` the same in words: ``"done"``,
        ``"running"`` (``max_waves`` or ``max_seconds`` stopped it: call again),
        ``"waiting_headroom"`` (below ``min_headroom_hours``), ``"waiting_metrics"`` (the
        stream has written no facts row with ``retention_headroom_hours``) or
        ``"no_snapshot"`` (no chunked snapshot opened by ``app_id``'s stream in ``target``:
        not yet, or a wrong ``app_id``, ``target`` or ``facts_table``).
        """
        from pyspark import inheritable_thread_target

        from .. import sink
        from ..client import last_bound, make_client
        from ..source import ISOLATION_LEVELS, snapshot_lsn
        from ..spark import available_cores

        if chunk_rows is not None and int(chunk_rows) < 1:
            raise ValueError(f"chunk_rows must be at least 1, not {chunk_rows}")
        target_s = _WAVE_SECONDS if target_wave_seconds is None else float(target_wave_seconds)
        if not target_s >= 0:  # NaN too
            raise ValueError(f"target_wave_seconds must be at least 0, not {target_wave_seconds}")
        asked = isolation if isolation is not None else _opt(self.options, "isolationLevel")
        level = str(asked or "readCommitted").strip().lower()
        if level not in ISOLATION_LEVELS:
            raise ValueError(f"isolation must be 'snapshot' or 'readCommitted', not {asked!r}")
        isolated = ISOLATION_LEVELS[level]  # as client._isolated takes it
        ci, t0 = self._capture_instance(), time.monotonic()
        ours = _family(app_id)
        kinds = (
            events.SNAPSHOT_OPEN,
            events.SNAPSHOT_PLAN,
            events.SNAPSHOT_CHUNK,
            *events.SNAPSHOTS,
        )

        def read_facts() -> list[Row]:
            return events.read(self.spark, facts_table, target, kinds, _SNAPSHOT_FACTS, ours)

        # a full snapshot of the stream still being read: no plan, no wave
        self._lock(facts_table, target, app_id, "chunked")
        rows = read_facts()
        opens = events.chunked_opens(rows)
        if not opens:
            return {
                "snapshot": None,
                "chunks_done": 0,
                "chunks_total": None,
                "done": False,
                "paused": True,
                "state": "no_snapshot",
                "reason": f"{target} has no chunked snapshot opened by {app_id}'s stream",
            }
        top = max(opens, key=lambda r: r["min_lsn"])  # a newer open supersedes an older one
        s, sink_id = top["min_lsn"], top["app_id"]
        info: SnapshotOpenDetail = json.loads(top["detail"])
        plan = events.plan_of(rows, s)
        if plan and chunk_rows is not None and int(chunk_rows) != plan["chunk_rows"]:
            _log.warning(
                "mssql_cdc: backfill(chunk_rows=%s) ignored: the snapshot at %s of %s was "
                "planned with chunk_rows=%s, and its chunks do not change while it is open",
                chunk_rows,
                s,
                target,
                plan["chunk_rows"],
            )
        chunks: list[_ChunkRead] = []
        for d, r in events.chunks_of(rows, s):
            c = _ChunkRead(d["chunk"], d["wave"], d.get("last", False), r["rows"], r["min_lsn"])
            chunks.append(c)
        chunks.sort(key=lambda c: c.chunk)
        status: BackfillStatus = {
            "snapshot": s,
            "chunks_done": len(chunks),
            "chunks_total": None,
            "done": False,
            "paused": False,
            "state": "running",
            "reason": None,
        }
        # its own row, or a newer whole snapshot's (a full re-snapshot) that supersedes it
        if any(lsn >= s for lsn in events.completions(rows)):
            return {**status, "chunks_total": len(chunks), "done": True, "state": "done"}
        given = str(_opt(self.options, "numPartitions") or "auto").strip().lower()
        k = int(given) if given != "auto" else available_cores(self.spark) or os.cpu_count() or 1
        k = max(1, k)
        base = _opt(self.options, "metricsPath")  # not the stream's own directory: it folds those
        metrics = join(base, f"{sink_id}.backfill") if base else None
        # seconds per round of k chunks of the last wave read: at first an earlier call's, from
        # its facts rows' duration (its read and its append)
        pace: float | None = None
        if chunks:
            ms = [
                r["duration_ms"]
                for d, r in events.chunks_of(rows, s)
                if d["wave"] == chunks[-1].wave
            ]
            pace = ms[0] / 1000 / math.ceil(len(ms) / k) if ms[0] else None

        def width(total: int) -> int:
            """How many chunks the next wave takes: whole rounds of k, as many as the target
            holds at ``pace`` and the rest of ``max_seconds`` too, at least one."""
            per_round = pace
            if not per_round:
                return k
            rounds = target_s / per_round
            if max_seconds is not None:
                rounds = min(rounds, (max_seconds - (time.monotonic() - t0)) / per_round)
            return k * max(1, int(min(rounds, total)))

        waves = 0
        cache = True  # until the platform refuses to cache a wave (ADR 0032)
        pending: _Pending | None = None

        def settle() -> bool:
            """Wait for the wave committing in the background, if any; whether its commit holds
            the chunks it read (an earlier attempt's may hold others: ``_committed``)."""
            nonlocal pending
            if pending is None:
                return True
            was, pending = pending, None
            held = was.future.result()
            chunks.extend(held)
            return [c.chunk for c in held] == was.chunks

        with closing(make_client(self.options)) as client:
            source = client.source_table(ci)
            if source.keys != info["keys"]:
                raise ValueError(
                    f"The unique index of {source.schema}.{source.table} is now on {source.keys}, "
                    f"not on {info['keys']} as when the snapshot at {s} opened: its chunks no "
                    "longer tile the table. Take a new snapshot."
                )
            # A wave is read while the one before commits in the background (its append, then
            # its facts rows), one commit at a time: bronze takes the waves in order. The
            # commits look up commit times on a connection of their own.
            with closing(make_client(self.options)) as clock, ThreadPoolExecutor(1) as committer:
                # its jobs inherit the caller's job group and scheduler pool. Without Connect or
                # pinned threads there is nothing to inherit, and PySpark 4.0 and 4.1 hand the
                # session back instead of a decorator
                wrap = inheritable_thread_target(cast("SparkSession", self.spark))
                inherit = wrap if callable(wrap) else (lambda f: f)
                while True:
                    if plan and pending and pending.chunks[-1] == len(plan["chunks"]) - 1:
                        settle()  # the plan's last chunk: nothing left to read ahead
                        continue
                    if chunks and chunks[-1].last:
                        break
                    if (max_waves is not None and waves >= max_waves) or (
                        max_seconds is not None and time.monotonic() - t0 >= max_seconds
                    ):
                        break
                    waiting = self._throttle(facts_table, sink_id, min_headroom_hours)
                    if waiting:
                        status["paused"] = True
                        status["state"], status["reason"] = waiting
                        break
                    if plan is None:
                        self._plan(
                            client=client,
                            ci=ci,
                            source=source,
                            info=info,
                            top=top,
                            target=target,
                            facts_table=facts_table,
                            app_id=app_id,
                            chunk_rows=chunk_rows,
                            isolation=isolated,
                        )
                        # a concurrent call's, if Delta skipped ours
                        plan = events.plan_of(read_facts(), s)
                        assert plan is not None  # written just now
                    t1 = time.monotonic()
                    lsn = snapshot_lsn(client, source)  # the wave's stamp, before its read
                    if lsn < s:
                        raise RuntimeError(
                            f"sys.fn_cdc_get_max_lsn() is {lsn}, below the snapshot's {s}: is this "
                            "the database the snapshot was opened on (not a readable secondary)?"
                        )
                    every = plan["chunks"]
                    extent = info["plan"]
                    if (
                        every[-1][1] is None
                        and extent["kind"] == "keyset"
                        and extent["max"] is not None
                    ):
                        # a keyset plan's last chunk ends at the first key after MAX, and there
                        # was none at planning: sought again, so that rows inserted above MAX
                        # since, the stream's, do not pile up in the last chunk
                        end = last_bound(client, ci, source, extent["max"], isolated)
                        every = [*every[:-1], [every[-1][0], end]]
                    # after the wave committing, as if it holds the chunks it read
                    if pending:
                        first, wave = pending.chunks[-1] + 1, pending.wave + 1
                    else:
                        first = chunks[-1].chunk + 1 if chunks else 0
                        wave = chunks[-1].wave + 1 if chunks else 0
                    stop = min(first + width(len(every)), len(every))
                    planned = [[i, *every[i]] for i in range(first, stop)]
                    _log.info(
                        "mssql_cdc: backfill of %s (snapshot %s, %s): wave %s, chunks %s to %s "
                        "of %s, stamped %s",
                        target,
                        s,
                        sink_id,
                        wave,
                        planned[0][0],
                        planned[-1][0],
                        len(every),
                        lsn,
                    )
                    read = self._read_wave(
                        app_id=app_id,
                        snapshot=s,
                        wave=wave,
                        lsn=lsn,
                        planned=planned,
                        every=every,
                        k=k,
                        client=client,
                        metrics=metrics,
                        isolation=isolated,
                        cache=cache,
                    )
                    cache = not read.lazy  # a platform that refused once is not asked again
                    took = time.monotonic() - t1  # its read, not the wait for the commit below
                    try:
                        held = settle()  # the wave before: committed and recorded first
                    except BaseException:
                        read.release()
                        raise
                    if not held:  # it read other chunks than its commit holds: read again
                        read.release()
                        continue
                    pace = took / math.ceil(len(planned) / k)
                    commit = partial(
                        self._commit_wave,
                        read,
                        target=target,
                        app_id=app_id,
                        facts_table=facts_table,
                        sink_id=sink_id,
                        snapshot=s,
                        every=every,
                        clock=clock,
                    )
                    future = committer.submit(inherit(commit))
                    pending = _Pending(future, wave, [i for i, *_ in planned])
                    waves += 1
                    if read.lazy:  # its append is its read: nothing to read ahead of it
                        settle()
                        pace = (time.monotonic() - t1) / math.ceil(len(planned) / k)
                settle()
        done = bool(chunks) and chunks[-1].last
        if done:  # also after a crash between the last wave's facts and this row
            rows_in = sum(c.rows or 0 for c in chunks)
            completion: SnapshotCompletionDetail = {
                "snapshot": s,
                "chunks": len(chunks),
                "rows": rows_in,
                "last_lsn": max(c.lsn for c in chunks),
            }
            events.write_event(
                self.spark,
                facts_table,
                # a key added later is read with a default (ADR 0021): an open written before
                # 'kind' (an unreleased shape put it in 'mode') is generation 0's bootstrap or a
                # later generation's re-snapshot (ADR 0018)
                info.get("kind") or (events.RESNAPSHOT if info["generation"] else events.BOOTSTRAP),
                app_id=sink_id,
                txn_app_id=f"{app_id}#events",
                version=info["generation"],
                target=target,
                lsn=s,
                commit_ts=_iso(top["min_commit_ts"]),
                rows=rows_in,
                started_at=top["written_at"],  # from the open to the last chunk
                duration_ms=round((sink._utc_now() - top["written_at"]).total_seconds() * 1000),
                lost_from_ts=top["lost_from_ts"],
                lost_to_ts=top["lost_to_ts"],
                detail=json.dumps(completion),
            )
        total = len(chunks) if done else len(plan["chunks"]) if plan else None
        if done:
            status["state"] = "done"
        return {**status, "chunks_done": len(chunks), "chunks_total": total, "done": done}

    def _plan(
        self,
        *,
        client: CdcClient,
        ci: str,
        source: SourceTable,
        info: SnapshotOpenDetail,
        top: Row,
        target: str,
        facts_table: str,
        app_id: str,
        chunk_rows: int | None,
        isolation: str | None,
    ) -> None:
        """Plan every chunk of the snapshot opened by facts row ``top`` (``info``, its detail),
        under ``isolation``, and record them in its 'snapshot_plan' row, once: Delta skips a
        rerun's."""
        from .. import sink
        from ..client import plan_chunks

        s, rows = top["min_lsn"], int(chunk_rows or 1_000_000)
        started_at, t0 = sink._utc_now(), time.monotonic()
        detail: SnapshotPlanDetail = {
            "snapshot": s,
            "kind": info["plan"]["kind"],
            "keys": source.keys,
            "chunk_rows": rows,
            "chunks": plan_chunks(client, ci, source, info["plan"], rows, isolation),
        }
        events.write_event(
            self.spark,
            facts_table,
            events.SNAPSHOT_PLAN,
            app_id=top["app_id"],
            txn_app_id=f"{app_id}#snapplan.{s}",
            version=0,
            target=target,
            lsn=s,
            commit_ts=_iso(top["min_commit_ts"]),
            rows=0,
            started_at=started_at,
            duration_ms=round((time.monotonic() - t0) * 1000),
            detail=json.dumps(detail),
        )

    def _throttle(
        self, facts_table: str, sink_id: str, min_headroom_hours: float | None
    ) -> tuple[BackfillState, str] | None:
        """Why ``backfill()`` pauses now, as its ``(state, reason)``, or None: the retention
        headroom of the stream ``sink_id``'s newest facts row, less that row's age, below
        ``min_headroom_hours``, or no such row."""
        if min_headroom_hours is None:
            return None
        from pyspark.sql import functions as F

        from .. import sink
        from ..tables import delta_table

        last = (
            delta_table(self.spark, facts_table)
            .toDF()
            .where(
                (F.col("app_id") == sink_id)
                & F.col("batch_id").isNotNull()
                & F.col("event").isNull()
            )
            .orderBy(F.col("written_at").desc())
            .select("retention_headroom_hours", "written_at")
            .first()
        )
        if last is None or last["retention_headroom_hours"] is None:
            return (
                "waiting_metrics",
                (
                    f"the stream {sink_id} has written no facts row with "
                    "retention_headroom_hours: is it running, with metrics?"
                ),
            )
        age = (sink._utc_now() - last["written_at"]).total_seconds() / 3600
        left = last["retention_headroom_hours"] - age
        if left < min_headroom_hours:
            return (
                "waiting_headroom",
                (
                    f"retention headroom {left:.2f} h (the stream's newest facts row, "
                    f"{age:.2f} h old) is below min_headroom_hours={min_headroom_hours}"
                ),
            )
        return None

    def _read_wave(
        self,
        *,
        app_id: str,
        snapshot: str,
        wave: int,
        lsn: str,
        planned: list[list[Any]],
        every: list[list[Any]],
        k: int,
        client: CdcClient,
        metrics: str | None,
        isolation: str | None,
        cache: bool,
    ) -> _Wave:
        """Read the ``planned`` chunks stamped ``lsn``, ``k`` at a time, into cached rows, with
        the tag their commit will carry; ``every``: the plan's chunks. Without ``cache``, or
        where the platform refuses to cache, a lazy wave that its append reads (ADR 0032)."""
        from pyspark.sql import functions as F

        from ..sink import _cached, _utc_now, bronze_rows

        drop = ("snapshotchunks", "snapshotkeys", "snapshotlsn", "metricspath", "isolationlevel")
        # one partition per chunk, in this order; coalesced below, partition p reads chunks p,
        # p + k...: neighbours are read side by side, as in a wave of k
        order = [c for p in range(k) for c in planned[p::k]]
        reader = (
            self.spark.read.format("mssql_cdc_snapshot")
            .options(**{o: v for o, v in self.options.items() if o.lower() not in drop})
            .option("snapshotChunks", json.dumps(order))
            .option("snapshotLsn", lsn)
        )
        if metrics:
            reader = reader.option("metricsPath", metrics)
        if isolation:
            reader = reader.option("isolationLevel", isolation)
        started_at, t0 = _utc_now(), time.monotonic()
        df = reader.load()
        if len(planned) > k:  # k connections at a time, whatever the wave's size
            df = df.coalesce(k)
        lazy = bronze_rows(df, snapshot=F.lit(snapshot))
        rows = _cached(lazy) if cache else None
        tag: WaveMetadata = {
            "backfill": f"{app_id}#snap.{snapshot}",
            "wave": wave,
            "lsn": lsn,
            "attempt": uuid4().hex,
            "chunks": _wave_chunks(planned, every, None, {}, None),
        }
        if rows is None:  # the wave before may be reading: its metrics files stay
            return _Wave(wave, lazy, tag, False, started_at, t0, lazy=True, metrics=metrics)
        try:
            if metrics:
                remove(list_json(metrics))  # a dead attempt's
            # reads the wave, once: the write takes the cached rows
            counts: dict[int, int] = dict(rows.groupBy("_chunk").count().collect())
            high = client.max_lsn()  # how far capture had got after the read: informational
            # folded into the tag; the next wave writes its own meanwhile
            tag["chunks"] = _wave_chunks(planned, every, counts, _chunk_metrics(metrics), high)
        except BaseException:
            rows.unpersist()
            raise
        return _Wave(wave, rows, tag, not counts, started_at, t0)

    def _commit_wave(
        self,
        w: _Wave,
        *,
        target: str,
        app_id: str,
        facts_table: str,
        sink_id: str,
        snapshot: str,
        every: list[list[Any]],
        clock: CdcClient,
    ) -> list[_ChunkRead]:
        """Append wave ``w`` to ``target``, unpersist it and record its chunks in the facts,
        their commit times looked up on ``clock``; ``every``: the plan's chunks. Returns those of
        the commit that holds the wave, which an earlier attempt may have made with other chunks
        (``_committed``). Runs in ``backfill()``'s background thread."""
        from ..sink import _utc_now, write_facts
        from ..tables import exists

        tag, started_at, t0 = w.tag, w.started_at, w.t0
        try:
            if w.lazy:  # read by its append, timed from there
                started_at, t0 = _utc_now(), time.monotonic()
                tag = self._append_read(w, target, snapshot, every, clock)
            elif not w.empty:  # an empty wave writes no commit
                tag = self._append_wave(target, snapshot, w.rows, tag, every)
            elif exists(self.spark, target):  # though an earlier attempt that read rows may have
                tag = self._committed(target, snapshot, tag, every) or tag
        finally:
            w.release()
        times: dict[str | None, datetime | None] = {}

        def at(x: str | None) -> datetime | None:
            if x not in times:
                times[x] = _ts(clock.lsn_to_time(x)) if x else None
            return times[x]

        duration_ms = round((time.monotonic() - t0) * 1000)
        write_facts(
            self.spark,
            facts_table,
            [
                events.chunk_row(
                    c,
                    app_id=sink_id,
                    target=target,
                    snapshot=snapshot,
                    wave=w.wave,
                    lsn=tag["lsn"],
                    lsn_ts=at(tag["lsn"]),
                    high_ts=at(c["high_lsn"]),
                    started_at=started_at,
                    duration_ms=duration_ms,
                )
                for c in tag["chunks"]
            ],
            f"{app_id}#snapchunks.{snapshot}",
            w.wave,
        )
        held = [
            _ChunkRead(c["chunk"], w.wave, c.get("last", False), c["rows"], tag["lsn"])
            for c in tag["chunks"]
        ]
        _log.info(
            "mssql_cdc: backfill of %s (snapshot %s, %s): wave %s in, %s rows in %.1f s",
            target,
            snapshot,
            sink_id,
            w.wave,
            sum(c.rows or 0 for c in held),
            time.monotonic() - t0,
        )
        return held

    def _append_read(
        self, w: _Wave, target: str, snapshot: str, every: list[list[Any]], clock: CdcClient
    ) -> WaveMetadata:
        """Append lazy wave ``w``, which reads it (ADR 0032): each chunk's rows counted on the
        way, its read metrics from its file and ``high_lsn`` from ``clock`` after the append.
        Returns the tag of the commit that holds the wave, as ``_append_wave``."""
        from pyspark.sql import Observation
        from pyspark.sql import functions as F

        if w.metrics:
            remove(list_json(w.metrics))  # a dead attempt's: the wave before is in
        seen = Observation()
        counts = [
            F.count(F.when(F.col("_chunk") == c["chunk"], 1)).alias(str(c["chunk"]))
            for c in w.tag["chunks"]
        ]
        tag = self._append_wave(target, snapshot, w.rows.observe(seen, *counts), w.tag, every)
        if tag is not w.tag:  # an earlier attempt's commit: Delta read nothing
            return tag
        rows = {int(i): n for i, n in seen.get.items()}
        if not rows:  # a Spark Connect server answered the write before its metrics were in
            held = self._held(target, snapshot, tag["chunks"][0]["chunk"]) or {}
            rows = {i: n for i, (n, _) in held.items()}
        planned = [[c["chunk"], c["lo"], c["hi"]] for c in tag["chunks"]]
        read, high = _chunk_metrics(w.metrics), clock.max_lsn()
        return {**tag, "chunks": _wave_chunks(planned, every, rows, read, high)}

    def _append_wave(
        self, target: str, snapshot: str, rows: DataFrame, tag: WaveMetadata, every: list[list[Any]]
    ) -> WaveMetadata:
        """Append a wave's ``rows`` to ``target`` in one commit with ``tag`` as its
        userMetadata, once per (snapshot, wave). Returns the tag of the commit that holds
        them: this one's or, when Delta skipped the append, the earlier attempt's
        (``_committed``)."""
        from .. import migrations
        from ..sink import BRONZE_COMMENT, _write, bronze_columns
        from ..tables import commit_after

        key, wave, text = tag["backfill"], tag["wave"], json.dumps(tag)
        for attempt in (0, 1):
            try:
                migrations.ensure(
                    self.spark, target, "bronze", bronze_columns(rows), BRONZE_COMMENT
                )
                before = _version(self.spark, target)
                _write(rows, target, key, wave, text, merge_schema=True)
                break
            except Exception as exc:
                # the stream commits too: creating the table, or a schema change (mergeSchema)
                flat = str(exc).replace("_", "").lower()
                if attempt or not any(s in flat for s in ("metadatachanged", "protocolchanged")):
                    raise
        if commit_after(self.spark, target, before, text) is not None:
            return tag
        earlier = self._committed(target, snapshot, tag, every)
        if earlier is None:
            raise RuntimeError(
                f"Delta skipped wave {wave} of the snapshot at {snapshot} as appended to {target} "
                f"already (txnAppId {key}), but neither {target}'s history nor its rows hold it"
            )
        return earlier

    def _held(self, target: str, snapshot: str, first: int) -> dict[int, tuple[int, str]] | None:
        """The rows and the lowest stamp of each chunk from ``first`` on of the snapshot at
        ``snapshot`` in ``target``; None when no chunk was ever appended to it."""
        from pyspark.sql import functions as F

        from ..tables import delta_table

        df = delta_table(self.spark, target).toDF()
        if "_chunk" not in df.columns:  # bronze migration 2: no chunk ever appended
            return None
        found = (
            df.where(
                (F.col("_operation") == 0)
                & (F.col("_snapshot") == snapshot)
                & (F.col("_chunk") >= first)
            )
            .groupBy("_chunk")
            .agg(F.count(F.lit(1)), F.min("_start_lsn"))
            .collect()
        )
        return {i: (n, low) for i, n, low in found}

    def _committed(
        self, target: str, snapshot: str, tag: WaveMetadata, every: list[list[Any]]
    ) -> WaveMetadata | None:
        """The tag of the commit an earlier attempt of ``tag``'s wave made to ``target``
        before it stopped short of its facts rows: from the history (``_earlier_wave``) or,
        once log cleanup has dropped it, rebuilt from its rows. None when there is none.

        The rebuild takes the chunks from the wave's first to the last that has rows in
        ``target``, whatever the rerun planned (its wave may take other chunks): bounds from
        ``every`` (the plan's), the stamp and the row counts from the rows, ``read_seconds``
        and ``read_mb`` NULL. The attempt's empty chunks after them are read again by the
        next wave."""
        earlier = _earlier_wave(self.spark, target, tag["backfill"], tag["wave"])
        if earlier and None not in (c["rows"] for c in earlier["chunks"]):
            return earlier
        first = (earlier or tag)["chunks"][0]["chunk"]  # the earlier waves' are all below it
        held = self._held(target, snapshot, first)
        if held is None:
            return None
        if earlier:  # a tag written before its wave was read (ADR 0032): counted in its rows
            stamp = earlier["lsn"]
            counted: list[WaveChunk] = [
                {**c, "rows": held.get(c["chunk"], (0,))[0], "high_lsn": c["high_lsn"] or stamp}
                for c in earlier["chunks"]
            ]
            return {**earlier, "chunks": counted}
        if not held:
            return None
        lsn = min(low for _, low in held.values())  # the wave's stamp, on every row
        chunks: list[WaveChunk] = [
            {
                "chunk": i,
                "lo": every[i][0],
                "hi": every[i][1],
                "last": i == len(every) - 1,
                "rows": held.get(i, (0,))[0],
                "high_lsn": lsn,
                "read_seconds": None,
                "read_mb": None,
            }
            for i in range(first, max(held) + 1)
        ]
        return {**tag, "lsn": lsn, "chunks": chunks}
