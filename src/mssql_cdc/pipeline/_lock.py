"""Which snapshot a stream generation has open, and in which mode (ADR 0028): a snapshot of
either mode writes a 'snapshot_open' facts row before it reads the table, and its 'bootstrap'
or 'resnapshot' row closes it. A run, ``snapshot()``, ``seed()`` or ``backfill()`` that finds
one of the other mode still open raises (``_ModeLock._lock``)."""

from __future__ import annotations

import json
from contextlib import closing
from datetime import datetime
from typing import TYPE_CHECKING

from .. import events
from ..types import Offset, SnapshotMode
from ._common import _family, _iso, _log, _lost, _StreamBase

if TYPE_CHECKING:
    from ..payloads import SnapshotKind, SnapshotOpenDetail


def _unfinished(target: str, lsn: str, mode: str, generation: int | None, what: str) -> ValueError:
    """The error for a snapshot of ``mode`` still open at ``lsn``, saying how to finish it."""
    how = (
        "run backfill() until it is done, or rerun with snapshot='chunked'"
        if mode == "chunked"
        else "rerun with snapshot='full', which reads the table again. If no run is reading it "
        f"any more, it also stops counting once CDC cleanup passes {lsn}: it can then never "
        "complete"
    )
    return ValueError(
        f"{target} has a {mode} snapshot open at {lsn} (generation {generation}) that has not "
        f"completed: {what}. To finish it, {how}. Then either mode may be used (ADR 0028)."
    )


class _Opened(Offset):
    """A snapshot opened for a generation (``CdcStream._opened``): its offset and more."""

    mode: SnapshotMode
    generation: int | None
    done: bool
    """A 'bootstrap' or 'resnapshot' row of the generation at or after its LSN."""


class _ModeLock(_StreamBase):
    """The snapshots a stream has opened, and the lock that keeps a run in one mode."""

    def _opened(self, facts_table: str, target: str, sink_id: str) -> _Opened | None:
        """The snapshot opened for the generation whose sink app_id is ``sink_id``, open or
        complete: its chunked 'snapshot_open' row, else its newest full one (each full run
        writes its own). Its offset, ``mode``, ``generation`` and ``done`` (a 'bootstrap' or
        'resnapshot' row of the generation at or after its LSN); None when there is none."""
        rows = events.read(
            self.spark,
            facts_table,
            target,
            (events.SNAPSHOT_OPEN, *events.SNAPSHOTS),
            ("event", "min_lsn", "max_lsn", "min_commit_ts", "detail"),
            sink_id,
        )
        opens = [r for r in rows if r["event"] == events.SNAPSHOT_OPEN]
        if not opens:
            return None
        row = max(opens, key=lambda r: (events.mode(r["detail"]) == "chunked", r["min_lsn"]))
        return {
            "lsn": row["min_lsn"],
            "commit_ts": _iso(row["min_commit_ts"]),
            "mode": events.mode(row["detail"]),
            "generation": json.loads(row["detail"]).get("generation"),
            "done": any(lsn >= row["min_lsn"] for lsn in events.completions(rows)),
        }

    def _purged(self, lsn: str) -> bool:
        """CDC cleanup passed ``lsn``: a full snapshot opened there can never complete."""
        from ..client import make_client

        with closing(make_client(self.options)) as client:
            return _lost(client, self._capture_instance(), lsn) is not None

    def _reusable(
        self, facts_table: str, target: str, sink_id: str, chunked: bool
    ) -> _Opened | None:
        """The snapshot opened for generation ``sink_id`` that a run starts the generation
        from instead of taking one: a chunked one, open or complete; in a chunked run a full
        one too once complete (an emptied table's: no rows to find it by). A full one still
        being read raises, opened after this run's ``_lock``; one CDC cleanup has passed is
        dead, and a chunked run opens the generation over it. A full run takes a full one
        again: its rows are found as a whole snapshot, or it stopped before writing them."""
        opened = self._opened(facts_table, target, sink_id)
        if opened is None or opened["mode"] == "chunked" or (chunked and opened["done"]):
            return opened
        if not chunked or self._purged(opened["lsn"]):
            return None
        what = "no chunked snapshot is taken until it does"
        raise _unfinished(target, opened["lsn"], "full", opened["generation"], what)

    def _lock(self, facts_table: str, target: str, app_id: str, mode: str | None) -> None:
        """Raise when ``app_id``'s stream (any generation) has a snapshot open in ``target``
        in another mode than ``mode``, or in any mode when None: a 'snapshot_open' row with
        no 'bootstrap' or 'resnapshot' row at or after its LSN. A full one counts only while a
        run may still be reading it: not once CDC cleanup has passed its S, as it can then
        never complete, nor next to a chunked one of its generation, which stopped the full
        run that read it back (``_open``) or took the generation once it was dead. The mode
        may change between runs, never while a snapshot of the other mode is unfinished."""
        rows = events.read(
            self.spark,
            facts_table,
            target,
            (events.SNAPSHOT_OPEN, *events.SNAPSHOTS),
            ("app_id", "event", "max_lsn", "detail"),
            _family(app_id),
        )
        done = max(events.completions(rows), default=None)
        opens = [r for r in rows if r["event"] == events.SNAPSHOT_OPEN]
        taken = {r["app_id"] for r in events.chunked_opens(opens)}
        others = [
            r
            for r in opens
            if (done is None or r["max_lsn"] > done)
            and events.mode(r["detail"]) != mode
            and (
                events.mode(r["detail"]) == "chunked"
                or (r["app_id"] not in taken and not self._purged(r["max_lsn"]))
            )
        ]
        if others:
            top = max(others, key=lambda r: r["max_lsn"])
            what = (
                "seed() writes nothing over it"
                if mode is None
                else f"no {mode} snapshot is taken until it does"
            )
            generation = json.loads(top["detail"]).get("generation")
            raise _unfinished(target, top["max_lsn"], events.mode(top["detail"]), generation, what)

    def _open(
        self,
        target: str,
        ci: str,
        *,
        app_id: str,
        sink_id: str,
        facts_table: str,
        generation: int,
        kind: SnapshotKind,
        lost_from_ts: datetime | None = None,
        lost_to_ts: datetime | None = None,
        mode: SnapshotMode = "chunked",
    ) -> _Opened:
        """Open a ``kind`` snapshot ('bootstrap' or 'resnapshot') for generation
        ``generation`` (sink ``sink_id``) in ``mode``: record an LSN S (and, for a chunked
        one, what its chunks tile) in a 'snapshot_open' facts row before the table is read,
        and return the offset of the one stored, where a chunked generation's stream starts.
        Nothing is read from the table here: ``backfill()`` reads the chunks (ADR 0028), the
        caller a full one, stamped with its own LSN, at or after S.

        A chunked one once per generation: Delta skips a rerun's append, so the row is read
        back. A full one per run, at its own S, so that ``_lock`` tells a run still reading
        from one CDC cleanup has passed. A chunked one of the generation read back by a full
        run raises: a run opened it after this one's ``_reusable``."""
        from ..client import make_client, snapshot_plan
        from ..source import snapshot_lsn

        with closing(make_client(self.options)) as client:
            source = client.source_table(ci)
            lsn = snapshot_lsn(client, source)  # first: the plan's MIN and MAX come after S
            plan = snapshot_plan(client, ci, source) if mode == "chunked" else None
            offset: Offset = {"lsn": lsn, "commit_ts": client.lsn_to_time(lsn) or ""}
        detail: SnapshotOpenDetail = {
            "mode": mode,
            "kind": kind,
            "generation": generation,
            "lost_from_ts": _iso(lost_from_ts) or None,
            "lost_to_ts": _iso(lost_to_ts) or None,
        }
        if plan is not None:  # a chunked one's
            detail["keys"], detail["plan"] = source.keys, plan
        events.write_event(
            self.spark,
            facts_table,
            events.SNAPSHOT_OPEN,
            app_id=sink_id,
            txn_app_id=f"{app_id}#snapshots" if mode == "chunked" else None,
            version=generation,
            target=target,
            lsn=lsn,
            commit_ts=offset["commit_ts"],
            rows=0,
            lost_from_ts=lost_from_ts,
            lost_to_ts=lost_to_ts,
            detail=json.dumps(detail),
        )
        stored = self._opened(facts_table, target, sink_id) or {
            **offset,
            "mode": mode,
            "generation": generation,
            "done": False,
        }
        if stored["mode"] != mode:
            what = f"another run opened it before this one opened a {mode} one, which stops"
            raise _unfinished(target, stored["lsn"], stored["mode"], generation, what)
        _log.info(
            "mssql_cdc: %s snapshot of %s opened at %s for %s (%s, generation %s)",
            mode,
            target,
            stored["lsn"],
            sink_id,
            kind,
            generation,
        )
        return stored
