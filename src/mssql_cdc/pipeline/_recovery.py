"""Recovery from CDC data loss before the query starts (ADR 0018): stream generations, the
state file that names the live one, the checkpoint's last offset, and the re-snapshot."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Any

from .. import events
from ..types import Offset
from ._common import _URI, _iso, _log, _lost, _opt, _ts
from ._snapshots import _Snapshots

_STATE = "_mssql_cdc_generation.json"


def _generation(checkpoint: str, app_id: str, n: int) -> tuple[str, str]:
    """Spark checkpoint and sink app_id of generation ``n``; 0 is the caller's own."""
    if not n:
        return checkpoint, app_id
    return os.path.join(checkpoint, "_generations", str(n)), f"{app_id}.g{n}"


def _read_state(checkpoint: str) -> dict[str, Any] | None:
    """The generation state; None is generation 0 (always, for a URI checkpoint)."""
    path = os.path.join(checkpoint, _STATE)
    if _URI.match(checkpoint) or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        state: dict[str, Any] = json.load(fh)  # _write_state's object
        return state


def _write_state(checkpoint: str, state: Mapping[str, Any]) -> None:
    path = os.path.join(checkpoint, _STATE)
    os.makedirs(checkpoint, exist_ok=True)
    with open(path + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(state, fh)
    os.replace(path + ".tmp", path)  # a crash leaves the old state or the new one


def _last_offset(checkpoint: str) -> Offset | None:
    """The source offset of the checkpoint's last committed batch: its last processed LSN.

    Spark's offset log ("v1"): ``offsets/<batch>`` holds the version, the batch metadata and
    then the offset of each source; ``commits/<batch>`` appears once the batch is done.
    """
    try:
        names = os.listdir(os.path.join(checkpoint, "commits"))
    except FileNotFoundError:
        return None
    last = max((int(name) for name in names if name.isdigit()), default=None)
    if last is None:
        return None
    with open(os.path.join(checkpoint, "offsets", str(last)), encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    if lines[0] != "v1":
        raise ValueError(
            f"{checkpoint}: Spark offset log version {lines[0]!r}; the "
            "on_data_loss='resnapshot' pre-flight reads only 'v1' (ADR 0018)"
        )
    offset: Offset = json.loads(lines[2])  # this source's: the stream's only one
    return offset


class _Recovery(_Snapshots):
    """The re-snapshot ``to_delta(on_data_loss="resnapshot")`` takes before it starts."""

    def _recover(
        self,
        target: str,
        app_id: str,
        checkpoint: str,
        facts_table: str,
        state: dict[str, Any] | None,
        interval_days: float,
        bootstrap: bool,
        chunked: bool = False,
    ) -> dict[str, Any] | None:
        """Before the query starts: when CDC cleanup deleted changes the current generation
        has not read, re-snapshot and return the next generation's state; None otherwise.
        ``chunked``: open a chunked snapshot instead (ADR 0028); the next generation starts at
        its LSN at once, and ``backfill()`` reads it and writes its 'resnapshot' row."""
        from ..client import DataLossError, make_client
        from ..lsn import ZERO_LSN, normalize
        from ..sink import _last_batch
        from ..tables import exists

        ci = self._capture_instance()
        state = state or {"generation": 0}
        n = state["generation"]
        spark_checkpoint, sink_id = _generation(checkpoint, app_id, n)
        # a recovery that stopped before its new state was written resumes from what it found
        start: Offset | None = state.get("recovering") or _last_offset(spark_checkpoint)
        if (
            start is None
            # Spark writes metadata when the query starts: seen, the first batch did not commit
            and not os.path.exists(os.path.join(spark_checkpoint, "metadata"))
            and exists(self.spark, facts_table)
            and _last_batch(self.spark, facts_table, sink_id) is not None
        ):
            raise ValueError(
                f"{facts_table} holds batches of {sink_id}, but Python cannot see Spark's commits "
                f"under {spark_checkpoint}. Either the two resolve that path differently (a "
                "schemeless /mnt/ or DBFS-root path on Databricks classic, an HDFS default file "
                "system, a Spark Connect client machine): use a path both resolve alike (local, "
                "or a Volume). Or the checkpoint was deleted: start it again with a new app_id."
            )
        if start is None:  # nothing committed yet: where the generation's stream starts
            given = (_opt(self.options, "startingLsn") or "").strip()
            if n:
                start = {"lsn": state["snapshot_lsn"], "commit_ts": state["commit_ts"]}
            elif bootstrap:  # as _bootstrap finds it
                start = self._last_snapshot(target, ci) or self._reusable(
                    facts_table, target, app_id, chunked
                )
            elif given.lower() not in ("", "earliest", "latest"):
                start = {"lsn": normalize(given), "commit_ts": ""}
        if start is None:
            return None
        next_id = _generation(checkpoint, app_id, n + 1)[1]
        with closing(make_client(self.options)) as client:
            # the source's retention guard, which runs only when there is a range to read
            if (client.max_lsn() or ZERO_LSN) <= start["lsn"]:
                return None
            low = _lost(client, ci, start["lsn"])
            if low is None:
                return None
            lost_to = _ts(client.lsn_to_time(low))
            # opened by a recovery that stopped before writing its state
            done: Offset | None = self._reusable(facts_table, target, next_id, chunked)
            # purged too, as a full re-snapshot's would be: open past it, a generation on
            while done and _lost(client, ci, done["lsn"]):
                n, next_id = n + 1, _generation(checkpoint, app_id, n + 2)[1]
                done = self._reusable(facts_table, target, next_id, chunked)
            if not (chunked or done):
                # newer than the checkpoint and not purged itself: a recovery that stopped
                # before writing its state
                done = self._last_snapshot(target, ci)
                if done and (done["lsn"] <= start["lsn"] or _lost(client, ci, done["lsn"])):
                    done = None
        lost = (
            f"{ci}: CDC no longer holds the changes after {start['lsn']} that the stream has "
            f"not read (min_lsn of the capture instance that held them is {low}): cleanup "
            "purged them, or that capture instance was dropped"
        )
        last = state.get("failed_at") or (state.get("at") if n else None)
        now = datetime.now(timezone.utc)
        if not done and last and now - datetime.fromisoformat(last) < timedelta(days=interval_days):
            raise DataLossError(
                f"{lost}, and the last automatic re-snapshot (or a failed one) was at {last}, "
                f"less than resnapshot_interval_days={interval_days} ago. The stream does not "
                "keep up with the CDC retention, or stops for longer than it, again: fix that, "
                "then rerun with a smaller resnapshot_interval_days to re-snapshot now."
            )
        _log.warning(
            "mssql_cdc: %s (commits from %s to %s). Re-snapshotting %s (%s) into generation %s, "
            "sink app_id %s.",
            lost,
            start["commit_ts"] or start["lsn"],
            _iso(lost_to) or low,
            target,
            "chunked" if chunked else "full",
            n + 1,
            next_id,
        )
        _write_state(checkpoint, {**state, "recovering": start})
        offset: Offset
        if chunked:  # nothing read here, so nothing to purge meanwhile; a newer open supersedes
            offset = done or self._open(
                target,
                ci,
                app_id=app_id,
                sink_id=next_id,
                facts_table=facts_table,
                generation=n + 1,
                kind="resnapshot",
                lost_from_ts=_ts(start["commit_ts"]),
                lost_to_ts=lost_to,
            )
        else:
            if not done:
                self._open(
                    target,
                    ci,
                    app_id=app_id,
                    sink_id=next_id,
                    facts_table=facts_table,
                    generation=n + 1,
                    kind="resnapshot",
                    lost_from_ts=_ts(start["commit_ts"]),
                    lost_to_ts=lost_to,
                    mode="full",
                )
            offset, timing = (done, {}) if done else self._take_snapshot(target, ci)
            with closing(make_client(self.options)) as client:
                if _lost(client, ci, offset["lsn"]):
                    failed = {"failed_at": now.isoformat(timespec="seconds")}
                    _write_state(checkpoint, {**state, "recovering": start, **failed})
                    raise DataLossError(
                        f"{lost}. The re-snapshot at {offset['lsn']} took longer than the CDC "
                        "retention, so the changes after it are gone too: lengthen the retention "
                        "or speed up the read (numPartitions), or read it next to the stream "
                        "with snapshot='chunked', which has no such limit (ADR 0028); then "
                        "rerun with resnapshot_interval_days=0."
                    )
            events.write_event(
                self.spark,
                facts_table,
                events.RESNAPSHOT,
                app_id=next_id,
                txn_app_id=f"{app_id}#events",
                version=n + 1,
                target=target,
                lsn=offset["lsn"],
                commit_ts=offset["commit_ts"],
                **timing,
                lost_from_ts=_ts(start["commit_ts"]),
                lost_to_ts=lost_to,
            )
        n += 1
        state = {
            "generation": n,
            "snapshot_lsn": offset["lsn"],
            "commit_ts": offset["commit_ts"],
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        _write_state(checkpoint, state)
        _log.info(
            "mssql_cdc: %s streams generation %s (sink app_id %s) from %s",
            target,
            n,
            next_id,
            offset["lsn"],
        )
        return state
