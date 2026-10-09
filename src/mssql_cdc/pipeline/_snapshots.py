"""Whole snapshots of the tracked table (ADR 0016): ``snapshot()``, ``seed()`` (ADR 0025)
and a bootstrap's, each appended to the target in one commit and found again by its rows."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from contextlib import closing
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .. import events
from ..types import Offset
from ._common import _instances, _iso, _log, _lost, _version
from ._lock import _ModeLock

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


class _Snapshots(_ModeLock):
    """Whole snapshots: taken, seeded, found again, and a bootstrap's."""

    def snapshot(
        self,
        target: str,
        *,
        resnapshot: bool = False,
        app_id: str | None = None,
        facts_table: str | None = None,
    ) -> Offset:
        """Append the tracked table's current rows to ``target`` as operation 0 and return the
        offset they are stamped with (an ``Offset``), for the stream's ``startingLsn``.

        The LSN is recorded before the table is read. Once ``target`` holds a snapshot taken
        under any capture instance of this table, its offset is returned and nothing is read,
        so a rerun cannot skip the changes after it: a chunked one's S too, open or complete,
        once its first wave is in (ADR 0028). ``resnapshot=True`` takes a new one:
        after a DataLossError, with a new checkpoint and app_id; downstream, rebuild from the
        newest snapshot.
        ``to_delta(on_data_loss="resnapshot")`` does all of that itself.

        With ``facts_table`` (and the stream's ``app_id``), a chunked snapshot of that stream
        still open in ``target`` raises: it is a full snapshot (ADR 0028).
        """
        from ..client import make_client

        if facts_table:
            if not app_id:
                raise ValueError("snapshot() with a facts_table needs the stream's app_id")
            self._lock(facts_table, target, app_id, "full")
        ci = self._capture_instance()
        if not resnapshot:
            done = self._last_snapshot(target, ci)
            if done:
                return done
            done = self._last_snapshot(target, ci, chunks=True)
            if done:  # bronze holds the commit times of the chunks' stamps, not of S
                with closing(make_client(self.options)) as client:
                    return {"lsn": done["lsn"], "commit_ts": client.lsn_to_time(done["lsn"]) or ""}
        return self._take_snapshot(target, ci)[0]

    def seed(
        self,
        target: str,
        df: DataFrame,
        as_of: str | datetime,
        *,
        app_id: str | None = None,
        facts_table: str | None = None,
        allow_missing_columns: bool = False,
        reseed: bool = False,
    ) -> Offset:
        """Append ``df``, a copy of the tracked table you already have, to ``target`` as its
        snapshot, and return the offset it is stamped with, an ``Offset`` (ADR 0025). For a
        table too big to snapshot within the CDC retention; ``to_delta(bootstrap=True)`` then
        starts from it.

        ``as_of``: when the copy started being read, as a UTC ``datetime`` (an aware one is
        converted), or an LSN (``"0x..."``) recorded before that. Every commit at or before it
        must be in the copy; later ones may be, the stream replays them. A datetime maps to
        the last commit at or before it, to the second, on the server's clock
        (``sourceTimeZone``). An LSN CDC cleanup has passed raises ``DataLossError``.

        ``df``'s columns match the captured ones by name, ignoring case; others are dropped.
        A captured column ``df`` lacks raises ``ValueError``, or reads NULL with
        ``allow_missing_columns=True``. With ``facts_table`` (and the stream's ``app_id``)
        a ``'bootstrap'`` event row records the seed, the one ``to_delta`` would write.

        A rerun with the same ``as_of`` returns the seed already written, also under a newer
        snapshot; once cleanup has passed a time ``as_of`` (and its ``cdc.lsn_time_mapping``
        row), the newest snapshot of the table at or before it. Any other snapshot of the
        table in ``target`` raises, unless ``reseed=True`` and ``as_of`` is newer: after a
        ``DataLossError``, then start a new checkpoint and ``app_id`` from it. With
        ``facts_table``, a snapshot of that stream still open in ``target``, in either mode,
        raises too.
        """
        from pyspark.sql import functions as F

        from ..client import DataLossError, make_client
        from ..lsn import ZERO_LSN, normalize
        from ..sink import _utc_now
        from ..source import METADATA_COLUMNS

        if facts_table:
            if not app_id:
                raise ValueError("seed() with a facts_table needs the stream's app_id")
            self._lock(facts_table, target, app_id, None)
        ci = self._capture_instance()
        started_at, t0 = _utc_now(), time.monotonic()
        schema = self.spark.read.format("mssql_cdc_snapshot").options(**self.options).load().schema
        meta = {n for n, _ in METADATA_COLUMNS}
        given = {c.lower(): c for c in df.columns}
        missing = [f.name for f in schema if f.name not in meta and f.name.lower() not in given]
        if missing and not allow_missing_columns:
            raise ValueError(
                f"The copy lacks captured columns {missing} of {ci}: pass "
                "allow_missing_columns=True to seed them as NULL"
            )
        with closing(make_client(self.options)) as client:
            # a computed column listed in 'columns' reads NULL in every row, a seed's too
            computed = {c.lower() for i in client.capture_instances(ci) for c in i.computed}
            done: Offset | None = None
            if isinstance(as_of, datetime):
                if as_of.tzinfo:
                    as_of = as_of.astimezone(timezone.utc).replace(tzinfo=None)
                lsn = client.time_to_lsn(as_of)
                if lsn is None and not reseed:
                    # cleanup deletes cdc.lsn_time_mapping rows too: once it has passed
                    # as_of, a rerun's seed is the newest snapshot at or before it
                    at = F.lit(as_of.isoformat()).cast("timestamp_ntz")
                    done = self._last_snapshot(target, ci, F.col("_commit_ts") <= at)
                if lsn is None and not done:
                    raise DataLossError(
                        f"{ci}: cdc.lsn_time_mapping holds no commit at or before {as_of} UTC: "
                        "the copy is older than what CDC holds"
                    )
            else:
                lsn = normalize(as_of)
            if lsn:  # a rerun's seed, even under a newer snapshot (a switch's, a reseed)
                done = self._last_snapshot(target, ci, F.col("_start_lsn") == lsn)
            timing: dict[str, Any] | None = None
            if done:  # a rerun: written already
                offset, timing = done, {}
            else:
                assert lsn is not None  # no seed found without it raised above
                last = self._last_snapshot(target, ci, chunks=True)
                if last and not (reseed and lsn > last["lsn"]):  # never a second seed silently
                    raise ValueError(
                        f"{target} already holds a snapshot of {ci} at {last['lsn']}: seed an "
                        f"empty target, or with reseed=True a copy newer than it (not {lsn})"
                    )
                if lsn > (client.max_lsn() or ZERO_LSN):
                    raise ValueError(f"{lsn} is after sys.fn_cdc_get_max_lsn(): not recorded yet")
                low = _lost(client, ci, lsn)
                if low:
                    raise DataLossError(
                        f"{ci}: CDC no longer holds the changes after {lsn} (min_lsn is {low}): "
                        "the copy is older than the CDC retention. Seed from a newer copy"
                    )
                offset = {"lsn": lsn, "commit_ts": client.lsn_to_time(lsn) or ""}
        if timing is None:
            values = {"_capture_instance": ci, "_start_lsn": lsn, "_operation": 0}
            values["_commit_ts"] = offset["commit_ts"] or None

            def column(name: str) -> Column:
                if name in meta:
                    return F.lit(values.get(name))
                if name.lower() in given and name.lower() not in computed:
                    return F.col("`" + given[name.lower()].replace("`", "``") + "`")
                return F.lit(None)

            rows = df.select(*(column(f.name).cast(f.dataType).alias(f.name) for f in schema))
            timing = self._write_snapshot(target, rows, {"seed": ci, **offset}, started_at, t0)
        if facts_table:
            assert app_id is not None  # checked above
            self._bootstrap_event(facts_table, app_id, target, offset, timing)
        return offset

    def _take_snapshot(self, target: str, ci: str) -> tuple[Offset, dict[str, Any]]:
        """Write a new snapshot into ``target``. Returns its offset and the ``rows``,
        ``started_at`` and ``duration_ms`` of its event row in the facts table."""
        from ..client import make_client
        from ..sink import _utc_now
        from ..source import snapshot_lsn

        started_at, t0 = _utc_now(), time.monotonic()
        with closing(make_client(self.options)) as client:
            lsn = snapshot_lsn(client, client.source_table(ci))
            offset: Offset = {"lsn": lsn, "commit_ts": client.lsn_to_time(lsn) or ""}
        _log.info("mssql_cdc: snapshot of %s into %s at %s: reading the table", ci, target, lsn)
        rows = (
            self.spark.read.format("mssql_cdc_snapshot")
            .options(**self.options)
            .option("snapshotLsn", lsn)
            .load()
        )
        timing = self._write_snapshot(target, rows, {"snapshot": ci, **offset}, started_at, t0)
        _log.info(
            "mssql_cdc: snapshot of %s into %s at %s: %s rows in %.1f s",
            ci,
            target,
            lsn,
            timing["rows"],
            timing["duration_ms"] / 1000,
        )
        return offset, timing

    def _write_snapshot(
        self,
        target: str,
        rows: DataFrame,
        meta: Mapping[str, Any],
        started_at: datetime,
        t0: float,
    ) -> dict[str, Any]:
        """Append snapshot ``rows`` to ``target`` in one commit with userMetadata ``meta``.
        Returns the ``rows``, ``started_at`` and ``duration_ms`` of its facts event row."""
        from pyspark.sql import functions as F

        from .. import migrations
        from ..sink import BRONZE_COMMENT, _write, bronze_columns, bronze_rows
        from ..tables import commit_after

        rows = bronze_rows(rows, snapshot=F.col("_start_lsn"))  # whole: its stamp is its LSN
        migrations.ensure(self.spark, target, "bronze", bronze_columns(rows), BRONZE_COMMENT)
        tag = json.dumps(meta)
        before = _version(self.spark, target)
        _write(rows, target, None, None, tag, merge_schema=True)  # as the stream's (ADR 0023)
        duration_ms = round((time.monotonic() - t0) * 1000)
        # this snapshot's own commit among those after it (auto compaction may commit after
        # it, as may other writers); an empty table writes no commit at all
        commit = commit_after(self.spark, target, before, tag)
        written = commit and (commit["operationMetrics"] or {}).get("numOutputRows")
        return {
            "rows": int(written or 0),
            "started_at": started_at,
            "duration_ms": duration_ms,
        }

    def _last_snapshot(
        self, target: str, ci: str, where: Column | None = None, chunks: bool = False
    ) -> Offset | None:
        """The newest whole snapshot (or seed) of the table in ``target``, of those ``where``
        (a Column) keeps. A chunked snapshot's rows are never one: its chunks are stamped
        with their own LSNs, and only its facts tell it complete (``_opened``, ADR 0028).
        ``chunks``: a chunked snapshot's S counts too, open or complete; ``commit_ts`` is then
        a chunk stamp's, not S's."""
        from pyspark.sql import functions as F

        from ..tables import delta_table, exists

        if not exists(self.spark, target):
            return None
        # ignoring case, as SQL Server resolves the name: a rerun may spell it differently
        cond = (F.col("_operation") == 0) & F.lower("_capture_instance").isin(
            _instances(self.options, ci)
        )
        df, lsn = delta_table(self.spark, target).toDF(), F.col("_start_lsn")
        if "_chunk" in df.columns:  # bronze migration 2; before it every snapshot was whole
            lsn = F.coalesce("_snapshot", "_start_lsn")
            cond = cond if chunks else cond & F.col("_chunk").isNull()
        row = (
            df.where(cond if where is None else cond & where)
            .agg(F.max(lsn).alias("lsn"), F.max("_commit_ts").alias("ts"))
            .first()
        )
        if row is None or row["lsn"] is None:
            return None
        return {"lsn": row["lsn"], "commit_ts": _iso(row["ts"])}

    def _bootstrap(
        self, target: str, app_id: str, facts_table: str | None, chunked: bool = False
    ) -> str:
        """``snapshot(target)``'s LSN, with its 'bootstrap' event. Without a whole snapshot in
        ``target``, the LSN of the chunked one opened for ``app_id``, open or complete, in
        either ``snapshot`` mode: the table is never read twice, and that snapshot's
        'bootstrap' event is ``backfill()``'s. ``chunked``: opened now if there is none. With
        a facts table a full one is opened too, before its read: until its 'bootstrap' row, or
        until CDC cleanup passes it, a chunked run raises (``_lock``)."""
        ci = self._capture_instance()
        offset = self._last_snapshot(target, ci)
        if offset is None and facts_table:
            opened = self._reusable(facts_table, target, app_id, chunked)
            if opened or chunked:
                opened = opened or self._open(
                    target,
                    ci,
                    app_id=app_id,
                    sink_id=app_id,
                    facts_table=facts_table,
                    generation=0,
                    kind="bootstrap",
                )
                return opened["lsn"]
        timing: dict[str, Any] = {}
        if offset is None:
            if facts_table:
                self._open(
                    target,
                    ci,
                    app_id=app_id,
                    sink_id=app_id,
                    facts_table=facts_table,
                    generation=0,
                    kind="bootstrap",
                    mode="full",
                )
            offset, timing = self._take_snapshot(target, ci)
        if facts_table:
            self._bootstrap_event(facts_table, app_id, target, offset, timing)
        return offset["lsn"]

    def _bootstrap_event(
        self, facts_table: str, app_id: str, target: str, offset: Offset, timing: dict[str, Any]
    ) -> None:
        """The 'bootstrap' event of a snapshot or a seed, written once: Delta skips a rerun's,
        so a crash between the snapshot and the event only delays it."""
        events.write_event(
            self.spark,
            facts_table,
            events.BOOTSTRAP,
            app_id=app_id,
            txn_app_id=f"{app_id}#events",
            version=0,
            target=target,
            lsn=offset["lsn"],
            commit_ts=offset["commit_ts"],
            **timing,
        )
