"""One declaration for the common pipeline: SQL Server CDC -> Delta, with facts and metrics.

    from mssql_cdc import stream
    query = stream(spark, options).to_delta("bronze.orders", "orders-v1",
                                            checkpoint="/Volumes/cat/sch/vol/ckpt/orders",
                                            facts_table="ops.ingestion_facts",
                                            bootstrap=True, on_data_loss="resnapshot")

The options go to ``readStream`` once, and the sink gets what it needs from them. With a
facts table, per-partition metrics default to ``<checkpoint>/_mssql_cdc_metrics`` (of the
live generation, see below) when the
checkpoint is a path Python can write on every node (local, or FUSE such as a Volume);
with a URI checkpoint (``dbfs:/``, ``abfss://``...) set ``metricsPath`` yourself: the files
then go to ``<metricsPath>/<sink app_id>``, so streams may share one ``metricsPath``.

``bootstrap=True`` first writes a snapshot of the tracked table into the target (once) and
starts a new checkpoint from its LSN, so the target holds the whole table, not only what CDC
retention still has (ADR 0016). For a table too big to snapshot within the CDC retention,
``seed(target, df, as_of)`` writes a copy you already have as that snapshot (ADR 0025).

``on_data_loss="resnapshot"`` checks, before the query starts, whether CDC cleanup already
deleted changes the checkpoint has not read. If so it snapshots the table into the target
again and moves the stream to a new generation ``n``: Spark checkpoint
``<checkpoint>/_generations/<n>`` and sink app_id ``<app_id>.g<n>`` (metrics then default
under that checkpoint), recorded in ``<checkpoint>/_mssql_cdc_generation.json`` (so it needs
a checkpoint path Python and Spark resolve alike: local, or a Volume) and as an event row in
the facts table, which it requires. At most once per ``resnapshot_interval_days``. A purge
while the query runs still fails it with ``DataLossError``; the next run recovers. Run one
job per stream (ADR 0018).

``snapshot="chunked"`` (ADR 0028) takes either snapshot next to the stream instead of before
it, for a table the link cannot read within the CDC retention: ``to_delta`` only opens it at
an LSN S (a 'snapshot_open' facts row with its plan) and starts the stream generation at S
at once; ``backfill()``, called repeatedly in its own task, reads the key space in chunks,
each stamped with an LSN at or after S, appends them to the target (``_snapshot`` S,
``_chunk``) and records them as 'snapshot_chunk' rows, then writes the snapshot's
'bootstrap' or 'resnapshot' row. Downstream rebuilds from S: its rows and the changes after
S. A loss while one is open opens a newer one, which abandons it.

The stream follows a newer capture instance of its table (ADR 0023): snapshots in the target
are found under any capture instance of the table, and ``snapshot_on_switch=True`` appends a
snapshot after the batch that first reads the newer one, so that rows unchanged since then
carry the columns only it captures instead of NULL.
"""

from __future__ import annotations

import json
import os
import re
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from uuid import uuid4

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]+:")  # a scheme; a Windows drive has one letter
_STATE = "_mssql_cdc_generation.json"
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
)


def _opt(options: dict, key: str):
    return next((v for k, v in options.items() if k.lower() == key.lower()), None)


def _ts(iso: str | None) -> datetime | None:
    """A commit time (UTC ISO-8601, as in offsets) for a TIMESTAMP_NTZ facts column."""
    return datetime.fromisoformat(iso) if iso else None


def _iso(ts: datetime | None) -> str:
    """A TIMESTAMP_NTZ facts value as an offset's commit_ts; '' when NULL."""
    return ts.isoformat(timespec="milliseconds") if ts else ""


# -- generations (ADR 0018) ------------------------------------------------------
def _generation(checkpoint: str, app_id: str, n: int) -> tuple[str, str]:
    """Spark checkpoint and sink app_id of generation ``n``; 0 is the caller's own."""
    if not n:
        return checkpoint, app_id
    return os.path.join(checkpoint, "_generations", str(n)), f"{app_id}.g{n}"


def _read_state(checkpoint: str) -> dict | None:
    """The generation state; None is generation 0 (always, for a URI checkpoint)."""
    path = os.path.join(checkpoint, _STATE)
    if _URI.match(checkpoint) or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _write_state(checkpoint: str, state: dict) -> None:
    path = os.path.join(checkpoint, _STATE)
    os.makedirs(checkpoint, exist_ok=True)
    with open(path + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(state, fh)
    os.replace(path + ".tmp", path)  # a crash leaves the old state or the new one


def _instances(options: dict, ci: str) -> list[str]:
    """``ci`` and the other capture instances of its source table, lower-cased (SQL Server
    resolves names ignoring case): after a switch the target holds rows of each (ADR 0023)."""
    from .client import make_client

    with closing(make_client(options)) as client:
        return sorted({ci.lower(), *(i.name.lower() for i in client.capture_instances(ci))})


def _lost(client, ci: str, lsn: str) -> str | None:
    """When CDC no longer holds the changes right after ``lsn``, the ``min_lsn`` of the
    capture instance that should: the one the source reads them from (ADR 0023), the newest
    instance of the table starting at or before them, else the oldest. Once an older instance
    is dropped, what only it held is gone too. None when they are all there."""
    nxt = client.increment_lsn(lsn)
    instances = client.capture_instances(ci)
    name = instances[0].name if instances else ci
    for i in instances[1:]:
        if i.start_lsn and i.start_lsn <= nxt:
            name = i.name
    low = client.min_lsn(name)
    return low if nxt < low else None


def _bounds(chunk: dict) -> dict:
    return {"chunk": chunk["chunk"], "lo": chunk["lo"], "hi": chunk["hi"]}


def _version(spark, target: str) -> int:
    from .tables import delta_table

    return int(delta_table(spark, target).history(1).first()["version"])


def _last_offset(checkpoint: str) -> dict | None:
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
    return json.loads(lines[2])


class CdcStream:
    def __init__(self, spark, options: dict):
        from . import register  # lazy: the package imports this module

        self.spark, self.options = spark, dict(options)
        register(spark)

    def _capture_instance(self) -> str:
        ci = _opt(self.options, "captureInstance")
        if not ci:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        return ci

    def snapshot(self, target: str, resnapshot: bool = False) -> dict:
        """Append the tracked table's current rows to ``target`` as operation 0 and return the
        offset they are stamped with, for the stream's ``startingLsn``.

        The LSN is recorded before the table is read. Once ``target`` holds a snapshot taken
        under any capture instance of this table, its offset is returned and nothing is read,
        so a rerun cannot skip the changes after it. ``resnapshot=True`` takes a new one:
        after a DataLossError, with a new checkpoint and app_id; downstream, rebuild from the
        newest snapshot.
        ``to_delta(on_data_loss="resnapshot")`` does all of that itself.
        """
        ci = self._capture_instance()
        if not resnapshot:
            done = self._last_snapshot(target, ci)
            if done:
                return done
        return self._take_snapshot(target, ci)[0]

    def seed(
        self,
        target: str,
        df,
        as_of: str | datetime,
        *,
        app_id: str | None = None,
        facts_table: str | None = None,
        allow_missing_columns: bool = False,
        reseed: bool = False,
    ) -> dict:
        """Append ``df``, a copy of the tracked table you already have, to ``target`` as its
        snapshot, and return the offset it is stamped with (ADR 0025). For a table too big to
        snapshot within the CDC retention; ``to_delta(bootstrap=True)`` then starts from it.

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
        ``DataLossError``, then start a new checkpoint and ``app_id`` from it.
        """
        from pyspark.sql import functions as F

        from .client import DataLossError, make_client
        from .lsn import ZERO_LSN, normalize
        from .sink import _utc_now
        from .source import METADATA_COLUMNS

        if facts_table and not app_id:
            raise ValueError("seed() with a facts_table needs the stream's app_id")
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
            done = None
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
            timing: dict | None = None
            if done:  # a rerun: written already
                offset, timing = done, {}
            else:
                assert lsn is not None  # no seed found without it raised above
                last = self._last_snapshot(target, ci)
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

            def column(name: str):
                if name in meta:
                    return F.lit(values.get(name))
                if name.lower() in given:
                    return F.col("`" + given[name.lower()].replace("`", "``") + "`")
                return F.lit(None)

            rows = df.select(*(column(f.name).cast(f.dataType).alias(f.name) for f in schema))
            timing = self._write_snapshot(target, rows, {"seed": ci, **offset}, started_at, t0)
        if facts_table:
            assert app_id is not None  # checked above
            self._bootstrap_event(facts_table, app_id, target, offset, timing)
        return offset

    def _take_snapshot(self, target: str, ci: str) -> tuple[dict, dict]:
        """Write a new snapshot into ``target``. Returns its offset and the ``rows``,
        ``started_at`` and ``duration_ms`` of its event row in the facts table."""
        from .client import make_client
        from .sink import _utc_now
        from .source import snapshot_lsn

        started_at, t0 = _utc_now(), time.monotonic()
        with closing(make_client(self.options)) as client:
            lsn = snapshot_lsn(client, client.source_table(ci))
            offset = {"lsn": lsn, "commit_ts": client.lsn_to_time(lsn) or ""}
        rows = (
            self.spark.read.format("mssql_cdc_snapshot")
            .options(**self.options)
            .option("snapshotLsn", lsn)
            .load()
        )
        return offset, self._write_snapshot(
            target, rows, {"snapshot": ci, **offset}, started_at, t0
        )

    def _write_snapshot(self, target: str, rows, meta: dict, started_at, t0: float) -> dict:
        """Append snapshot ``rows`` to ``target`` in one commit with userMetadata ``meta``.
        Returns the ``rows``, ``started_at`` and ``duration_ms`` of its facts event row."""
        from pyspark.sql import functions as F

        from . import migrations
        from .sink import BRONZE_COMMENT, _write, bronze_columns, bronze_rows
        from .tables import delta_table

        rows = bronze_rows(rows, snapshot=F.col("_start_lsn"))  # whole: its stamp is its LSN
        migrations.ensure(self.spark, target, "bronze", bronze_columns(rows), BRONZE_COMMENT)
        tag = json.dumps(meta)
        _write(rows, target, None, None, tag, merge_schema=True)  # as the stream's (ADR 0023)
        duration_ms = round((time.monotonic() - t0) * 1000)
        # this snapshot's own commit (auto compaction may commit after it); an empty table
        # writes no commit at all
        commit = (
            delta_table(self.spark, target).history(5).where(F.col("userMetadata") == tag).first()
        )
        written = commit and (commit["operationMetrics"] or {}).get("numOutputRows")
        return {
            "rows": int(written or 0),
            "started_at": started_at,
            "duration_ms": duration_ms,
        }

    def _last_snapshot(self, target: str, ci: str, where=None) -> dict | None:
        """The newest whole snapshot (or seed) of the table in ``target``, of those ``where``
        (a Column) keeps. A chunked snapshot's rows are never one: its chunks are stamped
        with their own LSNs, and only its facts tell it complete (``_opened``, ADR 0028)."""
        from pyspark.sql import functions as F

        from .tables import delta_table, exists

        if not exists(self.spark, target):
            return None
        # ignoring case, as SQL Server resolves the name: a rerun may spell it differently
        cond = (F.col("_operation") == 0) & F.lower("_capture_instance").isin(
            _instances(self.options, ci)
        )
        df, lsn = delta_table(self.spark, target).toDF(), F.col("_start_lsn")
        if "_chunk" in df.columns:  # bronze migration 2; before it every snapshot was whole
            cond, lsn = cond & F.col("_chunk").isNull(), F.coalesce("_snapshot", "_start_lsn")
        row = (
            df.where(cond if where is None else cond & where)
            .agg(F.max(lsn).alias("lsn"), F.max("_commit_ts").alias("ts"))
            .first()
        )
        if row is None or row["lsn"] is None:
            return None
        return {"lsn": row["lsn"], "commit_ts": _iso(row["ts"])}

    def _opened(self, facts_table: str, target: str, sink_id: str) -> dict | None:
        """The offset of the chunked snapshot opened for the generation whose sink app_id is
        ``sink_id`` (its 'snapshot_open' row), open or complete; None when there is none."""
        from pyspark.sql import functions as F

        from .tables import delta_table, exists

        if not exists(self.spark, facts_table):
            return None
        row = (
            delta_table(self.spark, facts_table)
            .toDF()
            .where(
                (F.col("event") == "snapshot_open")
                & (F.col("target") == target)
                & (F.col("app_id") == sink_id)
            )
            .select("min_lsn", "min_commit_ts")
            .first()
        )
        return {"lsn": row["min_lsn"], "commit_ts": _iso(row["min_commit_ts"])} if row else None

    def _open(
        self,
        target: str,
        ci: str,
        app_id: str,
        sink_id: str,
        facts_table: str,
        generation: int,
        mode: str,
        lost_from_ts: datetime | None = None,
        lost_to_ts: datetime | None = None,
    ) -> dict:
        """Open a chunked snapshot for generation ``generation`` (sink ``sink_id``): record
        its LSN S, then what its chunks tile, in a 'snapshot_open' facts row, and return its
        offset, where the generation's stream starts. Nothing is read from the table here;
        ``backfill()`` reads the chunks (ADR 0028)."""
        from .client import make_client, snapshot_plan
        from .sink import write_event
        from .source import snapshot_lsn

        with closing(make_client(self.options)) as client:
            source = client.source_table(ci)
            lsn = snapshot_lsn(client, source)  # first: the plan's MIN and MAX come after S
            plan = snapshot_plan(client, ci, source)
            offset = {"lsn": lsn, "commit_ts": client.lsn_to_time(lsn) or ""}
        detail = {
            "mode": mode,
            "keys": source.keys,
            "plan": plan,
            "generation": generation,
            "lost_from_ts": _iso(lost_from_ts) or None,
            "lost_to_ts": _iso(lost_to_ts) or None,
        }
        write_event(
            self.spark,
            facts_table,
            "snapshot_open",
            app_id=sink_id,
            txn_app_id=f"{app_id}#snapshots",
            version=generation,
            target=target,
            lsn=lsn,
            commit_ts=offset["commit_ts"],
            rows=0,
            lost_from_ts=lost_from_ts,
            lost_to_ts=lost_to_ts,
            detail=json.dumps(detail),
        )
        return offset

    def _bootstrap(
        self, target: str, app_id: str, facts_table: str | None, chunked: bool = False
    ) -> str:
        """``snapshot(target)``'s LSN, with its 'bootstrap' event. ``chunked``: without a
        whole snapshot in ``target``, the LSN of the chunked one opened for ``app_id``,
        opened now if there is none; its 'bootstrap' event waits for its last chunk."""
        ci = self._capture_instance()
        offset = self._last_snapshot(target, ci)
        if offset is None and chunked:
            assert facts_table is not None  # to_delta checks it
            offset = self._opened(facts_table, target, app_id) or self._open(
                target, ci, app_id, app_id, facts_table, 0, "bootstrap"
            )
            return offset["lsn"]
        timing: dict = {}
        if offset is None:
            offset, timing = self._take_snapshot(target, ci)
        if facts_table:
            self._bootstrap_event(facts_table, app_id, target, offset, timing)
        return offset["lsn"]

    def _bootstrap_event(
        self, facts_table: str, app_id: str, target: str, offset: dict, timing: dict
    ) -> None:
        """The 'bootstrap' event of a snapshot or a seed, written once: Delta skips a rerun's,
        so a crash between the snapshot and the event only delays it."""
        from .sink import write_event

        write_event(
            self.spark,
            facts_table,
            "bootstrap",
            app_id=app_id,
            txn_app_id=f"{app_id}#events",
            version=0,
            target=target,
            lsn=offset["lsn"],
            commit_ts=offset["commit_ts"],
            **timing,
        )

    def _recover(
        self,
        target: str,
        app_id: str,
        checkpoint: str,
        facts_table: str,
        state: dict | None,
        interval_days: float,
        bootstrap: bool,
        chunked: bool = False,
    ) -> dict | None:
        """Before the query starts: when CDC cleanup deleted changes the current generation
        has not read, re-snapshot and return the next generation's state; None otherwise.
        ``chunked``: open a chunked snapshot instead (ADR 0028); the next generation starts at
        its LSN at once, and ``backfill()`` reads it and writes its 'resnapshot' row."""
        from .client import DataLossError, make_client
        from .lsn import ZERO_LSN, normalize
        from .sink import write_event

        ci = self._capture_instance()
        state = state or {"generation": 0}
        n = state["generation"]
        # a recovery that stopped before its new state was written resumes from what it found
        start = state.get("recovering") or _last_offset(_generation(checkpoint, app_id, n)[0])
        if start is None:  # nothing committed yet: where the generation's stream starts
            given = (_opt(self.options, "startingLsn") or "").strip()
            if n:
                start = {"lsn": state["snapshot_lsn"], "commit_ts": state["commit_ts"]}
            elif bootstrap:  # as _bootstrap finds it
                start = self._last_snapshot(target, ci)
                if start is None and chunked:
                    start = self._opened(facts_table, target, app_id)
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
            if chunked:  # opened by a recovery that stopped before writing its state
                done = self._opened(facts_table, target, next_id)
            else:
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
        _write_state(checkpoint, {**state, "recovering": start})
        if chunked:  # nothing read here, so nothing to purge meanwhile; a newer open supersedes
            offset = done or self._open(
                target,
                ci,
                app_id,
                next_id,
                facts_table,
                n + 1,
                "resnapshot",
                lost_from_ts=_ts(start["commit_ts"]),
                lost_to_ts=lost_to,
            )
        else:
            offset, timing = (done, {}) if done else self._take_snapshot(target, ci)
            with closing(make_client(self.options)) as client:
                if _lost(client, ci, offset["lsn"]):
                    failed = {"failed_at": now.isoformat(timespec="seconds")}
                    _write_state(checkpoint, {**state, "recovering": start, **failed})
                    raise DataLossError(
                        f"{lost}. The re-snapshot at {offset['lsn']} took longer than the CDC "
                        "retention, so the changes after it are gone too: lengthen the retention "
                        "or speed up the read (numPartitions), then rerun with "
                        "resnapshot_interval_days=0."
                    )
            write_event(
                self.spark,
                facts_table,
                "resnapshot",
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
        return state

    def to_delta(
        self,
        target: str,
        app_id: str,
        checkpoint: str,
        facts_table: str | None = None,
        trigger: dict | None = None,
        query_name: str | None = None,
        bootstrap: bool = False,
        on_data_loss: str = "fail",
        resnapshot_interval_days: float = 7.0,
        snapshot_on_switch: bool = False,
        snapshot: str = "full",
    ):
        """Start the stream into ``target`` through ``delta_sink``; returns the StreamingQuery.

        ``trigger``: keyword arguments for ``DataStreamWriter.trigger``, e.g.
        ``{"availableNow": True}``. ``bootstrap``: snapshot the table first (see
        ``snapshot``); a checkpoint that already has offsets ignores the starting LSN.
        ``snapshot``: how ``bootstrap`` and ``on_data_loss="resnapshot"`` take one.
        ``"full"`` reads the table before the stream starts; ``"chunked"`` (needs
        ``facts_table``) only opens one at an LSN S and starts the stream there at once, and
        ``backfill()``, run apart, reads it in chunks next to the stream (ADR 0028).
        ``on_data_loss``: ``"fail"`` (the query stops with ``DataLossError``) or
        ``"resnapshot"`` (recover before starting, in a new generation; needs ``facts_table``
        and a checkpoint that is a local or FUSE path, not a URI or ``/dbfs/``).
        ``resnapshot_interval_days`` must exceed the CDC retention: a second loss within it
        raises ``DataLossError`` instead of snapshotting again.
        ``snapshot_on_switch``: after the batch that first reads a newer capture instance of the
        table, append a snapshot, so that rows unchanged since then carry the columns only the
        newer instance captures, instead of NULL (ADR 0023). It reads the reader's events from
        the metrics directory, so a URI checkpoint needs ``metricsPath``.
        """
        from .sink import delta_sink

        if on_data_loss not in ("fail", "resnapshot"):
            raise ValueError(f"on_data_loss must be 'fail' or 'resnapshot', not {on_data_loss!r}")
        if snapshot not in ("full", "chunked"):
            raise ValueError(f"snapshot must be 'full' or 'chunked', not {snapshot!r}")
        chunked = snapshot == "chunked"
        if chunked and not facts_table:
            raise ValueError(
                "snapshot='chunked' needs a facts_table: its event rows hold the snapshot's plan "
                "and which chunks are in"
            )
        if snapshot_on_switch and not _opt(self.options, "metricsPath") and _URI.match(checkpoint):
            raise ValueError(
                "snapshot_on_switch=True learns of a switch from the reader's events in "
                "metricsPath, which a URI checkpoint has no default for: set it"
            )
        if on_data_loss == "resnapshot":
            # Spark resolves /dbfs/x as dbfs:/dbfs/x, Python as dbfs:/x: two directories
            if _URI.match(checkpoint) or f"{checkpoint}/".startswith("/dbfs/"):
                raise ValueError(
                    "on_data_loss='resnapshot' reads Spark's checkpoint from Python: use a path "
                    "both resolve to the same directory (local, or /Volumes/...), not a URI or "
                    "/dbfs/..."
                )
            if not facts_table:
                raise ValueError(
                    "on_data_loss='resnapshot' needs a facts_table: its event rows "
                    "tell downstream to rebuild, and from which LSN"
                )
        if bootstrap and _opt(self.options, "startingLsn"):
            raise ValueError("bootstrap=True sets startingLsn itself; pass one or the other")
        state = _read_state(checkpoint)
        if on_data_loss == "resnapshot":
            assert facts_table is not None  # checked above
            state = (
                self._recover(
                    target,
                    app_id,
                    checkpoint,
                    facts_table,
                    state,
                    resnapshot_interval_days,
                    bootstrap,
                    chunked,
                )
                or state
            )
        n = state["generation"] if state else 0  # a gen-0 state only records a recovery
        checkpoint, sink_id = _generation(checkpoint, app_id, n)
        options = dict(self.options)
        if n:  # the generation starts at its snapshot, whatever startingLsn said
            assert state is not None  # n comes from it
            options = {k: v for k, v in options.items() if k.lower() != "startinglsn"}
            options["startingLsn"] = state["snapshot_lsn"]
        elif bootstrap:
            options["startingLsn"] = self._bootstrap(target, app_id, facts_table, chunked)
        metrics = _opt(options, "metricsPath")
        if metrics:  # one directory per stream: the sink folds and removes every file in it
            metrics = os.path.join(metrics, sink_id)
        elif (facts_table or snapshot_on_switch) and metrics is None and not _URI.match(checkpoint):
            metrics = os.path.join(checkpoint, "_mssql_cdc_metrics")
        if metrics:
            options = {k: v for k, v in options.items() if k.lower() != "metricspath"}
            options["metricsPath"] = metrics
        write = delta_sink(target, sink_id, facts_table, metrics_path=metrics)  # removes its files
        if snapshot_on_switch:
            assert metrics  # checked above
            write = _snapshot_after_switch(
                write, dict(self.options), self._capture_instance(), target, metrics
            )
        writer = (
            self.spark.readStream.format("mssql_cdc")
            .options(**options)
            .load()
            .writeStream.foreachBatch(write)
            .option("checkpointLocation", checkpoint)
        )
        if trigger:
            writer = writer.trigger(**trigger)
        if query_name:
            writer = writer.queryName(query_name)
        return writer.start()

    # -- chunked snapshots (ADR 0028) -----------------------------------------------
    def backfill(
        self,
        target: str,
        *,
        app_id: str,
        facts_table: str,
        chunk_rows: int = 1_000_000,
        max_waves: int | None = None,
        max_seconds: float | None = None,
        min_headroom_hours: float | None = None,
        isolation: str | None = None,
    ) -> dict:
        """Read the newest open chunked snapshot of ``target`` (``to_delta(...,
        snapshot="chunked")`` opens one) in waves of ``numPartitions`` chunks of about
        ``chunk_rows`` rows, next to the running stream; returns how far it got. Run it
        apart from the stream (its own task) and call it again until ``done``.

        Each wave is stamped with ``max_lsn`` before it is read, at or after the snapshot's
        LSN S, appended to ``target`` in one commit (operation 0, ``_snapshot`` S,
        ``_chunk``) and then recorded as one 'snapshot_chunk' facts row per chunk; after the
        last chunk comes the snapshot's 'bootstrap' or 'resnapshot' row, which tells
        downstream to rebuild from S. After a crash between a wave's append and its facts
        rows, the rerun's append is skipped by Delta and its rows are rebuilt from the one
        committed: nothing is appended twice.

        ``app_id``: the stream's, as given to ``to_delta``. ``max_waves`` and ``max_seconds``
        bound one call. ``min_headroom_hours``: pause before a wave while the stream's
        retention headroom (of its newest facts row, less that row's age) is below it, or
        the stream has written none: the snapshot shares the link with the stream, and a
        stream the retention passes loses the snapshot too. ``isolation``: ``"snapshot"``
        reads under SNAPSHOT isolation, which the database must allow; else READ COMMITTED.

        Returns ``{"snapshot", "chunks_done", "chunks_total", "done", "paused", "reason"}``:
        ``chunks_total`` is an estimate for one integer key column, else None until done.
        """
        from pyspark.sql import functions as F

        from . import sink
        from .client import int_step, make_client, next_chunks
        from .source import snapshot_lsn
        from .spark import available_cores
        from .tables import delta_table, exists

        chunk_rows = int(chunk_rows)
        if chunk_rows < 1:
            raise ValueError(f"chunk_rows must be at least 1, not {chunk_rows}")
        ci, t0 = self._capture_instance(), time.monotonic()
        ours = re.compile(re.escape(app_id) + r"(\.g\d+)?")  # the stream's generations
        kinds = ("snapshot_open", "snapshot_chunk", "bootstrap", "resnapshot")
        rows = []
        if exists(self.spark, facts_table):
            facts = delta_table(self.spark, facts_table).toDF()
            rows = [
                r
                for r in facts.where((F.col("target") == target) & F.col("event").isin(*kinds))
                .select(*_SNAPSHOT_FACTS)
                .collect()
                if ours.fullmatch(r["app_id"] or "")
            ]
        opens = [r for r in rows if r["event"] == "snapshot_open"]
        if not opens:
            return {
                "snapshot": None,
                "chunks_done": 0,
                "chunks_total": None,
                "done": False,
                "paused": True,
                "reason": f"{target} has no chunked snapshot opened by {app_id}'s stream",
            }
        top = max(opens, key=lambda r: r["min_lsn"])  # a newer open supersedes an older one
        s, sink_id, info = top["min_lsn"], top["app_id"], json.loads(top["detail"])
        plan = info["plan"]
        chunks = sorted(
            (
                {**d, "rows": r["rows"], "lsn": r["min_lsn"]}
                for r in rows
                if r["event"] == "snapshot_chunk"
                and (d := json.loads(r["detail"]))["snapshot"] == s
            ),
            key=lambda c: c["chunk"],
        )
        status = {"snapshot": s, "paused": False, "reason": None}
        # its own row, or a newer whole snapshot's (a full re-snapshot) that supersedes it
        if any(r["event"] in ("bootstrap", "resnapshot") and r["max_lsn"] >= s for r in rows):
            return {**status, "chunks_done": len(chunks), "chunks_total": len(chunks), "done": True}
        given = str(_opt(self.options, "numPartitions") or "auto").strip().lower()
        k = int(given) if given != "auto" else available_cores(self.spark) or os.cpu_count() or 1
        base = _opt(self.options, "metricsPath")  # not the stream's own directory: it folds those
        metrics = os.path.join(base, f"{sink_id}.backfill") if base else None
        waves = 0
        with closing(make_client(self.options)) as client:
            source = client.source_table(ci)
            if source.keys != info["keys"]:
                raise ValueError(
                    f"The unique index of {source.schema}.{source.table} is now on {source.keys}, "
                    f"not on {info['keys']} as when the snapshot at {s} opened: its chunks no "
                    "longer tile the table. Take a new snapshot."
                )
            while not (chunks and chunks[-1]["hi"] is None):
                if (max_waves is not None and waves >= max_waves) or (
                    max_seconds is not None and time.monotonic() - t0 >= max_seconds
                ):
                    break
                reason = self._throttle(facts_table, sink_id, min_headroom_hours)
                if reason:
                    status.update(paused=True, reason=reason)
                    break
                last = chunks[-1] if chunks else None
                lsn = snapshot_lsn(client, source)  # the wave's stamp, before its read
                if lsn < s:
                    raise RuntimeError(
                        f"sys.fn_cdc_get_max_lsn() is {lsn}, below the snapshot's {s}: is this "
                        "the database the snapshot was opened on (not a readable secondary)?"
                    )
                planned = next_chunks(
                    client,
                    ci,
                    source,
                    plan,
                    last["chunk"] + 1 if last else 0,
                    last["hi"] if last else None,
                    max(1, k),
                    chunk_rows,
                )
                wave = last["wave"] + 1 if last else 0
                chunks += self._backfill_wave(
                    target,
                    app_id,
                    facts_table,
                    sink_id,
                    s,
                    wave,
                    lsn,
                    planned,
                    client,
                    metrics,
                    isolation,
                )
                waves += 1
        done = bool(chunks) and chunks[-1]["hi"] is None
        if done:  # also after a crash between the last wave's facts and this row
            rows_in = sum(c["rows"] or 0 for c in chunks)
            sink.write_event(
                self.spark,
                facts_table,
                "bootstrap" if info["mode"] == "bootstrap" else "resnapshot",
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
                detail=json.dumps(
                    {
                        "snapshot": s,
                        "chunks": len(chunks),
                        "rows": rows_in,
                        "last_lsn": max(c["lsn"] for c in chunks),
                    }
                ),
            )
        total = len(chunks) if done else None
        if not done and plan["kind"] == "int":
            start = chunks[-1]["hi"] if chunks else plan["lo"]
            left = -(-(plan["hi"] - start + 1) // int_step(plan, chunk_rows))
            total = len(chunks) + max(1, left)
        return {**status, "chunks_done": len(chunks), "chunks_total": total, "done": done}

    def _throttle(
        self, facts_table: str, sink_id: str, min_headroom_hours: float | None
    ) -> str | None:
        """Why ``backfill()`` pauses now, or None: the retention headroom of the stream
        ``sink_id``'s newest facts row, less that row's age, below ``min_headroom_hours``."""
        if min_headroom_hours is None:
            return None
        from pyspark.sql import functions as F

        from . import sink
        from .tables import delta_table

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
                f"the stream {sink_id} has written no facts row with retention_headroom_hours: "
                "is it running, with metrics?"
            )
        age = (sink._utc_now() - last["written_at"]).total_seconds() / 3600
        left = last["retention_headroom_hours"] - age
        if left < min_headroom_hours:
            return (
                f"retention headroom {left:.2f} h (the stream's newest facts row, {age:.2f} h "
                f"old) is below min_headroom_hours={min_headroom_hours}"
            )
        return None

    def _backfill_wave(
        self,
        target: str,
        app_id: str,
        facts_table: str,
        sink_id: str,
        snapshot: str,
        wave: int,
        lsn: str,
        planned: list[list],
        client,
        metrics: str | None,
        isolation: str | None,
    ) -> list[dict]:
        """Read the ``planned`` chunks stamped ``lsn``, append them to ``target`` and record
        them in the facts. Returns them as ``{chunk, wave, lo, hi, rows, lsn}``."""
        from pyspark.sql import functions as F

        from .sink import _files, _remove, _utc_now, bronze_rows, write_facts

        drop = ("snapshotchunks", "snapshotlsn", "metricspath", "isolationlevel")
        reader = (
            self.spark.read.format("mssql_cdc_snapshot")
            .options(**{k: v for k, v in self.options.items() if k.lower() not in drop})
            .option("snapshotChunks", json.dumps(planned))
            .option("snapshotLsn", lsn)
        )
        if metrics:
            _remove(_files(metrics))  # a dead attempt's
            reader = reader.option("metricsPath", metrics)
        if isolation:
            reader = reader.option("isolationLevel", isolation)
        started_at, t0 = _utc_now(), time.monotonic()
        rows = bronze_rows(reader.load(), snapshot=F.lit(snapshot)).persist()
        try:
            # reads the wave, once: the write below takes the cached rows
            counts: dict[int, int] = dict(rows.groupBy("_chunk").count().collect())
            high = client.max_lsn()  # how far capture had got after the read: informational
            read: dict = {}
            for name in _files(metrics) if metrics else []:
                try:
                    with open(name, encoding="utf-8") as fh:
                        m = json.load(fh)
                    read[m["chunk"]] = m
                except (OSError, ValueError, KeyError):
                    continue
            tag: dict = {
                "backfill": f"{app_id}#snap.{snapshot}",
                "wave": wave,
                "lsn": lsn,
                "attempt": uuid4().hex,
                "chunks": [
                    {
                        "chunk": i,
                        "lo": lo,
                        "hi": hi,
                        "rows": counts.get(i, 0),
                        "high_lsn": read.get(i, {}).get("high_lsn") or high,
                        "read_seconds": read[i]["seconds"] if i in read else None,
                        "read_mb": round(read[i]["bytes"] / 1e6, 6) if i in read else None,
                    }
                    for i, lo, hi in planned
                ],
            }
            if counts:  # an empty wave writes no commit
                tag = self._append_wave(target, rows, tag)
        finally:
            rows.unpersist()
        times: dict = {}

        def at(x: str | None) -> datetime | None:
            if x not in times:
                times[x] = _ts(client.lsn_to_time(x)) if x else None
            return times[x]

        duration_ms = round((time.monotonic() - t0) * 1000)
        write_facts(
            self.spark,
            facts_table,
            [
                {
                    "app_id": sink_id,
                    "rows": c["rows"],
                    "min_lsn": tag["lsn"],
                    "max_lsn": c["high_lsn"],
                    "min_commit_ts": at(tag["lsn"]),
                    "max_commit_ts": at(c["high_lsn"]),
                    "deletes": 0,
                    "inserts": 0,
                    "updates": 0,
                    "started_at": started_at,
                    "duration_ms": duration_ms,
                    "read_seconds": c["read_seconds"],
                    "read_mb": c["read_mb"],
                    "event": "snapshot_chunk",
                    "detail": json.dumps({"snapshot": snapshot, "wave": wave, **_bounds(c)}),
                    "target": target,
                }
                for c in tag["chunks"]
            ],
            f"{app_id}#snapchunks.{snapshot}",
            wave,
        )
        if metrics:  # folded into the facts
            _remove(_files(metrics))
        return [
            {"wave": wave, **_bounds(c), "rows": c["rows"], "lsn": tag["lsn"]}
            for c in tag["chunks"]
        ]

    def _append_wave(self, target: str, rows, tag: dict) -> dict:
        """Append a wave's ``rows`` to ``target`` in one commit with ``tag`` as its
        userMetadata, once per (snapshot, wave). Returns the tag of the commit that holds
        them: this one's or, when Delta skipped the append, the one an earlier attempt of the
        wave committed before it stopped short of its facts rows."""
        from pyspark.sql import functions as F

        from . import migrations
        from .sink import BRONZE_COMMENT, _write, bronze_columns
        from .tables import delta_table

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
        table = delta_table(self.spark, target)
        new = _version(self.spark, target) - before
        if new and table.history(new).where(F.col("userMetadata") == text).first():
            return tag
        found = table.history().where(F.col("userMetadata").contains(key)).select("userMetadata")
        for (meta,) in found.collect():
            earlier = json.loads(meta)
            if earlier.get("backfill") == key and earlier.get("wave") == wave:
                return earlier
        raise RuntimeError(
            f"{target} holds wave {wave} of {key} (Delta skipped its append), but its commit is "
            "no longer in the table's history: take a new snapshot"
        )


def _snapshot_after_switch(sink, options: dict, ci: str, target: str, metrics: str):
    """``sink``, then a snapshot of the table after the batch that first read a newer capture
    instance (``to_delta(snapshot_on_switch=True)``). Holds no session: Spark Connect pickles
    ``foreachBatch`` functions."""
    from .sink import _read_events

    def write(df, batch_id):
        switched = any(
            e.get("event") == "capture_instance_switched" for e in _read_events(metrics)[1]
        )
        sink(df, batch_id)  # folds the events into the facts and removes their files
        if switched:  # a replay after a crash here snapshots again: harmless, only slower
            CdcStream(df.sparkSession, options)._take_snapshot(target, ci)

    return write


def stream(spark, options: dict) -> CdcStream:
    """The CDC stream described by ``options`` (the data source options)."""
    return CdcStream(spark, options)
