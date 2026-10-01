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

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]+:")  # a scheme; a Windows drive has one letter
_STATE = "_mssql_cdc_generation.json"


def _opt(options: dict, key: str):
    return next((v for k, v in options.items() if k.lower() == key.lower()), None)


def _ts(iso: str | None) -> datetime | None:
    """A commit time (UTC ISO-8601, as in offsets) for a TIMESTAMP_NTZ facts column."""
    return datetime.fromisoformat(iso) if iso else None


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
        from .sink import BRONZE_COMMENT, _write, bronze_columns
        from .tables import delta_table

        rows = rows.withColumn("_batch_id", F.lit(None).cast("int"))
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
        """The newest snapshot of the table in ``target``, of those ``where`` (a Column) keeps."""
        from pyspark.sql import functions as F

        from .tables import delta_table, exists

        if not exists(self.spark, target):
            return None
        # ignoring case, as SQL Server resolves the name: a rerun may spell it differently
        cond = (F.col("_operation") == 0) & F.lower("_capture_instance").isin(
            _instances(self.options, ci)
        )
        row = (
            delta_table(self.spark, target)
            .toDF()
            .where(cond if where is None else cond & where)
            .agg(F.max("_start_lsn").alias("lsn"), F.max("_commit_ts").alias("ts"))
            .first()
        )
        if row is None or row["lsn"] is None:
            return None
        ts = row["ts"].isoformat(timespec="milliseconds") if row["ts"] else ""
        return {"lsn": row["lsn"], "commit_ts": ts}

    def _bootstrap(self, target: str, app_id: str, facts_table: str | None) -> str:
        """``snapshot(target)``'s LSN, with its 'bootstrap' event."""
        ci = self._capture_instance()
        offset = self._last_snapshot(target, ci)
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
    ) -> dict | None:
        """Before the query starts: when CDC cleanup deleted changes the current generation
        has not read, re-snapshot and return the next generation's state; None otherwise."""
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
            elif bootstrap:
                start = self._last_snapshot(target, ci)
            elif given.lower() not in ("", "earliest", "latest"):
                start = {"lsn": normalize(given), "commit_ts": ""}
        if start is None:
            return None
        with closing(make_client(self.options)) as client:
            # the source's retention guard, which runs only when there is a range to read
            if (client.max_lsn() or ZERO_LSN) <= start["lsn"]:
                return None
            low = _lost(client, ci, start["lsn"])
            if low is None:
                return None
            lost_to = _ts(client.lsn_to_time(low))
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
        offset, timing = (done, {}) if done else self._take_snapshot(target, ci)
        with closing(make_client(self.options)) as client:
            if _lost(client, ci, offset["lsn"]):
                _write_state(
                    checkpoint,
                    {**state, "recovering": start, "failed_at": now.isoformat(timespec="seconds")},
                )
                raise DataLossError(
                    f"{lost}. The re-snapshot at {offset['lsn']} took longer than the CDC "
                    "retention, so the changes after it are gone too: lengthen the retention or "
                    "speed up the read (numPartitions), then rerun with "
                    "resnapshot_interval_days=0."
                )
        n += 1
        write_event(
            self.spark,
            facts_table,
            "resnapshot",
            app_id=_generation(checkpoint, app_id, n)[1],
            txn_app_id=f"{app_id}#events",
            version=n,
            target=target,
            lsn=offset["lsn"],
            commit_ts=offset["commit_ts"],
            **timing,
            lost_from_ts=_ts(start["commit_ts"]),
            lost_to_ts=lost_to,
        )
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
    ):
        """Start the stream into ``target`` through ``delta_sink``; returns the StreamingQuery.

        ``trigger``: keyword arguments for ``DataStreamWriter.trigger``, e.g.
        ``{"availableNow": True}``. ``bootstrap``: snapshot the table first (see
        ``snapshot``); a checkpoint that already has offsets ignores the starting LSN.
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
            options["startingLsn"] = self._bootstrap(target, app_id, facts_table)
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
