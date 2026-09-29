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
with a URI checkpoint (``dbfs:/``, ``abfss://``...) set ``metricsPath`` yourself.

``bootstrap=True`` first writes a snapshot of the tracked table into the target (once) and
starts a new checkpoint from its LSN, so the target holds the whole table, not only what CDC
retention still has (ADR 0016).

``on_data_loss="resnapshot"`` checks, before the query starts, whether CDC cleanup already
deleted changes the checkpoint has not read. If so it snapshots the table into the target
again and moves the stream to a new generation ``n``: Spark checkpoint
``<checkpoint>/_generations/<n>`` and sink app_id ``<app_id>.g<n>`` (metrics then default
under that checkpoint), recorded in ``<checkpoint>/_mssql_cdc_generation.json`` (so it needs
a checkpoint path Python and Spark resolve alike: local, or a Volume) and as an event row in
the facts table, which it requires. At most once per ``resnapshot_interval_days``. A purge
while the query runs still fails it with ``DataLossError``; the next run recovers. Run one
job per stream (ADR 0018).
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
        raise ValueError(f"{checkpoint}: Spark offset log version {lines[0]!r}; the "
                         "on_data_loss='resnapshot' pre-flight reads only 'v1' (ADR 0018)")
    return json.loads(lines[2])


class CdcStream:
    def __init__(self, spark, options: dict):
        self.spark, self.options = spark, dict(options)

    def _capture_instance(self) -> str:
        ci = _opt(self.options, "captureInstance")
        if not ci:
            raise ValueError("Option 'captureInstance' is required (e.g. 'dbo_orders')")
        return ci

    def snapshot(self, target: str, resnapshot: bool = False) -> dict:
        """Append the tracked table's current rows to ``target`` as operation 0 and return the
        offset they are stamped with, for the stream's ``startingLsn``.

        The LSN is recorded before the table is read. Once ``target`` holds a snapshot of this
        capture instance, its offset is returned and nothing is read, so a rerun cannot skip
        the changes after it. ``resnapshot=True`` takes a new one: after a DataLossError, with
        a new checkpoint and app_id; downstream, rebuild from the newest snapshot.
        ``to_delta(on_data_loss="resnapshot")`` does all of that itself.
        """
        ci = self._capture_instance()
        if not resnapshot:
            done = self._last_snapshot(target, ci)
            if done:
                return done
        return self._take_snapshot(target, ci)[0]

    def _take_snapshot(self, target: str, ci: str) -> tuple[dict, dict]:
        """Write a new snapshot into ``target``. Returns its offset and the ``rows``,
        ``started_at`` and ``duration_ms`` of its event row in the facts table."""
        from pyspark.sql import functions as F

        from . import migrations, register
        from .client import make_client
        from .sink import BRONZE_COMMENT, _utc_now, bronze_columns
        from .source import snapshot_lsn
        from .tables import delta_table, is_path

        started_at, t0 = _utc_now(), time.monotonic()
        with closing(make_client(self.options)) as client:
            lsn = snapshot_lsn(client, client.source_table(ci))
            offset = {"lsn": lsn, "commit_ts": client.lsn_to_time(lsn) or ""}
        register(self.spark)
        rows = (self.spark.read.format("mssql_cdc_snapshot").options(**self.options)
                .option("snapshotLsn", lsn).load()
                .withColumn("_batch_id", F.lit(None).cast("int")))
        migrations.ensure(self.spark, target, "bronze", bronze_columns(rows), BRONZE_COMMENT)
        meta = json.dumps({"snapshot": ci, **offset})
        writer = rows.write.format("delta").mode("append").option("userMetadata", meta)
        writer.save(target) if is_path(target) else writer.saveAsTable(target)
        duration_ms = round((time.monotonic() - t0) * 1000)
        # this snapshot's own commit (auto compaction may commit after it); an empty table
        # writes no commit at all
        commit = (delta_table(self.spark, target).history(5)
                  .where(F.col("userMetadata") == meta).first())
        written = commit and (commit["operationMetrics"] or {}).get("numOutputRows")
        return offset, {"rows": int(written or 0), "started_at": started_at,
                        "duration_ms": duration_ms}

    def _last_snapshot(self, target: str, ci: str) -> dict | None:
        from pyspark.sql import functions as F

        from .tables import delta_table, is_path

        if is_path(target):
            from delta.tables import DeltaTable

            exists = DeltaTable.isDeltaTable(self.spark, target)
        else:
            exists = self.spark.catalog.tableExists(target)
        if not exists:
            return None
        row = (delta_table(self.spark, target).toDF()
               .where((F.col("_operation") == 0) & (F.col("_capture_instance") == ci))
               .agg(F.max("_start_lsn").alias("lsn"), F.max("_commit_ts").alias("ts")).first())
        if row is None or row["lsn"] is None:
            return None
        ts = row["ts"].isoformat(timespec="milliseconds") if row["ts"] else ""
        return {"lsn": row["lsn"], "commit_ts": ts}

    def _bootstrap(self, target: str, app_id: str, facts_table: str | None) -> str:
        """``snapshot(target)``'s LSN, with its 'bootstrap' event (written once: Delta skips a
        rerun's, so a crash between the snapshot and the event only delays it)."""
        from .sink import write_event

        ci = self._capture_instance()
        offset, timing = self._last_snapshot(target, ci), {}
        if offset is None:
            offset, timing = self._take_snapshot(target, ci)
        if facts_table:
            write_event(self.spark, facts_table, "bootstrap", app_id=app_id,
                        txn_app_id=f"{app_id}#events", version=0, target=target,
                        lsn=offset["lsn"], commit_ts=offset["commit_ts"], **timing)
        return offset["lsn"]

    def _recover(self, target: str, app_id: str, checkpoint: str, facts_table: str,
                 state: dict | None, interval_days: float, bootstrap: bool) -> dict | None:
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
            low = client.min_lsn(ci)
            if client.increment_lsn(start["lsn"]) >= low:
                return None
            lost_to = _ts(client.lsn_to_time(low))
            # newer than the checkpoint and not purged itself: a recovery that stopped
            # before writing its state
            done = self._last_snapshot(target, ci)
            if done and (done["lsn"] <= start["lsn"] or client.increment_lsn(done["lsn"]) < low):
                done = None
        lost = (f"{ci}: CDC cleanup deleted changes after {start['lsn']} before the stream read "
                f"them (min_lsn is {low})")
        last = state.get("failed_at") or (state.get("at") if n else None)
        now = datetime.now(timezone.utc)
        if not done and last and now - datetime.fromisoformat(last) < timedelta(days=interval_days):
            raise DataLossError(
                f"{lost}, and the last automatic re-snapshot (or a failed one) was at {last}, "
                f"less than resnapshot_interval_days={interval_days} ago. The stream does not "
                "keep up with the CDC retention, or stops for longer than it, again: fix that, "
                "then rerun with a smaller resnapshot_interval_days to re-snapshot now.")
        _write_state(checkpoint, {**state, "recovering": start})
        offset, timing = (done, {}) if done else self._take_snapshot(target, ci)
        with closing(make_client(self.options)) as client:
            if client.increment_lsn(offset["lsn"]) < client.min_lsn(ci):
                _write_state(checkpoint, {**state, "recovering": start,
                                          "failed_at": now.isoformat(timespec="seconds")})
                raise DataLossError(
                    f"{lost}. The re-snapshot at {offset['lsn']} took longer than the CDC "
                    "retention, so the changes after it are gone too: lengthen the retention or "
                    "speed up the read (numPartitions), then rerun with "
                    "resnapshot_interval_days=0.")
        n += 1
        write_event(self.spark, facts_table, "resnapshot", app_id=_generation(checkpoint, app_id, n)[1],
                    txn_app_id=f"{app_id}#events", version=n, target=target,
                    lsn=offset["lsn"], commit_ts=offset["commit_ts"], **timing,
                    lost_from_ts=_ts(start["commit_ts"]), lost_to_ts=lost_to,
                    retention_watermark_ts=lost_to)
        state = {"generation": n, "snapshot_lsn": offset["lsn"], "commit_ts": offset["commit_ts"],
                 "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        _write_state(checkpoint, state)
        return state

    def to_delta(self, target: str, app_id: str, checkpoint: str, facts_table: str | None = None,
                 trigger: dict | None = None, query_name: str | None = None, bootstrap: bool = False,
                 on_data_loss: str = "fail", resnapshot_interval_days: float = 7.0):
        """Start the stream into ``target`` through ``delta_sink``; returns the StreamingQuery.

        ``trigger``: keyword arguments for ``DataStreamWriter.trigger``, e.g.
        ``{"availableNow": True}``. ``bootstrap``: snapshot the table first (see
        ``snapshot``); a checkpoint that already has offsets ignores the starting LSN.
        ``on_data_loss``: ``"fail"`` (the query stops with ``DataLossError``) or
        ``"resnapshot"`` (recover before starting, in a new generation; needs ``facts_table``:
        see the module doc).
        ``resnapshot_interval_days`` must exceed the CDC retention: a second loss within it
        raises ``DataLossError`` instead of snapshotting again.
        """
        from . import register
        from .sink import delta_sink

        if on_data_loss not in ("fail", "resnapshot"):
            raise ValueError(f"on_data_loss must be 'fail' or 'resnapshot', not {on_data_loss!r}")
        if on_data_loss == "resnapshot":
            # Spark resolves /dbfs/x as dbfs:/dbfs/x, Python as dbfs:/x: two directories
            if _URI.match(checkpoint) or f"{checkpoint}/".startswith("/dbfs/"):
                raise ValueError(
                    "on_data_loss='resnapshot' reads Spark's checkpoint from Python: use a path "
                    "both resolve to the same directory (local, or /Volumes/...), not a URI or "
                    "/dbfs/...")
            if not facts_table:
                raise ValueError("on_data_loss='resnapshot' needs a facts_table: its event rows "
                                 "tell downstream to rebuild, and from which LSN")
        if bootstrap and _opt(self.options, "startingLsn"):
            raise ValueError("bootstrap=True sets startingLsn itself; pass one or the other")
        register(self.spark)
        state = _read_state(checkpoint)
        if on_data_loss == "resnapshot":
            state = self._recover(target, app_id, checkpoint, facts_table, state,
                                  resnapshot_interval_days, bootstrap) or state
        n = state["generation"] if state else 0  # a gen-0 state only records a recovery
        checkpoint, sink_id = _generation(checkpoint, app_id, n)
        options = dict(self.options)
        if n:  # the generation starts at its snapshot, whatever startingLsn said
            options = {k: v for k, v in options.items() if k.lower() != "startinglsn"}
            options["startingLsn"] = state["snapshot_lsn"]
        elif bootstrap:
            options["startingLsn"] = self._bootstrap(target, app_id, facts_table)
        metrics = _opt(options, "metricsPath")
        if facts_table and metrics is None and not _URI.match(checkpoint):
            metrics = options["metricsPath"] = os.path.join(checkpoint, "_mssql_cdc_metrics")
        writer = (
            self.spark.readStream.format("mssql_cdc").options(**options).load()
            .writeStream.foreachBatch(delta_sink(target, sink_id, facts_table,
                                                 metrics_path=metrics if facts_table else None))
            .option("checkpointLocation", checkpoint)
        )
        if trigger:
            writer = writer.trigger(**trigger)
        if query_name:
            writer = writer.queryName(query_name)
        return writer.start()


def stream(spark, options: dict) -> CdcStream:
    """The CDC stream described by ``options`` (the data source options)."""
    return CdcStream(spark, options)
