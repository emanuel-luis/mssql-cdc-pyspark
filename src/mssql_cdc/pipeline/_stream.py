"""``CdcStream``, what ``stream()`` returns: snapshots, recovery and ``backfill()`` from its
parts, and ``to_delta``, which starts the stream into a target."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from .. import events
from ..types import OnDataLoss, SnapshotMode
from ._chunked import _Chunked
from ._common import _URI, _log, _opt
from ._recovery import _generation, _read_state, _Recovery

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming.query import StreamingQuery

    from ..source import SourceOptions
    from ..types import SparkSessionLike


class CdcStream(_Recovery, _Chunked):
    def __init__(self, spark: SparkSessionLike, options: SourceOptions | Mapping[str, Any]):
        from .. import register  # lazy: the package imports this module
        from ..source import warn_unknown

        self.spark: SparkSessionLike = spark
        self.options: dict[str, Any] = dict(options)
        warn_unknown(self.options)  # here, in the caller's log; the reader's goes to a worker's
        register(spark)

    def to_delta(
        self,
        target: str,
        app_id: str,
        checkpoint: str,
        facts_table: str | None = None,
        *,
        trigger: dict[str, Any] | None = None,
        query_name: str | None = None,
        bootstrap: bool = False,
        on_data_loss: OnDataLoss = "fail",
        resnapshot_interval_days: float = 7.0,
        snapshot_on_switch: bool = False,
        snapshot: SnapshotMode = "full",
    ) -> StreamingQuery:
        """Start the stream into ``target`` through ``delta_sink``; returns the StreamingQuery.
        The parameters after ``facts_table`` are keyword-only.

        ``trigger``: keyword arguments for ``DataStreamWriter.trigger``, e.g.
        ``{"availableNow": True}``. ``query_name``: the query's name; the sink app_id (with
        its generation) when None. ``bootstrap``: snapshot the table first (see
        ``snapshot``); a checkpoint that already has offsets ignores the starting LSN.
        ``snapshot``: how ``bootstrap`` and ``on_data_loss="resnapshot"`` take one.
        ``"full"`` reads the table before the stream starts; ``"chunked"`` (needs
        ``facts_table``) only opens one at an LSN S and starts the stream there at once, and
        ``backfill()``, run apart, reads it in chunks next to the stream (ADR 0028). With a
        ``facts_table`` the mode may change between runs, but a snapshot of the other mode
        still open (not completed) raises.
        ``on_data_loss``: ``"fail"`` (the query stops with ``DataLossError``) or
        ``"resnapshot"`` (recover before starting, in a new generation; needs ``facts_table``,
        ``failOnDataLoss`` true and a checkpoint that is a local or FUSE path, not a URI or
        ``/dbfs/``).
        ``resnapshot_interval_days`` must exceed the CDC retention: a second loss within it
        raises ``DataLossError`` instead of snapshotting again.
        ``snapshot_on_switch``: after the batch that first reads a newer capture instance of the
        table, append a snapshot, so that rows unchanged since then carry the columns only the
        newer instance captures, instead of NULL (ADR 0023). It reads the reader's events from
        the metrics directory, so a URI checkpoint needs ``metricsPath``. It reads the whole
        table inside the batch, so not with ``snapshot="chunked"``.
        ``metricsPath``, when given, must be a local or FUSE path every node sees: executors
        write it and the driver reads it with Python file calls.
        """
        from ..sink import delta_sink
        from ..source import _bool

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
        if snapshot_on_switch and chunked:
            raise ValueError(
                "snapshot_on_switch=True reads the whole table inside the stream's batch, which "
                "snapshot='chunked' exists to avoid: use one or the other"
            )
        given_metrics = _opt(self.options, "metricsPath")
        if given_metrics and _URI.match(str(given_metrics)):
            raise ValueError(
                f"metricsPath {given_metrics!r} is a URI, but executors write it and the driver "
                "reads it with Python file calls: use a local or FUSE path every node sees "
                "(e.g. /Volumes/...)"
            )
        if snapshot_on_switch and not given_metrics and _URI.match(checkpoint):
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
            if not _bool(self.options, "failOnDataLoss", "true"):  # a typo raises there
                fail = _opt(self.options, "failOnDataLoss")
                raise ValueError(
                    f"on_data_loss='resnapshot' needs failOnDataLoss true, not {fail!r}: a purge "
                    "while the query runs would be skipped past, and the next run's check could "
                    "no longer see the gap"
                )
        if bootstrap and _opt(self.options, "startingLsn"):
            raise ValueError("bootstrap=True sets startingLsn itself; pass one or the other")
        if facts_table and (bootstrap or on_data_loss == "resnapshot"):
            self._lock(facts_table, target, app_id, snapshot)  # one mode while one is open
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
            _log.info("mssql_cdc: bootstrap of %s for %s (%s snapshot)", target, app_id, snapshot)
            options["startingLsn"] = self._bootstrap(target, app_id, facts_table, chunked)
            _log.info(
                "mssql_cdc: bootstrap of %s for %s: the stream starts at %s",
                target,
                app_id,
                options["startingLsn"],
            )
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
        # named after the sink, the facts' app_id: Spark's UI and progress events match them
        return writer.queryName(query_name or sink_id).start()


def _snapshot_after_switch(
    sink: Callable[[DataFrame, int], None],
    options: dict[str, Any],
    ci: str,
    target: str,
    metrics: str,
) -> Callable[[DataFrame, int], None]:
    """``sink``, then a snapshot of the table after the batch that first read a newer capture
    instance (``to_delta(snapshot_on_switch=True)``). Holds no session: Spark Connect pickles
    ``foreachBatch`` functions."""
    from ..sink import _read_events

    def write(df: DataFrame, batch_id: int) -> None:
        switched = any(
            e.get("event") == events.CAPTURE_INSTANCE_SWITCHED for e in _read_events(metrics)[1]
        )
        sink(df, batch_id)  # folds the events into the facts and removes their files
        if switched:  # a replay after a crash here snapshots again: harmless, only slower
            CdcStream(df.sparkSession, options)._take_snapshot(target, ci)

    return write


def stream(spark: SparkSessionLike, options: SourceOptions | Mapping[str, Any]) -> CdcStream:
    """The CDC stream described by ``options`` (the data source options)."""
    return CdcStream(spark, options)
