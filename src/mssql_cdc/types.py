"""The types of the public API: the values a mode parameter takes, and what the calls return.

The results are ``TypedDict``s: plain dicts at run time, with keys a type checker knows. A
minor release may add keys to them; removing or renaming one is a break the changelog lists
under "Breaking" (ADR 0021).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal, TypeAlias, TypedDict

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
    from pyspark.sql.connect.streaming.query import StreamingQuery as ConnectStreamingQuery
    from pyspark.sql.streaming.query import StreamingQuery

SparkSessionLike: TypeAlias = "SparkSession | ConnectSparkSession"
"""What a ``spark`` parameter takes: a classic SparkSession or a Spark Connect one (Databricks
Connect, serverless compute), which is not its subclass. For type checkers: at run time it is
a string."""

StreamingQueryLike: TypeAlias = "StreamingQuery | ConnectStreamingQuery"
"""What a ``query`` parameter takes: a classic StreamingQuery or a Spark Connect one. For type
checkers: at run time it is a string."""

SnapshotMode = Literal["full", "chunked"]
"""How ``to_delta(snapshot=...)`` takes a snapshot: before the stream, or in chunks next to it."""

OnDataLoss = Literal["fail", "resnapshot"]
"""What ``to_delta(on_data_loss=...)`` does when CDC cleanup purged changes not read yet."""

Isolation = Literal["snapshot", "readCommitted"]
"""The isolation level ``backfill(isolation=...)`` reads and plans under (any case at run time)."""

Granularity = Literal["minute", "hour", "day"]
"""The period ``finalized_until`` is truncated to."""

BackfillState = Literal["done", "running", "waiting_headroom", "waiting_metrics", "no_snapshot"]
"""Why ``backfill()`` returned: see ``BackfillStatus``."""


class Offset(TypedDict):
    """A position in the change log, as in a stream's offsets: what ``snapshot()`` and
    ``seed()`` return, for the stream's ``startingLsn``."""

    lsn: str
    """The LSN, ``0x`` and 20 uppercase hex digits."""
    commit_ts: str
    """Its commit time, UTC ISO-8601 to the millisecond; ``""`` when unknown."""


class BackfillStatus(TypedDict):
    """What ``backfill()`` returns: how far the chunked snapshot got."""

    snapshot: str | None
    """The snapshot's LSN S; None with state ``no_snapshot``."""
    chunks_done: int
    """Chunks read so far."""
    chunks_total: int | None
    """The plan's chunks; None until a call has planned them."""
    done: bool
    """The snapshot is complete: stop calling."""
    paused: bool
    """The call waited instead of reading: see ``state``."""
    state: BackfillState
    """``done``; ``running`` (``max_waves`` or ``max_seconds`` ran out: call again);
    ``waiting_headroom`` and ``waiting_metrics`` (paused by ``min_headroom_hours``);
    ``no_snapshot`` (no chunked snapshot of ``target`` opened by ``app_id``'s stream)."""
    reason: str | None
    """``state`` in words, when paused."""


class ApplyResult(TypedDict):
    """What ``apply_changes()`` returns."""

    rebuilt: bool
    """Silver was rebuilt from a snapshot in this call."""
    applied_lsn: str | None
    """How far silver is applied: the highest ``_start_lsn`` of the changes in it."""
    finalized_until: datetime | None
    """Silver's verdict after the call, naive UTC."""
    bronze_found: bool
    """False when ``bronze`` does not exist yet: nothing was applied."""


class Freshness(TypedDict):
    """What ``finalization.freshness()`` returns: how old a table's verdict is, for alerts."""

    table_name: str
    """The control row's name, as passed."""
    finalized_until: datetime | None
    """The verdict, naive UTC; None before the first."""
    updated_at: datetime | None
    """When the verdict last moved, naive UTC; None before the first."""
    age_seconds: float | None
    """Seconds from ``finalized_until``, the last final instant, to now. The first period not
    final yet ends one period after it, so its consumers have waited this less the period,
    when that is positive. A current stream keeps it under the period (an hour by default)
    plus its trigger interval and the idle entries' 5 minutes."""
    seconds_since_update: float | None
    """Seconds since the verdict last moved. Small with a large ``age_seconds``: the verdict
    moves, the stream is behind. Large too: it stopped moving."""
    open_snapshot_lsn: str | None
    """Tables built by ``apply_changes``: the LSN of the chunked snapshot still open, which
    holds the verdict until the snapshot completes; None otherwise."""


class ReconcileResult(TypedDict):
    """What ``reconcile()`` returns: a summary of the run, and its report."""

    run_id: str
    """The run's id (a UUID), the ``run_id`` of its report rows."""
    silver_version: int
    """The Delta version of silver compared."""
    silver_lsn: str | None
    """Silver's ``applied_lsn`` read before that version."""
    source_lsn: str
    """``sys.fn_cdc_get_max_lsn()`` read just before the source was counted."""
    buckets: int
    """Buckets counted on both sides."""
    match: int
    """Buckets whose counts match."""
    in_flight: int
    """Buckets that differ while a change to them is in flight: check again later."""
    mismatch: int
    """Buckets that differ."""
    hashed: int
    """Buckets (or ranges) compared row by row."""
    failures: dict[str, int]
    """Report rows by ``failure_type``: ``MISSING_TARGET``, ``MISSING_SOURCE``,
    ``RECORD_DIFF``, ``IN_FLIGHT``, ``CHUNK_TILING``, ``CHUNK_ROWS``, ``CHUNK_STAMP``."""
    report: DataFrame
    """The run's report rows, in the columns of the reconcile report table, as
    ``report_table`` gets them."""
