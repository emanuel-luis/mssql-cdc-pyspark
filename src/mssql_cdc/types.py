"""The types of the public API: the values a mode parameter takes, and what the calls return.

The results are ``TypedDict``s: plain dicts at run time, with keys a type checker knows. A
minor release may add keys to them, never remove or rename one (ADR 0021).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal, TypedDict

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

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
    """The run's report rows (``REPORT_COLUMNS``), as ``report_table`` gets them."""
