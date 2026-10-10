"""The JSON payloads a later release reads back (ADR 0021): the ``detail`` of the facts rows,
and the ``userMetadata`` of a chunked snapshot's wave commits in bronze.

``TypedDict``s, plain dicts at run time, for what ``json.loads`` of those columns returns::

    d: SnapshotChunkDetail = json.loads(row["detail"])

A release only adds keys to them, never renames or removes one, nor gives one another meaning.
A key added after its payload first shipped is optional here, and a reader takes it with a
default (``.get``), so rows an older release wrote stay readable. The one exception: since
0.7.0 a chunked snapshot's facts rows leave its key values (``keys`` and ``plan`` of
'snapshot_open', ``chunks`` of 'snapshot_plan', ``lo`` and ``hi`` of 'snapshot_chunk') to
the plans table next to bronze, whose ``detail`` holds the payload whole (ADR 0028
Amendment 4, ADR 0021): optional here, as a facts row may lack them.

Key values (``lo``, ``hi``, ``max``) are JSON: a scalar for one key column, a list for a
composite key, ``None`` for an open end; a value JSON has no type for (a date, a decimal) is
its text, binary its ``0x`` hex.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict

from .types import SnapshotMode

SnapshotKind = Literal["bootstrap", "resnapshot"]
"""Why a snapshot is taken: the first one of the target, or one after CDC data loss."""


class IntExtent(TypedDict):
    """A chunked snapshot's extent for one integer key, read just after S."""

    kind: Literal["int"]
    lo: int
    """The key's MIN."""
    hi: int
    """The key's MAX: the chunks end at MAX + 1, the keys above are the stream's."""
    rows: int
    """The table's rows, from metadata, not a count."""


class KeysetExtent(TypedDict):
    """A chunked snapshot's extent for any other key, read just after S."""

    kind: Literal["keyset"]
    max: Any
    """The key's MAX; None (one chunk, the whole table) for an empty table, a table without a
    unique index, or a key type no bound can be bound as."""


SnapshotExtent = IntExtent | KeysetExtent
"""The ``plan`` of a chunked snapshot's ``'snapshot_open'`` row: what its chunks tile."""


class _SnapshotOpen(TypedDict):
    mode: SnapshotMode
    """How it is taken; a reader takes a missing or unknown one as ``chunked``."""
    kind: SnapshotKind
    generation: int
    """The stream generation it starts (ADR 0018): 0 for a bootstrap."""
    lost_from_ts: str | None
    """A re-snapshot's: the commit time (UTC ISO-8601) of the last offset processed."""
    lost_to_ts: str | None
    """A re-snapshot's: the commit time of the ``min_lsn`` cleanup had got to."""


class SnapshotOpenDetail(_SnapshotOpen, total=False):
    """``detail`` of a ``'snapshot_open'`` facts row, written before a snapshot reads the
    table; ``keys`` and ``plan`` only on a chunked one, and since 0.7.0 only in its plans
    table's row."""

    keys: list[str]
    """The key columns its chunks are cut on."""
    plan: SnapshotExtent


class _SnapshotPlan(TypedDict):
    snapshot: str
    """The snapshot's LSN S."""
    kind: Literal["int", "keyset"]
    """The ``kind`` of its extent."""
    keys: list[str]
    chunk_rows: int


class SnapshotPlanDetail(_SnapshotPlan, total=False):
    """``detail`` of a ``'snapshot_plan'`` facts row: every chunk of a chunked snapshot,
    planned by its first ``backfill()`` and fixed while it is open; ``chunks`` since 0.7.0
    only in its plans table's row."""

    chunks: list[list[Any]]
    """``[lo, hi]`` of every chunk, in order: from ``lo`` (inclusive) to ``hi`` (exclusive)."""
    chunk_count: int
    """How many chunks, since 0.7.0."""


class _SnapshotChunk(TypedDict):
    snapshot: str
    wave: int
    """The ``backfill()`` wave that read it, from 0."""
    chunk: int
    """Its index in the plan."""
    last: bool
    """The plan's final chunk, whose read completes the snapshot."""


class SnapshotChunkDetail(_SnapshotChunk, total=False):
    """``detail`` of a ``'snapshot_chunk'`` facts row: a chunk of a chunked snapshot, read;
    ``lo`` and ``hi``, the key range it was read in, since 0.7.0 only in its plans table's
    row."""

    lo: Any
    hi: Any


class SnapshotCompletionDetail(TypedDict):
    """``detail`` of a chunked snapshot's ``'bootstrap'`` or ``'resnapshot'`` row, written
    after its last chunk (a full snapshot's has none)."""

    snapshot: str
    chunks: int
    rows: int
    last_lsn: str
    """The newest stamp of its chunks."""


# functional syntax: "from" is a keyword. The reference reads code statically and lists no
# member of it, so DataSkippedDetail's docstring names these in an Attributes section.
_DataSkipped = TypedDict("_DataSkipped", {"from": str, "to": str, "certain": bool})


class DataSkippedDetail(_DataSkipped, total=False):
    """``detail`` of a ``'data_skipped'`` facts row (ADR 0018): from the driver, ``certain``
    true; or, from a task that found cleanup had run while it read a range, ``certain`` false
    and a ``reason``.

    Attributes:
        from (str): The first LSN not read; from a task, its range's first LSN.
        to (str): The ``min_lsn`` reading resumed at; from a task, the one it found.
        certain (bool): True when the changes are lost (the driver's check), false when they
            may be (a task's).
    """

    reason: str
    """Why the task flagged its range, from a task only; text for people, which may change."""


class BatchDetail(TypedDict):
    """``detail`` of a micro-batch row (``event`` NULL), set only when it has warnings."""

    warnings: list[str]
    """The warnings the reader logged on the driver; text for people, which may change."""


class WaveChunk(TypedDict):
    """A chunk in a wave's ``userMetadata``."""

    chunk: int
    lo: Any
    hi: Any
    last: bool
    rows: int | None
    """None where the platform refuses to cache the wave (Databricks serverless): its commit
    is written as it is read, before the rows are counted; its 'snapshot_chunk' facts row
    has them (ADR 0032)."""
    high_lsn: str | None
    """``max_lsn`` after the read: informational; None when it could not be read, and where
    ``rows`` is None."""
    read_seconds: float | None
    read_mb: float | None


class WaveMetadata(TypedDict):
    """The ``userMetadata`` of the bronze commit that appended a ``backfill()`` wave."""

    backfill: str
    """``<app_id>#snap.<S>``, the commit's ``txnAppId``."""
    wave: int
    """The commit's ``txnVersion``."""
    lsn: str
    """The wave's stamp: ``max_lsn`` recorded before its read, at or after S."""
    attempt: str
    """A UUID per attempt: tells this commit from an earlier attempt's."""
    chunks: list[WaveChunk]
