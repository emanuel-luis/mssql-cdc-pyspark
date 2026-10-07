"""The facts table's event rows in one place: their names, the rows their writers append and
the readers that find them again (``pipeline``, ``silver``, ``reconcile``).

A facts row whose ``event`` is NULL is a micro-batch's (``sink.delta_sink``); every other row
is an event:

* 'schema_change' and 'capture_instance_switched' (ADR 0023), 'data_skipped' (ADR 0018): the
  reader leaves them as files in ``metricsPath`` (a task's 'data_skipped' rides in its metrics
  file), and the sink writes them (``event_row``) in the commit of the batch that read past
  them, with its ``batch_id``.
* 'snapshot_open': a snapshot of either mode, before it reads the table
  (``SnapshotOpenDetail``); 'snapshot_plan': a chunked one's chunks, planned once by its first
  ``backfill()`` (``SnapshotPlanDetail``); 'snapshot_chunk': one per chunk read
  (``chunk_row``, ``SnapshotChunkDetail``) (ADR 0028).
* 'bootstrap' and 'resnapshot' (``SNAPSHOTS``): a snapshot complete (ADR 0016, 0018), a seed's
  too (ADR 0025); a chunked one's after its last chunk, with ``SnapshotCompletionDetail``.
  Only these are snapshots: downstream rebuilds from them.

Snapshot events have no ``batch_id`` and go through ``write_event``, skipped by Delta when
their ``txn_app_id`` already wrote that version. The names and the ``detail`` payloads
(``payloads``) are state (ADR 0021): a release adds one, never renames or drops one.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row

    from .payloads import SnapshotChunkDetail, SnapshotPlanDetail, WaveChunk
    from .types import SnapshotMode, SparkSessionLike

SCHEMA_CHANGE: Final = "schema_change"
CAPTURE_INSTANCE_SWITCHED: Final = "capture_instance_switched"
DATA_SKIPPED: Final = "data_skipped"
SNAPSHOT_OPEN: Final = "snapshot_open"
SNAPSHOT_PLAN: Final = "snapshot_plan"
SNAPSHOT_CHUNK: Final = "snapshot_chunk"
BOOTSTRAP: Final = "bootstrap"
RESNAPSHOT: Final = "resnapshot"
SNAPSHOTS: Final = (BOOTSTRAP, RESNAPSHOT)
"""The events that are snapshots, each a snapshot's completion."""


# -- writers ------------------------------------------------------------------------
def write_event(
    spark: SparkSessionLike,
    facts_table: str,
    event: str,
    *,
    app_id: str,
    txn_app_id: str | None,
    version: int,
    target: str,
    lsn: str,
    commit_ts: str,
    rows: int | None = None,
    started_at: datetime | None = None,
    duration_ms: int | None = None,
    lost_from_ts: datetime | None = None,
    lost_to_ts: datetime | None = None,
    detail: str | None = None,
) -> None:
    """Record a snapshot (``event`` 'bootstrap', 'resnapshot', the 'snapshot_open' written
    before either is read or a chunked snapshot's 'snapshot_plan') as one facts row.

    The row has no ``batch_id``; ``lsn`` and ``commit_ts`` are the snapshot's offset.
    Idempotent like the batch rows: a rerun with the same ``txn_app_id`` and ``version``
    is skipped by Delta. ``txn_app_id`` None: appended every time (a full snapshot's open).
    """
    from .sink import _headroom, write_facts

    ts = datetime.fromisoformat(commit_ts) if commit_ts else None
    facts = {
        "app_id": app_id,
        "rows": rows,
        "min_lsn": lsn,
        "max_lsn": lsn,
        "min_commit_ts": ts,
        "max_commit_ts": ts,
        "deletes": 0,
        "inserts": 0,
        "updates": 0,
        "started_at": started_at,
        "duration_ms": duration_ms,
        **_headroom(lost_to_ts, ts),
        "end_lsn": lsn,  # the offset the stream starts from
        "end_commit_ts": ts,
        "event": event,
        "lost_from_ts": lost_from_ts,
        "lost_to_ts": lost_to_ts,
        "detail": detail,
        "target": target,
    }
    write_facts(spark, facts_table, [facts], txn_app_id, version)


def event_row(event: Mapping[str, Any], **batch: Any) -> dict[str, Any]:
    """A facts row for one of the reader's events: in the batch that read past it, 0 rows;
    a 'data_skipped' one also has the gap (ADR 0018)."""
    ts, lost_from, lost_to = (
        datetime.fromisoformat(event[k]) if event.get(k) else None
        for k in ("commit_ts", "lost_from_ts", "lost_to_ts")
    )
    lsn = event["lsn"]
    return {
        **batch,
        "event": event["event"],
        "detail": event.get("detail"),
        "lost_from_ts": lost_from,
        "lost_to_ts": lost_to,
        "rows": 0,
        "deletes": 0,
        "inserts": 0,
        "updates": 0,
        "min_lsn": lsn,
        "max_lsn": lsn,
        "end_lsn": lsn,
        "min_commit_ts": ts,
        "max_commit_ts": ts,
        "end_commit_ts": ts,
    }


def chunk_row(
    c: WaveChunk,
    *,
    app_id: str,
    target: str,
    snapshot: str,
    wave: int,
    lsn: str,
    lsn_ts: datetime | None,
    high_ts: datetime | None,
    started_at: datetime,
    duration_ms: int,
) -> dict[str, Any]:
    """The 'snapshot_chunk' facts row of chunk ``c`` of wave ``wave``, stamped ``lsn``:
    ``min_lsn`` the stamp, ``max_lsn`` how far capture had got after the read, with their
    commit times ``lsn_ts`` and ``high_ts``; the wave's timing."""
    return {
        "app_id": app_id,
        "rows": c["rows"],
        "min_lsn": lsn,
        "max_lsn": c["high_lsn"],
        "min_commit_ts": lsn_ts,
        "max_commit_ts": high_ts,
        "deletes": 0,
        "inserts": 0,
        "updates": 0,
        "started_at": started_at,
        "duration_ms": duration_ms,
        "read_seconds": c["read_seconds"],
        "read_mb": c["read_mb"],
        "event": SNAPSHOT_CHUNK,
        "detail": json.dumps(chunk_detail(snapshot, wave, c)),
        "target": target,
    }


def chunk_detail(snapshot: str, wave: int, c: WaveChunk) -> SnapshotChunkDetail:
    """A 'snapshot_chunk' row's detail; ``last`` false when an older release's tag lacks it."""
    return {
        "snapshot": snapshot,
        "wave": wave,
        "chunk": c["chunk"],
        "lo": c["lo"],
        "hi": c["hi"],
        "last": c.get("last", False),
    }


# -- readers ------------------------------------------------------------------------
def read(
    spark: SparkSessionLike,
    facts_table: str,
    target: str,
    events: Sequence[str],
    columns: Sequence[str],
    app_id: str | re.Pattern[str] | None = None,
) -> list[Row]:
    """``columns`` of the rows of ``facts_table`` for ``target`` whose event is one of
    ``events``; none before the table exists. ``app_id``: only that sink's rows or, a
    pattern, those whose ``app_id`` it matches whole (a stream's generations; ``columns``
    must hold ``app_id``)."""
    from pyspark.sql import functions as F

    from .tables import delta_table, exists

    if not exists(spark, facts_table):
        return []
    where = (F.col("target") == target) & F.col("event").isin(*events)
    if isinstance(app_id, str):
        where &= F.col("app_id") == app_id
    rows = delta_table(spark, facts_table).toDF().where(where).select(*columns).collect()
    if isinstance(app_id, re.Pattern):
        rows = [r for r in rows if app_id.fullmatch(r["app_id"] or "")]
    return rows


def mode(detail: str | None) -> SnapshotMode:
    """The mode a 'snapshot_open' facts row was opened in: 'full' or 'chunked'."""
    return "full" if json.loads(detail or "{}").get("mode") == "full" else "chunked"


def chunked_opens(rows: Iterable[Row]) -> list[Row]:
    """The 'snapshot_open' rows of chunked snapshots among ``rows``: a full snapshot's open
    row only locks the mode, it has no chunks."""
    return [r for r in rows if r["event"] == SNAPSHOT_OPEN and mode(r["detail"]) == "chunked"]


def completions(rows: Iterable[Row]) -> list[str]:
    """The LSNs (``max_lsn``) of the 'bootstrap' and 'resnapshot' rows among ``rows``: where
    a snapshot completed, a chunked one at its S."""
    return [r["max_lsn"] for r in rows if r["event"] in SNAPSHOTS]


def plan_of(rows: Iterable[Row], snapshot: str) -> SnapshotPlanDetail | None:
    """The detail of the 'snapshot_plan' facts row of ``snapshot`` among ``rows``, or None:
    ``{snapshot, kind, keys, chunk_rows, chunks}``, chunk i from ``chunks[i][0]`` to
    ``chunks[i][1]`` (``client.plan_chunks``)."""
    for r in rows:
        if r["event"] == SNAPSHOT_PLAN and (d := json.loads(r["detail"]))["snapshot"] == snapshot:
            plan: SnapshotPlanDetail = d
            return plan
    return None


def chunks_of(rows: Iterable[Row], snapshot: str) -> list[tuple[SnapshotChunkDetail, Row]]:
    """The 'snapshot_chunk' rows of ``snapshot`` among ``rows``, each with its detail."""
    found: list[tuple[SnapshotChunkDetail, Row]] = []
    for r in rows:
        if r["event"] == SNAPSHOT_CHUNK:
            d: SnapshotChunkDetail = json.loads(r["detail"])
            if d["snapshot"] == snapshot:
                found.append((d, r))
    return found


def where_chunked_open(facts: DataFrame) -> DataFrame:
    """``chunked_opens`` on a facts DataFrame, uncollected."""
    from pyspark.sql import functions as F

    full = F.get_json_object("detail", "$.mode").eqNullSafe("full")
    return facts.where((F.col("event") == SNAPSHOT_OPEN) & ~full)


def where_chunks(facts: DataFrame, snapshot: str) -> DataFrame:
    """``chunks_of`` on a facts DataFrame, uncollected: its rows, without their detail."""
    from pyspark.sql import functions as F

    return facts.where(
        (F.col("event") == SNAPSHOT_CHUNK) & (F.get_json_object("detail", "$.snapshot") == snapshot)
    )
