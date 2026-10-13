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

A chunked snapshot's key values (its extent, every chunk's bounds) are in a plans table next
to bronze (``plans_table``), not in the facts: its 'snapshot_open', 'snapshot_plan' and
'snapshot_chunk' rows leave out the keys ``_IN_PLANS`` names, and a row of the plans table
holds each one's ``detail`` whole, written first. A snapshot opened before 0.7.0 keeps them
in its facts rows to its end; readers take the whole payload either way (``Plans.whole``)
(ADR 0028 Amendment 4).
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row

    from .payloads import SnapshotChunkDetail, SnapshotPlanDetail, WaveChunk
    from .tables import ColumnDef
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

_IN_PLANS: Final = {
    SNAPSHOT_OPEN: ("keys", "plan"),
    SNAPSHOT_PLAN: ("chunks",),
    SNAPSHOT_CHUNK: ("lo", "hi"),
}
"""The keys of a chunked snapshot's payloads its facts rows leave to the plans table: the
source's key values, and on 'snapshot_open' the key columns too, so that a release before
0.7.0 reading such a row cuts no key range it has no bounds for."""

PLANS_COMMENT = (
    "Key values of the chunked snapshots stream().backfill() reads into a bronze table, kept "
    "by mssql-cdc-pyspark next to it, under its name with _snapshot_plans appended: one row "
    "per 'snapshot_open', 'snapshot_plan' and 'snapshot_chunk' row of the ingestion facts "
    "table, with that row's detail whole. The facts rows leave these values out; they are "
    "values of the source table's key, so give this table bronze's access policy."
)
PLANS_COLUMNS: list[ColumnDef] = [
    (
        "snapshot",
        "STRING",
        (
            "The snapshot's LSN S (0x + 20 hex): min_lsn of its 'snapshot_open' facts row, and "
            "_snapshot of its rows in bronze."
        ),
    ),
    (
        "event",
        "STRING",
        (
            "The facts row of the snapshot this one completes: 'snapshot_open', "
            "'snapshot_plan' or 'snapshot_chunk' (with the chunk and wave of its detail)."
        ),
    ),
    (
        "detail",
        "STRING",
        (
            "JSON, the facts row's detail with the keys it leaves out: 'snapshot_open' {mode, "
            "kind, generation, lost_from_ts, lost_to_ts, keys, plan, and switch_batch_id on one "
            "opened after a capture instance switch}, keys the columns the "
            "chunks are cut on and plan the key's extent read just after S ({kind: 'int', lo, "
            "hi, rows} for one integer key, else {kind: 'keyset', max}); 'snapshot_plan' "
            "{snapshot, kind, keys, chunk_rows, chunk_count, chunks}, chunks the [lo, hi] of "
            "every chunk in order, fixed while the snapshot is open; 'snapshot_chunk' "
            "{snapshot, chunk, wave, lo, hi, last}, the key range the chunk was read in, from lo "
            "(inclusive) to hi (exclusive). A bound is null for an open end and a list for a "
            "composite key."
        ),
    ),
]
_PLANS_SCHEMA = ", ".join(f"{name} {data_type}" for name, data_type, _ in PLANS_COLUMNS)


def plans_table(target: str) -> str:
    """The plans table of bronze ``target``: ``<target>_snapshot_plans``, in the same schema
    for a name, a sibling directory for a path."""
    if target.endswith("`"):  # a quoted name: the suffix inside the quotes
        return target[:-1] + "_snapshot_plans`"
    return target.rstrip("/") + "_snapshot_plans"


def in_facts(event: str, detail: Mapping[str, Any]) -> bool:
    """Whether ``detail`` of a chunked snapshot's facts row of ``event`` holds its key values,
    as a snapshot opened before 0.7.0 writes them."""
    return any(k in detail for k in _IN_PLANS[event])


def public(event: str, detail: Mapping[str, Any]) -> dict[str, Any]:
    """``detail`` of a chunked snapshot's facts row of ``event``, without the keys its plans
    table holds."""
    return {k: v for k, v in detail.items() if k not in _IN_PLANS[event]}


def write_plans(
    spark: SparkSessionLike,
    target: str,
    snapshot: str,
    event: str,
    details: Sequence[Mapping[str, Any]],
    txn_app_id: str | None,
    version: int,
) -> None:
    """Append the whole ``details`` of the snapshot at ``snapshot``'s facts rows of ``event``
    to ``target``'s plans table, created first, in one commit: before those rows, which
    leave out what these hold. Skipped by Delta when ``txn_app_id`` already wrote
    ``version`` (never when None)."""
    from . import migrations
    from .sink import _write

    table = plans_table(target)
    migrations.ensure(spark, table, "plans", PLANS_COLUMNS, PLANS_COMMENT)
    rows = [(snapshot, event, json.dumps(d)) for d in details]
    _write(spark.createDataFrame(rows, _PLANS_SCHEMA), table, txn_app_id, version)


@dataclass(frozen=True, slots=True)
class Plans:
    """The rows of a chunked snapshot in its plans table, which complete its facts rows."""

    table: str
    snapshot: str
    details: Mapping[tuple[str, int | None, int | None], Any]
    """Each row's ``detail``, by its event and, on a chunk, its chunk and wave."""

    def whole(self, event: str, detail: Mapping[str, Any]) -> Any:
        """``detail`` of the snapshot's facts row of ``event``, whole: as the row has it when
        a release before 0.7.0 wrote it, else from this table. Raises when it is not here."""
        if in_facts(event, detail):
            return detail
        found = self.details.get((event, detail.get("chunk"), detail.get("wave")))
        if found is None:
            which = "" if event != SNAPSHOT_CHUNK else f" for chunk {detail.get('chunk')}"
            raise ValueError(
                f"{self.table} has no {event!r} row of the snapshot at {self.snapshot}{which}, "
                "whose facts row leaves its key values to it: was it dropped or renamed? It is "
                "found by bronze's name, as given to to_delta"
            )
        return found


def plans(spark: SparkSessionLike, target: str, snapshot: str) -> Plans:
    """The rows of the snapshot at ``snapshot`` in bronze ``target``'s plans table; none
    before the table exists."""
    from pyspark.sql import functions as F

    from .tables import delta_table, exists

    table, found = plans_table(target), {}
    if exists(spark, table):
        rows = delta_table(spark, table).toDF().where(F.col("snapshot") == snapshot).collect()
        for r in rows:
            d = json.loads(r["detail"])
            found[(r["event"], d.get("chunk"), d.get("wave"))] = d
    return Plans(table, snapshot, found)


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
    whole: bool,
) -> dict[str, Any]:
    """The 'snapshot_chunk' facts row of chunk ``c`` of wave ``wave``, stamped ``lsn``:
    ``min_lsn`` the stamp, ``max_lsn`` how far capture had got after the read, with their
    commit times ``lsn_ts`` and ``high_ts``; the wave's timing. Its bounds only when
    ``whole`` (a snapshot opened before 0.7.0), else they are in the plans table."""
    detail = chunk_detail(snapshot, wave, c)
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
        "detail": json.dumps(detail if whole else public(SNAPSHOT_CHUNK, detail)),
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


def plan_of(rows: Iterable[Row], snapshot: str, kept: Plans) -> SnapshotPlanDetail | None:
    """The detail of the 'snapshot_plan' facts row of ``snapshot`` among ``rows``, whole
    (``kept``, the snapshot's plans), or None: ``{snapshot, kind, keys, chunk_rows, chunks}``,
    chunk i from ``chunks[i][0]`` to ``chunks[i][1]`` (``client.plan_chunks``)."""
    for r in rows:
        if r["event"] == SNAPSHOT_PLAN and (d := json.loads(r["detail"]))["snapshot"] == snapshot:
            plan: SnapshotPlanDetail = kept.whole(SNAPSHOT_PLAN, d)
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
