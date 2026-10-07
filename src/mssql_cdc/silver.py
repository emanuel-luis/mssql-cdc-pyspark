"""Apply the bronze change log to a current-state ("silver") Delta table (ADR 0019).

    from mssql_cdc import apply_changes
    apply_changes(spark, "bronze.orders", "silver.orders", capture_instance="dbo_orders",
                  keys=["order_id"], control_table="ops.table_finalization",
                  facts_table="ops.ingestion_facts")

Each call applies what the capture instance's rows in ``bronze`` hold beyond the last one:
the latest image per key by ``(_start_lsn, _command_id, _seqval, _operation)``; operation
1 deletes the key, 0 (snapshot), 2 and 4 upsert it, and 3 (the row before an update)
deletes its own key: the 4 of an update that keeps the key outranks it, so a 3 is the latest
row only when the update moved the row to another key. A bronze without ``_command_id``
(``includeCommandId=false``) is ordered by ``(_start_lsn, _seqval, _operation)``, as the
reader orders it then: within a transaction, by ``__$seqval``.
Run it after the stream, in the same job or another; one job per silver table.

* Capture instances: after the stream switched to a newer capture instance of the table,
  bronze holds rows of both (ADR 0023); with ``options`` both are read from SQL Server.
  ``_command_id`` is numbered per instance, but all rows of one ``_start_lsn`` come from
  one, so the order holds. A row in the range of any other instance fails the call rather
  than being skipped. A column bronze gained is added to silver (older rows read NULL).

* Position: ``applied_lsn`` in the control table, the highest ``_start_lsn`` of the changes
  applied, recorded after the MERGE. A crash in between leaves it behind, and applying from
  behind changes nothing: each key takes the latest image of a range that reaches the head
  of bronze, and a row only takes an image newer than its own ``_start_lsn``.
* Snapshots: a snapshot is its LSN S (``_snapshot`` of its rows, ``_start_lsn`` on rows
  written before that column). When bronze holds a complete snapshot newer than
  ``snapshot_lsn``, the one silver was last rebuilt from, silver is rebuilt from its rows and
  the changes after S, so keys absent from the snapshot are deleted, whatever their type.
  Complete: the newest ``max_lsn`` of the facts' 'bootstrap' and 'resnapshot' rows for
  ``bronze`` (an emptied table's snapshot has only its event), or of the operation-0 rows
  that are no chunks. The events carry no capture instance, so ``bronze`` holds one source
  table, as its verdict already requires. The facts rows are matched by ``target`` as
  ``to_delta`` got it: bronze must be named the same here, or a call on chunk rows fails.
* Chunked snapshots (``_chunk`` set) arrive in waves after their 'snapshot_open' facts row,
  stamped at or above S, and are complete at their completion row: ``facts_table`` is
  required. While one is open (a 'snapshot_open' newer than every complete snapshot), a
  bootstrap or a re-snapshot, each call applies the waves its 'snapshot_chunk' rows announce,
  tracked by ``open_snapshot_lsn`` and ``snapshot_wave`` (the chunks land below
  ``applied_lsn``), with every change after S of their keys: a chunk row never brings back a
  key deleted since. With one key of an integer, date or timestamp type, the one the chunks
  are cut on, a chunk also deletes the silver keys of its range [lo, hi) it does not hold
  whose image is older than its stamp L: it saw every commit up to L. So the keys deleted in
  a re-snapshot's purged gap go wave by wave. A datetime2(7) bound with digits below the
  microsecond Spark keeps leaves the keys of that microsecond alone. Either way, the rebuild
  comes at completion, and deletes the keys absent from the snapshot and the changes after it.
* Verdict: silver's ``finalized_until`` advances to the bronze verdict read before bronze
  itself. Bronze commits its rows before its verdict (ADR 0005), so the rows applied hold
  every commit up to it: silver never claims more than it has applied. While a snapshot is
  open, silver lacks keys or holds stale ones: its verdict is held until the rebuild. Without
  ``facts_table`` it is never advanced (a warning says so once per table): a chunked
  snapshot opens, and an emptied table's re-snapshot happens, in the facts alone, before
  bronze holds any row of it.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from . import events, finalization, migrations
from .sink import BRONZE_COLUMN_COMMENTS
from .tables import delta_table, exists, retrying, table_ref
from .types import ApplyResult, Granularity

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, Row
    from pyspark.sql.types import DataType

    from .payloads import SnapshotChunkDetail
    from .source import SourceOptions
    from .tables import ColumnDef
    from .types import SparkSessionLike

_log = logging.getLogger(__name__)
_warned: set[tuple[str, str]] = set()  # (what, table) warned about once in this process

SILVER_COMMENT = (
    "Current state of a SQL Server table, one row per key, built from the bronze change log "
    "by mssql-cdc-pyspark's apply_changes. Deleted rows are removed. How far it is applied is "
    "applied_lsn in the control table; finalized_until there says which periods are complete."
)
SILVER_COLUMNS: list[ColumnDef] = [
    (
        "_start_lsn",
        "STRING",
        (
            "Commit LSN (0x + 20 hex) of the source transaction that wrote this row's current "
            "image; on a row unchanged since a snapshot, the snapshot's LSN."
        ),
    ),
    ("_commit_ts", "TIMESTAMP_NTZ", "Commit time of _start_lsn, UTC."),
]
_META = set(BRONZE_COLUMN_COMMENTS)  # bronze's metadata columns, none of them copied to silver


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _one(df: DataFrame) -> Any:
    row = df.first()
    return row[0] if row else None


def _warn_once(what: str, table: str, message: str, *args: object) -> None:
    if (what, table) not in _warned:
        _warned.add((what, table))
        _log.warning(message, *args)


def _source_keys(capture_instance: str, options: Mapping[str, Any] | None) -> list[str]:
    if not options:
        raise ValueError(
            "pass keys, or the stream's options to read them from the capture instance's "
            "unique index"
        )
    from .client import make_client

    with closing(make_client(options)) as client:
        keys = client.source_table(capture_instance).keys
    if not keys:
        raise ValueError(f"{capture_instance} has no unique index to key rows by: pass keys")
    return keys


def _record(
    spark: SparkSessionLike,
    control_table: str,
    target: str,
    lsn: str | None,
    snapshot: str | None,
    open_lsn: str | None = None,
    wave: int | None = None,
) -> None:
    src = spark.createDataFrame(
        [(target, lsn, snapshot, open_lsn, wave)],
        "table_name STRING, applied_lsn STRING, snapshot_lsn STRING, open_snapshot_lsn STRING, "
        "snapshot_wave INT",
    )
    names = ("applied_lsn", "snapshot_lsn", "open_snapshot_lsn", "snapshot_wave")
    values: dict[str, str | Column] = {n: f"s.{n}" for n in names}
    retrying(  # trackers and other silver jobs MERGE into it too
        lambda: (
            delta_table(spark, control_table)
            .alias("t")
            .merge(src.alias("s"), "t.table_name = s.table_name")
            .whenMatchedUpdate(set=values)
            .whenNotMatchedInsert(values={"table_name": "s.table_name", **values})
            .execute()
        )
    )


@dataclass(frozen=True, slots=True)
class _Chunk:
    """A chunk a 'snapshot_chunk' facts row announces."""

    wave: int
    lo: Any
    hi: Any
    """Its key range [lo, hi), JSON bounds; None: open."""
    stamp: str
    """The LSN it was read under."""


def _chunks(facts: DataFrame, snapshot: str) -> dict[int, _Chunk]:
    """The chunks of ``snapshot`` its 'snapshot_chunk' facts rows announce, by index. They
    are written after the chunks' bronze rows."""
    rows = (
        events.where_chunks(facts, snapshot).select("detail", "min_lsn").collect()
    )  # ponytail: every chunk of the snapshot on each call; filter by wave if it shows up
    chunks: dict[int, _Chunk] = {}
    for row in rows:
        d: SnapshotChunkDetail = json.loads(row["detail"])
        chunks[int(d["chunk"])] = _Chunk(int(d["wave"]), d.get("lo"), d.get("hi"), row["min_lsn"])
    return chunks


def _by_key(df: DataFrame, other: DataFrame, keys: Sequence[str], how: str) -> DataFrame:
    """``df``'s rows whose key is (``left_semi``) or is not (``left_anti``) in ``other``;
    <=>, as a unique index admits one NULL key."""
    from pyspark.sql import functions as F

    a, b = df.alias("a"), other.alias("b")
    return a.join(b, [F.col(f"a.{_q(k)}").eqNullSafe(F.col(f"b.{_q(k)}")) for k in keys], how)


def _bound(v: Any, key_type: DataType, lower: bool) -> int | date | datetime | None:
    """A chunk bound of the facts as a value of ``key_type`` that puts each Spark key on the
    side SQL Server put it, or past the keys it cannot; None: open. A datetime2(7) bound has
    100 ns digits, a key in Spark only microseconds (drivers truncate them): the keys of a
    bound's microsecond fall on either side when it has digits below it. A ``lower`` bound
    then moves up past them; an upper one, truncated, already excludes them (``<``)."""
    if v is None:
        return None
    from pyspark.sql.types import DateType, IntegralType

    if isinstance(key_type, IntegralType):  # bound as BIGINT
        v = int(v)
        return None if v > 2**63 - 1 else v  # the plan's MAX + 1 past BIGINT: no key above
    if isinstance(key_type, DateType):
        return date.fromisoformat(str(v)[:10])
    whole, _, digits = str(v).replace(" ", "T").partition(".")
    t = datetime.fromisoformat(whole) + timedelta(microseconds=int(digits[:6].ljust(6, "0")))
    return t + timedelta(microseconds=1) if lower and digits[6:].strip("0") else t


def _range_key(keys: Sequence[str], cut: object) -> bool:
    """Whether chunk ranges may delete silver's keys: bounds are cut on the snapshot's key
    columns (``cut``, from its 'snapshot_open' row), so only when silver's key is that one
    column (ADR 0028)."""
    return len(keys) == 1 and cut == keys


def _absent(
    spark: SparkSessionLike,
    target: str,
    key: str,
    key_type: DataType,
    chunks: dict[int, _Chunk],
    held: DataFrame,
) -> DataFrame | None:
    """Synthetic deletes at each chunk's stamp L of the silver keys in its range [lo, hi)
    that it does not hold (``held``: its rows' keys), when their image is older than L. The
    chunk saw every commit up to L, so those keys were gone by then. Only for an integer,
    date or timestamp key, which Spark orders as SQL Server does; None for other keys. A
    NULL key is in no range here unless the range is the whole table (lo and hi open)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DateType,
        IntegralType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
    )

    if not isinstance(key_type, (IntegralType, DateType, TimestampNTZType)):
        return None
    ranges: list[tuple[Any, Any, str]] = []
    for c in chunks.values():
        try:
            ranges.append((_bound(c.lo, key_type, True), _bound(c.hi, key_type, False), c.stamp))
        except OverflowError:  # a lower bound past the last microsecond: no key surely inside
            continue
    if not ranges:
        return None
    bound = LongType() if isinstance(key_type, IntegralType) else key_type  # MAX + 1 fits
    schema = StructType(
        [StructField("lo", bound), StructField("hi", bound), StructField("l", StringType())]
    )
    r = F.broadcast(spark.createDataFrame(ranges, schema)).alias("r")
    # the ranges' span as literals, which Delta's file stats prune silver by before the join
    # (inequalities only: a nested loop). A NULL key fails either: with every range closed on
    # that side, it is in none.
    k = F.col(_q(key))
    near = F.col("_start_lsn") < max(stamp for _, _, stamp in ranges)
    if all(lo is not None for lo, _, _ in ranges):
        near &= k >= F.lit(str(min(lo for lo, _, _ in ranges))).cast(bound)
    if all(hi is not None for _, hi, _ in ranges):
        near &= k < F.lit(str(max(hi for _, hi, _ in ranges))).cast(bound)
    t = delta_table(spark, target).toDF().where(near).alias("t")
    k = F.col(f"t.{_q(key)}")
    inside = (
        (F.col("r.lo").isNull() | (k >= F.col("r.lo")))
        & (F.col("r.hi").isNull() | (k < F.col("r.hi")))
        & (F.col("t._start_lsn") < F.col("r.l"))
    )
    gone = t.join(r, inside).select(
        k.alias(key), F.col("r.l").alias("_start_lsn"), F.lit(1).alias("_operation")
    )
    return _by_key(gone, held, [key], "left_anti")


def apply_changes(
    spark: SparkSessionLike,
    bronze: str,
    target: str,
    *,
    capture_instance: str | None = None,
    keys: Sequence[str] | None = None,
    control_table: str,
    facts_table: str | None = None,
    options: SourceOptions | Mapping[str, Any] | None = None,
    granularity: Granularity = "hour",
) -> ApplyResult:
    """Bring ``target`` up to the capture instance's changes in ``bronze``, a table fed by
    that capture instance alone (and the newer ones of its table it switched to). Until the
    stream creates ``bronze``, it does nothing but log a warning that names it.

    ``capture_instance``: the stream's; without it, ``captureInstance`` of ``options``.
    ``keys``: the source's key columns; without them, read from the capture instance's
    unique index through ``options`` (the stream's), which also name the table's other
    capture instances: needed once bronze holds a newer one's rows. ``control_table`` keeps
    the position and gets the verdict; ``bronze``'s own verdict must be under the same name
    or path.
    ``facts_table``: the stream's, needed to see the re-snapshot of an emptied table and
    any chunked snapshot (a call fails on chunk rows without it, or when its rows name
    bronze otherwise). Without it, the verdict is not advanced.
    The parameters after ``target`` are keyword-only.
    Returns an ``ApplyResult``, ``{"rebuilt", "applied_lsn", "finalized_until",
    "bronze_found"}``;
    ``bronze_found`` is False when ``bronze`` does not exist (nothing was applied).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    finalization._check_granularity(granularity)  # before any write, not at the verdict
    from .pipeline import _opt

    # given both, the argument wins: they may name two instances of one table (ADR 0023)
    capture_instance = capture_instance or (_opt(options, "captureInstance") if options else None)
    if not capture_instance:
        raise ValueError("pass capture_instance, or the stream's options with captureInstance")
    if not facts_table:
        _warn_once(
            "no facts",
            target,
            "apply_changes into %s without facts_table: its finalized_until is not advanced "
            "(pass the stream's facts table)",
            target,
        )
    keys = list(keys) if keys else _source_keys(capture_instance, options)
    finalization.ensure_control_table(spark, control_table)
    control = delta_table(spark, control_table).toDF()
    # Read before bronze: bronze commits its rows before its verdict, and a snapshot's rows
    # before its event rows, so the bronze version read below holds what they point to.
    verdict = (
        control.where(F.col("table_name") == bronze).select("end_lsn", "end_commit_ts").first()
    )
    row = control.where(F.col("table_name") == target).select(
        "applied_lsn", "snapshot_lsn", "finalized_until", "open_snapshot_lsn", "snapshot_wave"
    )
    applied, rebuilt_from, finalized, open_from, wave_from = row.first() or (None,) * 5
    if not exists(spark, bronze):  # the stream has written no batch yet, or a wrong name
        _log.warning(
            "bronze %s does not exist: nothing applied to %s. Expected until the stream writes "
            "its first batch; otherwise check the name (a typo, or bronze and target swapped)",
            bronze,
            target,
        )
        return {
            "rebuilt": False,
            "applied_lsn": applied,
            "finalized_until": finalized,
            "bronze_found": False,
        }
    points: list[str | None] = []
    every: DataFrame | None = None
    facts: DataFrame | None = None
    opened: Row | None = None
    chunks: dict[int, _Chunk] = {}
    if facts_table and exists(spark, facts_table):
        every = delta_table(spark, facts_table).toDF()
        facts = every.where(F.col("target") == bronze)
        snapshots = F.col("event").isin(*events.SNAPSHOTS)  # not the source's changes
        counted = facts.agg(F.max(F.when(snapshots, F.col("max_lsn"))), F.count(F.lit(1))).first()
        assert counted is not None  # a global aggregate always returns one row
        newest, found = counted
        points.append(newest)
        if not found and ("facts", bronze) not in _warned:  # e.g. written by snapshot() alone
            _warn_once(
                "facts",
                bronze,
                "%s has no row for target %r: if the stream writes it under another name, "
                "snapshots are missed. Its targets: %s",
                facts_table,
                bronze,
                sorted(r[0] for r in every.select("target").distinct().collect() if r[0]),
            )
        if "detail" in facts.columns:  # facts migration 6
            # a full snapshot's open row only locks the mode: its rows come whole
            opened = (
                events.where_chunked_open(facts)
                .orderBy(F.col("max_lsn").desc())
                .select("max_lsn", "detail")
                .first()
            )
        if opened and (points[0] is None or opened["max_lsn"] > points[0]):
            chunks = _chunks(facts, opened["max_lsn"])
    latest_commit = delta_table(spark, bronze).history(1).first()
    assert latest_commit is not None  # an existing table has a version
    version = int(latest_commit["version"])
    pinned = spark.sql(f"SELECT * FROM {table_ref(bronze)} VERSION AS OF {version}")
    # ignoring case, as SQL Server resolves the names
    instances = [capture_instance.lower()]
    if options:
        from .pipeline import _instances

        instances = _instances(options, capture_instance)
    ours = F.lower("_capture_instance").isin(instances)
    columns = set(pinned.columns)
    snap = F.coalesce("_snapshot", "_start_lsn") if "_snapshot" in columns else F.col("_start_lsn")
    chunked = F.col("_chunk").isNotNull() if "_chunk" in columns else F.lit(False)
    op = F.col("_operation")
    snapshot_rows = ours & (op == 0)
    if rebuilt_from:
        # A whole snapshot's rows carry its S in _start_lsn too, and rebuilt_from stays among
        # the points (bronze and the facts are append-only): no older snapshot can change the
        # result, and Delta's stats on _start_lsn skip their files.
        snapshot_rows &= F.col("_start_lsn") >= rebuilt_from
    whole, chunk_rows = (
        pinned.where(snapshot_rows)
        .agg(F.max(F.when(~chunked, snap)), F.count(F.when(chunked, 1)))
        .collect()[0]  # a global aggregation: one row
    )
    if chunk_rows and facts is None:
        raise ValueError(
            f"{bronze} holds rows of a chunked snapshot: pass facts_table (the stream's), "
            "whose rows say which chunks arrived and when the snapshot is complete"
        )
    if chunk_rows and opened is None and every is not None:
        named = every.where(F.col("event") == events.SNAPSHOT_OPEN).select("target").distinct()
        raise ValueError(
            f"{bronze} holds rows of a chunked snapshot, but {facts_table} has no "
            f"'snapshot_open' row for target = {bronze!r}: is it named otherwise in to_delta? "
            f"Targets with one: {sorted(r[0] for r in named.collect() if r[0])}"
        )
    points.append(whole)
    snapshot = max((p for p in points if p), default=None)
    s_open = opened["max_lsn"] if opened else None
    is_open = s_open is not None and (snapshot is None or s_open > snapshot)
    # against the snapshot last rebuilt from, not applied_lsn: a bootstrap added to a stream
    # on a quiet database is stamped with the LSN silver has already applied
    rebuild = (applied is None and open_from is None) or (
        snapshot is not None and (rebuilt_from is None or snapshot > rebuilt_from)
    )
    change = op != 0
    if rebuild and snapshot:  # the snapshot's rows, whatever their stamps, and the changes after
        base = (change & (F.col("_start_lsn") > snapshot)) | ((op == 0) & (snap == snapshot))
    elif rebuild or applied is None:
        base = change
    else:
        base = change & (F.col("_start_lsn") > applied)
    after = wave_from if not rebuild and open_from == s_open and wave_from is not None else -1
    new = {i: c for i, c in chunks.items() if c.wave > after} if is_open else {}
    rows = pinned.where(base)
    if new:  # the open snapshot's waves that arrived since the last call
        waves = pinned.where(
            (op == 0) & (F.col("_snapshot") == s_open) & F.col("_chunk").isin(*new)
        )
        rows = rows.unionByName(waves)
    # a row of another instance would be skipped for good: a switch silver was not told about.
    # One pass for it and the newest change (the waves are operation 0: no change).
    other, top = rows.agg(
        F.max(F.when(~ours, F.col("_capture_instance"))),
        F.max(F.when(ours & change, F.col("_start_lsn"))),
    ).collect()[0]
    if other is not None:
        raise ValueError(
            f"{bronze} holds rows of capture instance {other!r}, which is not "
            f"{capture_instance!r} or another capture instance of its table"
            + ("" if options else ": pass options (the stream's) to read them from SQL Server")
        )
    rows = rows.where(ours)

    captured = [f for f in pinned.schema if f.name not in _META]
    names = [f.name for f in captured]
    missing = [k for k in keys if k not in names]
    if missing:
        raise ValueError(f"keys {missing} are not captured columns of {bronze}: {names}")
    migrations.ensure(
        spark,
        target,
        "silver",
        [(f.name, f.dataType, None) for f in captured] + SILVER_COLUMNS,
        SILVER_COMMENT,
    )
    # captured by a newer capture instance: bronze gained it, so does silver
    migrations.add_columns(spark, target, [(f.name, f.dataType, None) for f in captured])

    merged = top is not None or (rebuild and snapshot is not None) or bool(new)
    if merged:
        if new:
            held = waves.select(*[F.col(_q(k)) for k in keys])
            # every later change of the chunks' keys, applied or not: a chunk row never
            # outranks a delete the stream committed after its stamp
            rows = rows.unionByName(
                _by_key(
                    pinned.where(ours & change & (F.col("_start_lsn") > F.lit(s_open))),
                    held,
                    keys,
                    "left_semi",
                )
            )
            # bounds are cut on the snapshot's key columns: by range only on those (ADR 0028)
            cut = json.loads(opened["detail"] or "{}").get("keys") if opened else None
            if _range_key(keys, cut):
                key_type = pinned.schema[keys[0]].dataType
                gone = _absent(spark, target, keys[0], key_type, new, held)
                if gone is not None:
                    rows = rows.unionByName(gone, allowMissingColumns=True)
        columns_out = [*names, "_start_lsn", "_commit_ts"]
        order = [F.col("_start_lsn").desc()]
        if "_command_id" in columns:
            order.append(F.col("_command_id").desc_nulls_last())
        else:
            _warn_once(
                "no _command_id",
                bronze,
                "%s has no _command_id (includeCommandId=false): the changes of one "
                "transaction are ordered by _seqval (__$seqval)",
                bronze,
            )
        order += [F.col("_seqval").desc_nulls_last(), F.col("_operation").desc()]
        last = Window.partitionBy(*[F.col(_q(k)) for k in keys]).orderBy(*order)
        latest = (
            rows.withColumn("_mssql_cdc_rank", F.row_number().over(last))
            .where(F.col("_mssql_cdc_rank") == 1)
            .select(*[F.col(_q(c)) for c in columns_out], "_operation")
        )
        values: dict[str, str | Column] = {_q(c): f"s.{_q(c)}" for c in columns_out}
        newer = "s._start_lsn > t._start_lsn"  # never an older image over a newer one
        gone_op = "(s._operation IN (1, 3))"  # a 3 outranked by no 4 of its key: the key moved
        merge = (
            delta_table(spark, target)
            .alias("t")
            # <=>: a unique index admits one NULL key
            .merge(latest.alias("s"), " AND ".join(f"t.{_q(k)} <=> s.{_q(k)}" for k in keys))
            .whenMatchedDelete(condition=f"{gone_op} AND {newer}")
            .whenMatchedUpdate(condition=f"NOT {gone_op} AND {newer}", set=values)
            .whenNotMatchedInsert(condition=f"NOT {gone_op}", values=values)
        )
        if rebuild:
            merge = merge.whenNotMatchedBySourceDelete()
        merge.execute()
    position = max((p for p in (applied, snapshot, top) if p), default=None)
    rebuilt = snapshot if rebuild else rebuilt_from
    progress: tuple[str | None, int | None] = (None, None)
    if is_open:
        wave = max([after, *(c.wave for c in new.values())])
        progress = (s_open, wave if wave >= 0 else None)
    if (position, rebuilt, *progress) != (applied, rebuilt_from, open_from, wave_from):
        _record(spark, control_table, target, position, rebuilt, *progress)
    offset = (
        {
            "lsn": verdict["end_lsn"],
            "commit_ts": verdict["end_commit_ts"].isoformat(timespec="milliseconds"),
        }
        if verdict and verdict["end_commit_ts"]
        else None
    )
    return {
        "rebuilt": rebuild and merged,
        "applied_lsn": position,
        # held while a snapshot is open: silver lacks its keys, or still has stale ones; and
        # without the facts, which alone tell that one is open
        "finalized_until": finalized
        if is_open or not facts_table
        else finalization.advance(spark, control_table, target, offset, granularity=granularity),
        "bronze_found": True,
    }
