"""Apply the bronze change log to a current-state ("silver") Delta table (ADR 0019).

    from mssql_cdc import apply_changes
    apply_changes(spark, "bronze.orders", "silver.orders", "dbo_orders", ["order_id"],
                  control_table="ops.table_finalization", facts_table="ops.ingestion_facts")

Each call applies what the capture instance's rows in ``bronze`` hold beyond the last one:
the latest image per key by ``(_start_lsn, _command_id, _seqval, _operation)``; operation
1 deletes the key, 0 (snapshot), 2 and 4 upsert it, and 3 (the row before an update)
deletes its own key: the 4 of an update that keeps the key outranks it, so a 3 is the latest
row only when the update moved the row to another key.
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
  table, as its verdict already requires.
* Chunked snapshots (``_chunk`` set) arrive in waves after their 'snapshot_open' facts row,
  stamped at or above S, and are complete at their completion row: ``facts_table`` is
  required. While a bootstrap is open (a 'snapshot_open' newer than every complete
  snapshot), each call applies the waves its 'snapshot_chunk' rows announce, tracked by
  ``open_snapshot_lsn`` and ``snapshot_wave`` (the chunks land below ``applied_lsn``), with
  every change after S of their keys: a chunk row never brings back a key deleted since.
  An open re-snapshot only keeps applying changes. Either way, the rebuild comes at
  completion, and deletes the keys absent from the snapshot and the changes after it.
* Verdict: silver's ``finalized_until`` advances to the bronze verdict read before bronze
  itself. Bronze commits its rows before its verdict (ADR 0005), so the rows applied hold
  every commit up to it: silver never claims more than it has applied. While a snapshot is
  open, silver lacks keys or holds stale ones: its verdict is held until the rebuild. Without
  ``facts_table`` it is never advanced: a chunked snapshot opens, and an emptied table's
  re-snapshot happens, in the facts alone, before bronze holds any row of it.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from contextlib import closing

from . import finalization, migrations
from .sink import BRONZE_COLUMN_COMMENTS
from .tables import delta_table, exists, table_ref

SILVER_COMMENT = (
    "Current state of a SQL Server table, one row per key, built from the bronze change log "
    "by mssql-cdc-pyspark's apply_changes. Deleted rows are removed. How far it is applied is "
    "applied_lsn in the control table; finalized_until there says which periods are complete."
)
SILVER_COLUMNS = [
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


def _one(df):
    row = df.first()
    return row[0] if row else None


def _source_keys(capture_instance: str, options: dict | None) -> list[str]:
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
    spark,
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
    values = {n: f"s.{n}" for n in names}
    (
        delta_table(spark, control_table)
        .alias("t")
        .merge(src.alias("s"), "t.table_name = s.table_name")
        .whenMatchedUpdate(set=values)
        .whenNotMatchedInsert(values={"table_name": "s.table_name", **values})
        .execute()
    )


def _resnapshot(opened) -> bool:
    """A 'snapshot_open' facts row of a re-snapshot (ADR 0018), not of a bootstrap: the mode
    the stream opened it in, which its completion row is named after."""
    return json.loads(opened["detail"] or "{}").get("mode") == "resnapshot"


def _chunks(facts, snapshot: str) -> dict[int, int]:
    """The wave of each chunk of ``snapshot`` its 'snapshot_chunk' facts rows announce, by
    index. They are written after the chunks' bronze rows."""
    from pyspark.sql import functions as F

    rows = (
        facts.where(
            (F.col("event") == "snapshot_chunk")
            & (F.get_json_object("detail", "$.snapshot") == snapshot)
        )
        .select("detail")
        .collect()
    )  # ponytail: every chunk of the snapshot on each call; filter by wave if it shows up
    return {int(d["chunk"]): int(d["wave"]) for d in (json.loads(r["detail"]) for r in rows)}


def _by_key(df, other, keys: Sequence[str], how: str):
    """``df``'s rows whose key is (``left_semi``) or is not (``left_anti``) in ``other``;
    <=>, as a unique index admits one NULL key."""
    from pyspark.sql import functions as F

    a, b = df.alias("a"), other.alias("b")
    return a.join(b, [F.col(f"a.{_q(k)}").eqNullSafe(F.col(f"b.{_q(k)}")) for k in keys], how)


def apply_changes(
    spark,
    bronze: str,
    target: str,
    capture_instance: str,
    keys: Sequence[str] | None = None,
    *,
    control_table: str,
    facts_table: str | None = None,
    options: dict | None = None,
    granularity: str = "hour",
) -> dict:
    """Bring ``target`` up to the capture instance's changes in ``bronze``, a table fed by
    that capture instance alone (and the newer ones of its table it switched to). Until the
    stream creates ``bronze``, it does nothing.

    ``keys``: the source's key columns; without them, read from the capture instance's
    unique index through ``options`` (the stream's), which also name the table's other
    capture instances: needed once bronze holds a newer one's rows. ``control_table`` keeps
    the position and gets the verdict; ``bronze``'s own verdict must be under the same name
    or path.
    ``facts_table``: the stream's, needed to see the re-snapshot of an emptied table and
    any chunked snapshot (a call fails on chunk rows without it). Without it, the verdict is
    not advanced.
    Returns ``{"rebuilt", "applied_lsn", "finalized_until"}``.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

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
    if not exists(spark, bronze):  # the stream has written no batch yet
        return {"rebuilt": False, "applied_lsn": applied, "finalized_until": finalized}
    points: list = []
    facts, opened, chunks = None, None, {}
    if facts_table and exists(spark, facts_table):
        facts = delta_table(spark, facts_table).toDF().where(F.col("target") == bronze)
        snapshots = F.col("event").isin("bootstrap", "resnapshot")  # not the source's changes
        points.append(_one(facts.where(snapshots).agg(F.max("max_lsn"))))
        if "detail" in facts.columns:  # facts migration 6
            opened = (
                facts.where(F.col("event") == "snapshot_open")
                .orderBy(F.col("max_lsn").desc())
                .select("max_lsn", "detail")
                .first()
            )
        if opened and (points[0] is None or opened["max_lsn"] > points[0]):
            chunks = _chunks(facts, opened["max_lsn"])
    version = int(delta_table(spark, bronze).history(1).first()["version"])
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
    # ponytail: a scan for operation 0 on every call (file stats skip change-only files);
    # keep the newest snapshot LSN in the control table if it shows up.
    whole, chunk_rows = (
        pinned.where(ours & (op == 0))
        .agg(F.max(F.when(~chunked, snap)), F.count(F.when(chunked, 1)))
        .first()
    )
    if chunk_rows and facts is None:
        raise ValueError(
            f"{bronze} holds rows of a chunked snapshot: pass facts_table (the stream's), "
            "whose rows say which chunks arrived and when the snapshot is complete"
        )
    points.append(whole)
    snapshot = max((p for p in points if p), default=None)
    s_open = opened["max_lsn"] if opened else None
    is_open = s_open is not None and (snapshot is None or s_open > snapshot)
    bootstrap_open = is_open and not _resnapshot(opened)
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
    new = {c: w for c, w in chunks.items() if w > after} if bootstrap_open else {}
    rows = pinned.where(base)
    if new:  # the open bootstrap's waves that arrived since the last call
        waves = pinned.where(
            (op == 0) & (F.col("_snapshot") == s_open) & F.col("_chunk").isin(*new)
        )
        rows = rows.unionByName(waves)
    # a row of another instance would be skipped for good: a switch silver was not told about
    other = _one(rows.where(~ours).select("_capture_instance"))
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
    have = set(delta_table(spark, target).toDF().columns)
    added = [(f.name, f.dataType, None) for f in captured if f.name not in have]
    if added:  # captured by a newer capture instance: bronze gained it, so does silver
        migrations.add_columns(spark, target, added)

    top = _one(rows.where(change).agg(F.max("_start_lsn")))
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
        columns_out = [*names, "_start_lsn", "_commit_ts"]
        last = Window.partitionBy(*[F.col(_q(k)) for k in keys]).orderBy(
            F.col("_start_lsn").desc(),
            F.col("_command_id").desc_nulls_last(),
            F.col("_seqval").desc_nulls_last(),
            F.col("_operation").desc(),
        )
        latest = (
            rows.withColumn("_mssql_cdc_rank", F.row_number().over(last))
            .where(F.col("_mssql_cdc_rank") == 1)
            .select(*[F.col(_q(c)) for c in columns_out], "_operation")
        )
        values = {_q(c): f"s.{_q(c)}" for c in columns_out}
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
    progress: tuple = (None, None)
    if bootstrap_open:
        wave = max([after, *new.values()])
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
        else finalization.advance(spark, control_table, target, offset, granularity),
    }
