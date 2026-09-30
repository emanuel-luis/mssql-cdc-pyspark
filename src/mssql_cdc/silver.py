"""Apply the bronze change log to a current-state ("silver") Delta table (ADR 0019).

    from mssql_cdc import apply_changes
    apply_changes(spark, "bronze.orders", "silver.orders", "dbo_orders", ["order_id"],
                  control_table="ops.table_finalization", facts_table="ops.ingestion_facts")

Each call applies what the capture instance's rows in ``bronze`` hold beyond the last one:
the latest image per key by ``(_start_lsn, _command_id, _seqval, _operation)``; operation
3 (the row before an update) is ignored, 1 deletes the key, 0 (snapshot), 2 and 4 upsert it.
Run it after the stream, in the same job or another; one job per silver table.

* Position: ``applied_lsn`` in the control table, the highest ``_start_lsn`` applied,
  recorded after the MERGE. A crash in between leaves it behind, and applying from behind
  changes nothing: each key takes the latest image of a range that reaches the head of
  bronze, and a row only takes an image newer than its own ``_start_lsn``.
* Re-snapshot: when bronze holds a snapshot newer than ``snapshot_lsn``, the one silver was
  last rebuilt from (the highest ``_start_lsn`` of its operation-0 rows, or ``max_lsn`` of
  the facts' event rows for ``bronze``: an emptied table's snapshot has only its event),
  silver is rebuilt from it, so keys absent from the snapshot and the changes after it are
  deleted. The events carry no capture instance, so ``bronze`` holds one capture instance,
  as its verdict already requires.
* Verdict: silver's ``finalized_until`` advances to the bronze verdict read before bronze
  itself. Bronze commits its rows before its verdict (ADR 0005), so the rows applied hold
  every commit up to it: silver never claims more than it has applied.
"""

from __future__ import annotations

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


def _record(spark, control_table: str, target: str, lsn: str, snapshot: str | None) -> None:
    src = spark.createDataFrame(
        [(target, lsn, snapshot)], "table_name STRING, applied_lsn STRING, snapshot_lsn STRING"
    )
    values = {"applied_lsn": "s.applied_lsn", "snapshot_lsn": "s.snapshot_lsn"}
    (
        delta_table(spark, control_table)
        .alias("t")
        .merge(src.alias("s"), "t.table_name = s.table_name")
        .whenMatchedUpdate(set=values)
        .whenNotMatchedInsert(values={"table_name": "s.table_name", **values})
        .execute()
    )


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
    that capture instance alone. Until the stream creates ``bronze``, it does nothing.

    ``keys``: the source's key columns; without them, read from the capture instance's
    unique index through ``options`` (the stream's). ``control_table`` keeps the position
    and gets the verdict; ``bronze``'s own verdict must be under the same name or path.
    ``facts_table``: the stream's, needed to see the re-snapshot of an emptied table.
    Returns ``{"rebuilt", "applied_lsn", "finalized_until"}``.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    keys = list(keys) if keys else _source_keys(capture_instance, options)
    finalization.ensure_control_table(spark, control_table)
    control = delta_table(spark, control_table).toDF()
    # Read before bronze: bronze commits its rows before its verdict, and a snapshot's rows
    # before its event row, so the bronze version read below holds what both point to.
    verdict = (
        control.where(F.col("table_name") == bronze).select("end_lsn", "end_commit_ts").first()
    )
    row = control.where(F.col("table_name") == target).select(
        "applied_lsn", "snapshot_lsn", "finalized_until"
    )
    applied, rebuilt_from, finalized = row.first() or (None, None, None)
    if not exists(spark, bronze):  # the stream has written no batch yet
        return {"rebuilt": False, "applied_lsn": applied, "finalized_until": finalized}
    points = []
    if facts_table and exists(spark, facts_table):
        facts = delta_table(spark, facts_table).toDF()
        points.append(
            _one(
                facts.where(F.col("event").isNotNull() & (F.col("target") == bronze)).agg(
                    F.max("max_lsn")
                )
            )
        )
    version = int(delta_table(spark, bronze).history(1).first()["version"])
    changes = spark.sql(f"SELECT * FROM {table_ref(bronze)} VERSION AS OF {version}").where(
        F.lower("_capture_instance") == capture_instance.lower()  # as SQL Server resolves it
    )
    # ponytail: a scan for operation 0 on every call (file stats skip change-only files);
    # keep the newest snapshot LSN in the control table if it shows up.
    points.append(_one(changes.where(F.col("_operation") == 0).agg(F.max("_start_lsn"))))
    snapshot = max((p for p in points if p), default=None)
    # against the snapshot last rebuilt from, not applied_lsn: a bootstrap added to a stream
    # on a quiet database is stamped with the LSN silver has already applied
    rebuild = applied is None or (
        snapshot is not None and (rebuilt_from is None or snapshot > rebuilt_from)
    )
    if not rebuild:
        changes = changes.where(F.col("_start_lsn") > F.lit(applied))
    elif snapshot:
        changes = changes.where(F.col("_start_lsn") >= snapshot)

    captured = [f for f in changes.schema if f.name not in BRONZE_COLUMN_COMMENTS]
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

    top = _one(changes.agg(F.max("_start_lsn")))
    merged = top is not None or (rebuild and snapshot is not None)
    if merged:
        columns = [*names, "_start_lsn", "_commit_ts"]
        last = Window.partitionBy(*[F.col(_q(k)) for k in keys]).orderBy(
            F.col("_start_lsn").desc(),
            F.col("_command_id").desc_nulls_last(),
            F.col("_seqval").desc_nulls_last(),
            F.col("_operation").desc(),
        )
        latest = (
            changes.where(F.col("_operation") != 3)
            .withColumn("_mssql_cdc_rank", F.row_number().over(last))
            .where(F.col("_mssql_cdc_rank") == 1)
            .select(*[F.col(_q(c)) for c in columns], "_operation")
        )
        values = {_q(c): f"s.{_q(c)}" for c in columns}
        newer = "s._start_lsn > t._start_lsn"  # never an older image over a newer one
        merge = (
            delta_table(spark, target)
            .alias("t")
            # <=>: a unique index admits one NULL key
            .merge(latest.alias("s"), " AND ".join(f"t.{_q(k)} <=> s.{_q(k)}" for k in keys))
            .whenMatchedDelete(condition=f"s._operation = 1 AND {newer}")
            .whenMatchedUpdate(condition=f"s._operation != 1 AND {newer}", set=values)
            .whenNotMatchedInsert(condition="s._operation != 1", values=values)
        )
        if rebuild:
            merge = merge.whenNotMatchedBySourceDelete()
        merge.execute()
    position = max((p for p in (applied, snapshot, top) if p), default=None)
    rebuilt = snapshot if rebuild else rebuilt_from
    if position and (position, rebuilt) != (applied, rebuilt_from):
        _record(spark, control_table, target, position, rebuilt)
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
        "finalized_until": finalization.advance(spark, control_table, target, offset, granularity),
    }
