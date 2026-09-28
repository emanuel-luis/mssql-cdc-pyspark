"""Completeness signal ("partition finalization") for CDC-fed tables.

Two layers, in the spirit of Pinterest's partition finalization:

* **Facts**, per micro-batch: what was written (row counts, LSN and commit-time
  ranges). See :mod:`mssql_cdc.sink`.
* **Verdict**, per table: ``finalized_until``. Every period strictly before it is
  complete in the target table and will not receive more source commits.

Why the verdict is sound for SQL Server CDC: a batch's end LSN never exceeds
``sys.fn_cdc_get_max_lsn()``, the last LSN the capture process has processed, and
the capture process writes change rows in commit order. So once a batch ending at
LSN ``L`` is committed to the target, every source transaction committed at or
before the commit time of ``L`` is in the target. Transactions sharing that exact
commit time may still be pending, which is why the period *containing* it is not
finalized; truncating to the period start handles that.

Ordering rather than atomicity keeps it safe: write data first, advance the verdict
after, and never let it move backwards. If the job dies in between, the verdict
lags (consumers wait a little longer); it can never run ahead of the data.
"""

from __future__ import annotations

import json
from datetime import datetime

_GRANULARITIES = ("minute", "hour", "day")


def truncate(ts: datetime, granularity: str = "hour") -> datetime:
    g = granularity.lower()
    if g not in _GRANULARITIES:
        raise ValueError(f"granularity must be one of {_GRANULARITIES}")
    ts = ts.replace(second=0, microsecond=0)
    if g in ("hour", "day"):
        ts = ts.replace(minute=0)
    if g == "day":
        ts = ts.replace(hour=0)
    return ts


def candidate(end_offset: dict | None, granularity: str = "hour") -> datetime | None:
    """``finalized_until`` implied by a committed batch's end offset."""
    if not end_offset or not end_offset.get("commit_ts"):
        return None
    return truncate(datetime.fromisoformat(end_offset["commit_ts"]), granularity)


def end_offset_from_progress(progress, source_index: int = 0) -> dict | None:
    """Extract the end offset from a StreamingQueryProgress (object or dict)."""
    if progress is None:
        return None
    data = json.loads(progress.json) if hasattr(progress, "json") else progress
    sources = data.get("sources") or []
    if len(sources) <= source_index:
        return None
    end = sources[source_index].get("endOffset")
    if isinstance(end, str):
        end = json.loads(end)
    return end


def table_ref(name_or_path: str) -> str:
    """A table name, or ``delta.`path``` when given a filesystem/object-store path."""
    if "/" in name_or_path or ":" in name_or_path:
        return f"delta.`{name_or_path}`"
    return name_or_path


def ensure_control_table(spark, control_table: str) -> None:
    ref = table_ref(control_table)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {ref} ("
        "table_name STRING, finalized_until TIMESTAMP_NTZ, end_lsn STRING, "
        "end_commit_ts TIMESTAMP_NTZ, updated_at TIMESTAMP) USING delta"
    )


def advance(spark, control_table: str, table_name: str, end_offset: dict | None,
            granularity: str = "hour") -> datetime | None:
    """Monotonically advance ``finalized_until`` for ``table_name``.

    Call only after the batch that produced ``end_offset`` is committed.
    """
    cand = candidate(end_offset, granularity)
    ensure_control_table(spark, control_table)
    if cand is not None:
        # No parameter markers: on a Delta-enabled session (delta-spark 4.4, Spark 4.2)
        # they stay unbound (UNBOUND_SQL_PARAMETER). The source row is a DataFrame.
        src = spark.createDataFrame(
            [(table_name, cand, end_offset["lsn"], datetime.fromisoformat(end_offset["commit_ts"]))],
            "table_name STRING, cand TIMESTAMP_NTZ, end_lsn STRING, end_ts TIMESTAMP_NTZ",
        )
        spark.sql(
            f"""
            MERGE INTO {table_ref(control_table)} t
            USING {{src}} s
            ON t.table_name = s.table_name
            WHEN MATCHED AND s.cand > t.finalized_until THEN UPDATE SET
                 finalized_until = s.cand, end_lsn = s.end_lsn,
                 end_commit_ts = s.end_ts, updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
                 (table_name, finalized_until, end_lsn, end_commit_ts, updated_at)
                 VALUES (s.table_name, s.cand, s.end_lsn, s.end_ts, current_timestamp())
            """,
            src=src,
        )
    return finalized_until(spark, control_table, table_name)


def finalized_until(spark, control_table: str, table_name: str) -> datetime | None:
    df = spark.sql(f"SELECT table_name, finalized_until FROM {table_ref(control_table)}")
    rows = df.where(df.table_name == table_name).select("finalized_until").collect()  # see advance()
    return rows[0][0] if rows else None


def is_final(spark, control_table: str, table_name: str, period_end: datetime) -> bool:
    """True when the period ending at ``period_end`` (exclusive) is complete."""
    fu = finalized_until(spark, control_table, table_name)
    return fu is not None and period_end <= fu
