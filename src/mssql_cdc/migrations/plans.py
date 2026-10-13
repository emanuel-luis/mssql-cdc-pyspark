"""Migrations for a bronze table's snapshot plans table (events.PLANS_COLUMNS), from 0.7.0.

Append only; see ``mssql_cdc.migrations``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import Migration, set_comments

if TYPE_CHECKING:
    from ..types import SparkSessionLike


# Migration 1 (2026-10-13): the 'snapshot_open' detail of a chunked snapshot opened after a
# capture instance switch has switch_batch_id (ADR 0023 Amendment 7). Comment only.
def _switch_comment(spark: SparkSessionLike, table: str) -> None:
    from ..events import PLANS_COLUMNS

    comment = next(c for n, _, c in PLANS_COLUMNS if n == "detail")
    assert comment is not None  # every plans column has one
    set_comments(spark, table, {"detail": comment})


MIGRATIONS: list[Migration] = [
    Migration("snapshots after a capture instance switch", _switch_comment),
]
