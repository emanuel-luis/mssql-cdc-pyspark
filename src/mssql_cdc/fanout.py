"""Many tables: one ``to_delta`` stream per capture instance, started together (ADR 0027).

    from mssql_cdc import await_all, start_many
    queries = start_many(spark, options, ["dbo_orders", "dbo_customers"],
                         target="bronze.{ci}", app_id="{ci}-v1",
                         checkpoint="/Volumes/main/ops/ckpt/{ci}",
                         facts_table="ops.ingestion_facts", trigger={"availableNow": True})
    failed = await_all(queries)

Each stream is the one ``stream(spark, options).to_delta(...)`` starts for its capture
instance alone: its own checkpoint, app_id, generations and metrics directory. They share
the facts table, whose rows carry each stream's app_id and target. Spark runs every query
on its own thread, so one that fails stops alone.
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterable, Mapping
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from .pipeline import stream

if TYPE_CHECKING:
    from pyspark.sql.streaming.query import StreamingQuery

    from .source import SourceOptions
    from .types import SparkSessionLike, StreamingQueryLike


def start_many(
    spark: SparkSessionLike,
    options: SourceOptions | Mapping[str, Any],
    capture_instances: Iterable[str] | Mapping[str, SourceOptions | Mapping[str, Any]],
    *,
    target: str,
    app_id: str,
    checkpoint: str,
    facts_table: str | None = None,
    **to_delta_kwargs: Any,
) -> dict[str, StreamingQuery]:
    """Start one ``to_delta`` stream per capture instance; returns the queries by capture
    instance, in the order given.

    ``target``, ``app_id`` and ``checkpoint`` are templates in which ``{ci}`` is the
    capture instance, e.g. ``"bronze.{ci}"``, ``"{ci}-v1"`` and
    ``"/Volumes/main/ops/ckpt/{ci}"``. Each must contain it: every stream needs its own
    target, app_id and checkpoint. ``capture_instances`` is a list of names, or a mapping
    of name to options for that stream alone, which override ``options``
    (``numPartitions``, ``columns``, ``startingLsn``...). ``facts_table`` and the other
    keyword arguments (``trigger``, ``bootstrap``, ``on_data_loss``...) go to every
    ``to_delta``; each query is named as ``to_delta`` names it, after its sink id
    (``<app_id>.g<n>`` after a re-snapshot), as its facts rows are.

    The facts table is created, or migrated, once before the first start: writers that
    commit to a new Delta table at the same time conflict. The streams start in order,
    each after its own bootstrap or re-snapshot. When a start raises, the queries already
    started are stopped and the error is raised.
    """
    templates = {"target": target, "app_id": app_id, "checkpoint": checkpoint}
    for name, template in templates.items():
        if "{ci}" not in template:
            raise ValueError(f"{name}={template!r} must contain '{{ci}}': one per stream")
    if isinstance(capture_instances, str):
        capture_instances = [capture_instances]
    tables = (
        capture_instances
        if isinstance(capture_instances, Mapping)
        else {ci: {} for ci in capture_instances}
    )
    if facts_table:
        from . import migrations
        from .sink import FACTS_COLUMNS, FACTS_COMMENT

        migrations.ensure(spark, facts_table, "facts", FACTS_COLUMNS, FACTS_COMMENT)
    queries: dict[str, StreamingQuery] = {}
    try:
        for ci, own in tables.items():
            # options match ignoring case: the stream's own replace the shared ones
            mine = {k.lower() for k in own} | {"captureinstance"}
            opts = {k: v for k, v in options.items() if k.lower() not in mine}
            opts.update({k: v for k, v in own.items() if k.lower() != "captureinstance"})
            opts["captureInstance"] = ci
            ids = {k: t.format(ci=ci) for k, t in templates.items()}
            queries[ci] = stream(spark, opts).to_delta(
                ids["target"],
                ids["app_id"],
                ids["checkpoint"],
                facts_table=facts_table,
                **to_delta_kwargs,
            )
    except BaseException:
        stop_all(queries)
        raise
    return queries


def await_all(
    queries: Mapping[str, StreamingQueryLike], *, timeout: float | None = None
) -> dict[str, Exception]:
    """Wait until every query has stopped, or ``timeout`` seconds in all (keyword-only), and
    return the error of each query that stopped with one, by capture instance.

    It raises nothing for them: raise when the result is not empty, or the run succeeds
    with a table behind. ``is_data_loss`` and ``is_schema_changed`` tell which errors need
    a decision rather than a retry. A query still running at the timeout is not in it (see
    ``isActive``). With ``trigger={"availableNow": True}`` every query stops once it has
    caught up; for queries that keep running, ``spark.streams.awaitAnyTermination()``
    returns as soon as one stops.
    """
    from pyspark.errors import StreamingQueryException

    deadline = None if timeout is None else time.monotonic() + timeout
    for query in queries.values():
        with suppress(StreamingQueryException):  # read back through exception() below
            if deadline is None:
                query.awaitTermination()
            elif (left := deadline - time.monotonic()) > 0:
                query.awaitTermination(math.ceil(left))  # whole seconds, as typed
    return {ci: e for ci, q in queries.items() if (e := q.exception()) is not None}


def stop_all(queries: Mapping[str, StreamingQueryLike]) -> None:
    """Stop every query still running. Each resumes from its checkpoint when started again,
    e.g. by ``start_many`` with the same templates."""
    for query in queries.values():
        query.stop()
