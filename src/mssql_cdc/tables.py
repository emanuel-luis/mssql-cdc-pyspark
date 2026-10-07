"""Delta Lake tables through delta-spark's ``DeltaTable`` API: open one by name or path,
and create one with typed, commented columns."""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, TypeAlias, TypeVar

if TYPE_CHECKING:
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DataType

T = TypeVar("T")
_RETRY_SECONDS = 60  # how long a commit that loses to concurrent ones is retried
# Delta's concurrent-modification errors: by class on classic PySpark, by error class in the
# message on Spark Connect, which raises its own exception types
_CONFLICTS = {
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "MetadataChangedException",
}
_CONFLICT_CODES = ("DELTA_CONCURRENT", "DELTA_METADATA_CHANGED")
# Delta refuses these in a column name unless the table maps columns by name; SQL Server
# allows them in a bracketed one, such as [Unit Price]
_MAPPED_ONLY = frozenset(" ,;{}()\n\t=")


def is_conflict(exc: BaseException) -> bool:
    if _CONFLICTS.intersection(c.__name__ for c in type(exc).__mro__):
        return True
    return any(code in str(exc) for code in _CONFLICT_CODES)


def retrying(commit: Callable[[], T]) -> T:
    """``commit()``, run again while it loses to a concurrent Delta commit: in OSS Delta two
    MERGEs on the small control table conflict even when they change other rows (ADR 0026),
    and every job sharing a table runs its migrations on the same upgrade. Safe only for an
    idempotent ``commit``. Exponential backoff with full jitter, for up to
    ``_RETRY_SECONDS``; then, or on any other error, it raises."""
    deadline, attempt = time.monotonic() + _RETRY_SECONDS, 0
    while True:
        try:
            return commit()
        except Exception as exc:
            left = deadline - time.monotonic()
            if not is_conflict(exc) or left <= 0:
                raise
            time.sleep(min(left, random.uniform(0, min(10.0, 0.5 * 2**attempt))))
            attempt += 1


def is_path(name_or_path: str) -> bool:
    return "/" in name_or_path or ":" in name_or_path


def table_ref(name_or_path: str) -> str:
    """A table name, or ``delta.`path``` when given a filesystem/object-store path (for SQL);
    a backtick in the path is doubled, as SQL escapes it inside backticks."""
    if not is_path(name_or_path):
        return name_or_path
    return "delta.`" + name_or_path.replace("`", "``") + "`"


def delta_table(spark: SparkSession, name_or_path: str) -> DeltaTable:
    from delta.tables import DeltaTable

    if is_path(name_or_path):
        return DeltaTable.forPath(spark, name_or_path)
    return DeltaTable.forName(spark, name_or_path)


def exists(spark: SparkSession, name_or_path: str) -> bool:
    if is_path(name_or_path):
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(spark, name_or_path)
    return spark.catalog.tableExists(name_or_path)


ColumnDef: TypeAlias = tuple[str, "str | DataType", "str | None"]
"""A column to create: ``(name, type, comment)``; ``type`` is a DDL string or a Spark DataType."""


def create_if_not_exists(
    spark: SparkSession,
    name_or_path: str,
    columns: Iterable[ColumnDef],
    comment: str | None = None,
    properties: Mapping[str, str] | None = None,
) -> None:
    """``columns``: ``(name, type, comment)``; ``type`` is a DDL string or a Spark DataType.

    A column name Delta takes only with column mapping (a space, or one of ``,;{}()=``)
    creates the table with ``delta.columnMapping.mode = 'name'`` (the
    ``columnMapping`` feature in its Delta protocol, which older engines refuse); no
    other name does.

    A no-op when the table exists: its schema, comments and properties are left as they
    are (``mssql_cdc.migrations`` changes existing tables). That includes a table another
    writer created meanwhile, such as a stream and a backfill both creating bronze: Delta
    fails the CREATE that loses the race (``DELTA_PROTOCOL_CHANGED`` on a path). An
    existing table without column mapping that lacks such a column raises
    ``SchemaChangedError`` instead, before anything writes it (see ``_require_mapping``).
    """
    from delta.tables import DeltaTable

    columns = list(columns)
    mapped = [c[0] for c in columns if _MAPPED_ONLY.intersection(c[0])]
    if mapped:
        properties = {"delta.columnMapping.mode": "name", **(properties or {})}
    builder = DeltaTable.createIfNotExists(spark)
    builder = (
        builder.location(name_or_path) if is_path(name_or_path) else builder.tableName(name_or_path)
    )
    if comment:
        builder = builder.comment(comment)
    for key, value in (properties or {}).items():
        builder = builder.property(key, value)
    for name, data_type, column_comment in columns:
        builder = builder.addColumn(name, data_type, comment=column_comment)
    try:
        builder.execute()
    except Exception:
        if not exists(spark, name_or_path):  # else another writer's CREATE won the race
            raise
    if mapped:
        _require_mapping(spark, name_or_path, mapped)


def _require_mapping(spark: SparkSession, name_or_path: str, names: list[str]) -> None:
    """Raise ``SchemaChangedError`` when ``names``, which Delta takes only with column
    mapping, holds a column the table lacks and the table maps no columns: a newer capture
    instance captures it, and the next append (``mergeSchema``) or ``apply_changes`` would
    fail with Delta's own error midway. The library never changes an existing table's
    protocol: the message gives the statement that does."""
    table = delta_table(spark, name_or_path)
    have = {f.name.lower() for f in table.toDF().schema}
    new = [n for n in names if n.lower() not in have]
    if not new:
        return
    detail = table.detail().first()
    assert detail is not None  # DESCRIBE DETAIL returns one row
    properties = detail["properties"] or {}
    if properties.get("delta.columnMapping.mode", "none") != "none":  # 'name' or 'id'
        return
    from .client import SchemaChangedError

    raise SchemaChangedError(
        f"{name_or_path}: column mapping is off on this table, and Delta takes the new "
        f"column(s) {', '.join(map(repr, new))} only with it. Enable it, then run again: "
        f"ALTER TABLE {table_ref(name_or_path)} SET TBLPROPERTIES "
        "('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', "
        "'delta.minWriterVersion' = '5'). That upgrades the table's Delta protocol: every "
        "reader and writer of the table then needs a Delta that supports column mapping."
    )
