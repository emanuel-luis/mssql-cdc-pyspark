"""Delta Lake tables through delta-spark's ``DeltaTable`` API: open one by name or path,
and create one with typed, commented columns."""

from __future__ import annotations

from collections.abc import Iterable


def is_path(name_or_path: str) -> bool:
    return "/" in name_or_path or ":" in name_or_path


def table_ref(name_or_path: str) -> str:
    """A table name, or ``delta.`path``` when given a filesystem/object-store path (for SQL);
    a backtick in the path is doubled, as SQL escapes it inside backticks."""
    if not is_path(name_or_path):
        return name_or_path
    return "delta.`" + name_or_path.replace("`", "``") + "`"


def delta_table(spark, name_or_path: str):
    from delta.tables import DeltaTable

    if is_path(name_or_path):
        return DeltaTable.forPath(spark, name_or_path)
    return DeltaTable.forName(spark, name_or_path)


def exists(spark, name_or_path: str) -> bool:
    if is_path(name_or_path):
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(spark, name_or_path)
    return spark.catalog.tableExists(name_or_path)


def create_if_not_exists(
    spark,
    name_or_path: str,
    columns: Iterable[tuple],
    comment: str | None = None,
    properties: dict | None = None,
):
    """``columns``: ``(name, type, comment)``; ``type`` is a DDL string or a Spark DataType.

    A no-op when the table exists: its schema, comments and properties are left as they
    are (``mssql_cdc.migrations`` changes existing tables). That includes a table another
    writer created meanwhile, such as a stream and a backfill both creating bronze: Delta
    fails the CREATE that loses the race (``DELTA_PROTOCOL_CHANGED`` on a path).
    """
    from delta.tables import DeltaTable

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
