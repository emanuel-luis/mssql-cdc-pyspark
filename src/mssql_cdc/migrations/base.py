"""Migration runner and helpers (see the package docstring)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from ..tables import delta_table, is_path, table_ref

SCHEMA_VERSION_PROPERTY = "mssql_cdc.schema_version"


@dataclass(frozen=True)
class Migration:
    description: str
    apply: Callable  # (spark, name_or_path) -> None


def _migrations(kind: str) -> list[Migration]:
    from . import bronze, control, facts, reconcile, silver

    kinds = {"bronze": bronze, "control": control, "facts": facts, "reconcile": reconcile}
    return {**kinds, "silver": silver}[kind].MIGRATIONS


def current_version(kind: str) -> int:
    """The schema version a table of ``kind`` is created with: its number of migrations."""
    return len(_migrations(kind))


def migrate(spark, table: str, kind: str) -> int:
    """Apply the migrations ``table`` has not had yet; return its version afterwards.

    A migration and its version stamp are two commits: one re-run after a crash between
    them, or by a job that lost the race to stamp it, must change nothing (``add_columns``
    skips the columns the table has, ``set_comments`` sets the same text)."""
    migrations = _migrations(kind)
    properties = delta_table(spark, table).detail().first()["properties"] or {}
    version = int(properties.get(SCHEMA_VERSION_PROPERTY, 0))  # unstamped = created before any
    if version > len(migrations):
        raise ValueError(
            f"{table} is at {kind} schema version {version}, and this mssql-cdc-pyspark knows "
            f"{len(migrations)}: a newer release migrated it. Upgrade mssql-cdc-pyspark rather "
            "than run an older one against it."
        )
    for number, migration in enumerate(migrations[version:], start=version + 1):
        migration.apply(spark, table)
        spark.sql(
            f"ALTER TABLE {table_ref(table)} SET TBLPROPERTIES "
            f"('{SCHEMA_VERSION_PROPERTY}' = '{number}')"
        )
    return len(migrations)


def ensure(spark, table: str, kind: str, columns, comment: str) -> None:
    """Create ``table`` in the latest shape of ``kind`` (stamped with its version), or
    bring an existing one up to it."""
    from ..tables import create_if_not_exists

    create_if_not_exists(
        spark,
        table,
        columns,
        comment,
        properties={SCHEMA_VERSION_PROPERTY: str(current_version(kind))},
    )
    migrate(spark, table, kind)


def add_columns(spark, table: str, columns) -> None:
    """Add nullable ``(name, type, comment)`` columns with an empty append and
    ``mergeSchema``: a metadata-only commit; existing rows read NULL. A column the table
    already has (ignoring case, as Delta does) is skipped; with none left, nothing is written."""
    from pyspark.sql.types import StructField, StructType

    fields = list(delta_table(spark, table).toDF().schema.fields)
    have = {f.name.lower() for f in fields}
    columns = [c for c in columns if c[0].lower() not in have]
    if not columns:
        return
    for name, data_type, comment in columns:
        if isinstance(data_type, str):
            data_type = spark.createDataFrame([], f"`{name}` {data_type}").schema[0].dataType
        fields.append(StructField(name, data_type, True, {"comment": comment} if comment else {}))
    writer = (
        spark.createDataFrame([], StructType(fields))
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
    )
    writer.save(table) if is_path(table) else writer.saveAsTable(table)


def set_comments(spark, table: str, columns: dict, table_comment: str | None = None) -> None:
    """Replace the comments of existing columns ``{name: comment}``, and of the table when
    ``table_comment`` is given: metadata-only commits, for columns whose meaning changed."""

    def literal(text: str) -> str:
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"

    ref = table_ref(table)
    if table_comment is not None:
        spark.sql(f"COMMENT ON TABLE {ref} IS {literal(table_comment)}")
    for name, comment in columns.items():
        spark.sql(f"ALTER TABLE {ref} ALTER COLUMN `{name}` COMMENT {literal(comment)}")
