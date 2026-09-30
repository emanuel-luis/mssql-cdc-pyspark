"""Migration runner and helpers (see the package docstring)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..tables import delta_table, is_path, table_ref

SCHEMA_VERSION_PROPERTY = "mssql_cdc.schema_version"


@dataclass(frozen=True)
class Migration:
    description: str
    apply: Callable  # (spark, name_or_path) -> None


def _migrations(kind: str) -> list[Migration]:
    from . import bronze, control, facts

    return {"bronze": bronze, "control": control, "facts": facts}[kind].MIGRATIONS


def current_version(kind: str) -> int:
    """The schema version a table of ``kind`` is created with: its number of migrations."""
    return len(_migrations(kind))


def migrate(spark, table: str, kind: str) -> int:
    """Apply the migrations ``table`` has not had yet; return its version afterwards."""
    migrations = _migrations(kind)
    properties = delta_table(spark, table).detail().first()["properties"] or {}
    version = int(properties.get(SCHEMA_VERSION_PROPERTY, 0))  # unstamped = created before any
    for number, migration in enumerate(migrations[version:], start=version + 1):
        migration.apply(spark, table)
        spark.sql(f"ALTER TABLE {table_ref(table)} SET TBLPROPERTIES "
                  f"('{SCHEMA_VERSION_PROPERTY}' = '{number}')")
    return max(version, len(migrations))


def ensure(spark, table: str, kind: str, columns, comment: str) -> None:
    """Create ``table`` in the latest shape of ``kind`` (stamped with its version), or
    bring an existing one up to it."""
    from ..tables import create_if_not_exists

    create_if_not_exists(spark, table, columns, comment,
                         properties={SCHEMA_VERSION_PROPERTY: str(current_version(kind))})
    migrate(spark, table, kind)


def add_columns(spark, table: str, columns) -> None:
    """Add nullable ``(name, type, comment)`` columns with an empty append and
    ``mergeSchema``: a metadata-only commit; existing rows read NULL."""
    from pyspark.sql.types import StructField, StructType

    fields = list(delta_table(spark, table).toDF().schema.fields)
    for name, data_type, comment in columns:
        if isinstance(data_type, str):
            data_type = spark.createDataFrame([], f"`{name}` {data_type}").schema[0].dataType
        fields.append(StructField(name, data_type, True, {"comment": comment} if comment else {}))
    writer = (spark.createDataFrame([], StructType(fields)).write.format("delta")
              .mode("append").option("mergeSchema", "true"))
    writer.save(table) if is_path(table) else writer.saveAsTable(table)
