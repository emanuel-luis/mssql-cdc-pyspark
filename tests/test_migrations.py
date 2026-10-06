"""Migrations re-run after a crash or a lost commit change nothing, and a table a newer release
migrated is written unless it needs that release."""

import os
import time
from unittest.mock import create_autospec

import pytest

from mssql_cdc import migrations, tables


def test_table_ref_doubles_a_backtick_in_a_path():  # the ALTER TABLEs migrations run take it
    assert tables.table_ref("/tmp/a`b") == "delta.`/tmp/a``b`"
    assert tables.table_ref("lab.cdc.orders") == "lab.cdc.orders"


def test_a_create_lost_to_a_concurrent_writer_takes_its_table(monkeypatch):
    # a stream and a backfill both create bronze: Delta fails the CREATE that loses the race
    delta = pytest.importorskip("delta.tables")
    builder = create_autospec(delta.DeltaTableBuilder, instance=True)  # a typo'd call fails
    for name in ("location", "tableName", "comment", "property", "addColumn"):
        getattr(builder, name).return_value = builder
    builder.execute.side_effect = RuntimeError("[DELTA_PROTOCOL_CHANGED] concurrent update")
    create = create_autospec(delta.DeltaTable.createIfNotExists, return_value=builder)
    monkeypatch.setattr(delta.DeltaTable, "createIfNotExists", create)
    exists = create_autospec(tables.exists, return_value=True)  # the other writer's table
    monkeypatch.setattr(tables, "exists", exists)
    spark = object()
    tables.create_if_not_exists(spark, "/w/t", [("a", "STRING", None)])
    create.assert_called_once_with(spark)
    builder.location.assert_called_once_with("/w/t")
    builder.addColumn.assert_called_once_with("a", "STRING", comment=None)
    exists.assert_called_once_with(spark, "/w/t")
    exists.return_value = False  # no table afterwards: the error is the caller's
    with pytest.raises(RuntimeError, match="DELTA_PROTOCOL_CHANGED"):
        tables.create_if_not_exists(spark, "/w/u", [("a", "INT", None)])


def _commits(spark, path):
    return spark.sql(f"DESCRIBE HISTORY delta.`{path}`").count()


def test_add_columns_skips_the_columns_a_table_already_has(delta_spark, workdir):
    path = os.path.join(workdir, "t")
    tables.create_if_not_exists(delta_spark, path, [("a", "STRING", None)])
    columns = [("b", "INT", "added"), ("A", "STRING", None)]  # a, in another case
    migrations.add_columns(delta_spark, path, columns)
    commits = _commits(delta_spark, path)
    migrations.add_columns(delta_spark, path, columns)  # a re-run: nothing left to add
    assert _commits(delta_spark, path) == commits
    assert delta_spark.read.format("delta").load(path).columns == ["a", "b"]


def test_migrate_stamps_a_migration_applied_before_a_crash(delta_spark, workdir):
    from mssql_cdc.finalization import CONTROL_COLUMNS
    from mssql_cdc.migrations import control
    from mssql_cdc.migrations.control import APPLIED_COLUMNS, WAVE_COLUMNS

    path = os.path.join(workdir, "control")
    added = {name for name, _, _ in APPLIED_COLUMNS + WAVE_COLUMNS}
    tables.create_if_not_exists(
        delta_spark,
        path,
        [c for c in CONTROL_COLUMNS if c[0] not in added],
        properties={migrations.SCHEMA_VERSION_PROPERTY: "0"},
    )
    migrations.add_columns(delta_spark, path, APPLIED_COLUMNS)  # migration 1, then the crash
    assert migrations.migrate(delta_spark, path, "control") == len(control.MIGRATIONS)
    detail = delta_spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    assert detail["properties"][migrations.SCHEMA_VERSION_PROPERTY] == str(len(control.MIGRATIONS))


def test_a_table_a_newer_release_migrated_is_written_unless_it_needs_that_release(
    delta_spark, workdir, caplog
):
    # jobs sharing a table upgrade one at a time: the ones still older keep writing it
    path = os.path.join(workdir, "control")
    tables.create_if_not_exists(
        delta_spark,
        path,
        [("table_name", "STRING", None)],
        properties={migrations.SCHEMA_VERSION_PROPERTY: "99"},
    )
    commits = _commits(delta_spark, path)
    with caplog.at_level("WARNING", logger="mssql_cdc.migrations.base"):
        assert migrations.migrate(delta_spark, path, "control") == 99
        assert migrations.migrate(delta_spark, path, "control") == 99
    [warning] = [r.getMessage() for r in caplog.records if "newer release" in r.getMessage()]
    assert "control schema version 99" in warning  # once per table and version
    assert _commits(delta_spark, path) == commits  # nothing written
    # unless a migration older releases would misread asked for one that knows it
    delta_spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('{migrations.MIN_VERSION_PROPERTY}' = '99')"
    )
    with pytest.raises(ValueError, match="knows 99 control migrations"):
        migrations.migrate(delta_spark, path, "control")


def test_a_migration_that_loses_to_a_concurrent_commit_runs_again(
    delta_spark, workdir, monkeypatch
):
    # every tracker, silver job and stream sharing a table migrates it on the same upgrade
    from mssql_cdc.migrations import base

    path, runs = os.path.join(workdir, "t"), []
    tables.create_if_not_exists(delta_spark, path, [("a", "STRING", None)])

    def lost_once(spark, table):
        runs.append(1)
        if len(runs) == 1:
            raise RuntimeError("[DELTA_METADATA_CHANGED] the metadata changed concurrently")

    monkeypatch.setattr(base, "_migrations", lambda kind: [migrations.Migration("m", lost_once)])
    clock = create_autospec(time, spec_set=True)  # tables' binding only: time stays real elsewhere
    clock.monotonic.side_effect = time.monotonic
    monkeypatch.setattr(tables, "time", clock)
    assert migrations.migrate(delta_spark, path, "facts") == 1
    detail = delta_spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    assert len(runs) == 2 and detail["properties"][migrations.SCHEMA_VERSION_PROPERTY] == "1"
    assert clock.sleep.call_count == 1  # one backoff, between the two runs
