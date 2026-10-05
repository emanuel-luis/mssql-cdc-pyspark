"""State a released version wrote resumes with this code (ADR 0021): offsets, the checkpoint
layout with its generations, and the bronze, silver, facts and control tables.

``tests/compat/<version>`` holds what ``generate.py`` wrote with that version's wheel; a
release adds its own (docs/RELEASING.md)."""

import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from mssql_cdc import apply_changes, finalization, migrations, stream
from mssql_cdc.fake import FakeCdcClient, FakeCdcDatabase

pytestmark = pytest.mark.delta
HERE = Path(__file__).parent
VERSIONS = sorted(p.name for p in HERE.iterdir() if (p / "manifest.json").is_file())


def _batches(checkpoint: str) -> list[int]:
    return sorted(int(n) for n in os.listdir(os.path.join(checkpoint, "commits")) if n.isdigit())


def _generation(checkpoint: str) -> int:
    with open(os.path.join(checkpoint, "_mssql_cdc_generation.json"), encoding="utf-8") as fh:
        return json.load(fh)["generation"]


@pytest.mark.parametrize("version", VERSIONS)
def test_the_state_a_release_wrote_resumes(delta_spark, tmp_path, version):
    spark = delta_spark
    shutil.copytree(HERE / version, tmp_path, dirs_exist_ok=True)
    m = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    tables = m["tables"]
    database = {name.split(".")[0] for name in tables.values()}.pop()
    spark.sql(f"CREATE DATABASE {database}")
    try:
        for kind, name in tables.items():
            location = (tmp_path / "delta" / kind).as_uri()
            spark.sql(f"CREATE TABLE {name} USING delta LOCATION '{location}'")
        _resume(spark, m, str(tmp_path / "src"), str(tmp_path / "ckpt"))
    finally:
        spark.sql(f"DROP DATABASE {database} CASCADE")  # external tables: tmp_path keeps them


def _resume(spark, m: dict, src: str, ckpt: str) -> None:
    tables, key, value, app_id = m["tables"], m["key"], m["value"], m["app_id"]
    bronze, silver, facts, control = (tables[k] for k in ("bronze", "silver", "facts", "control"))
    ci = m["options"]["captureInstance"]
    options = {**m["options"], "backend": "fake", "fakePath": src}
    db = FakeCdcDatabase(src, [ci])
    reader = FakeCdcClient(src)
    last = datetime.fromisoformat(reader.lsn_to_time(reader.max_lsn()))
    minutes = iter(range(1, 1000))

    def at():
        return last + timedelta(minutes=next(minutes))

    def commit(*changes):
        return db.commit(ci, [(op, {key: k, value: v}) for op, k, v in changes], at=at())

    def run(**kw):
        q = stream(spark, options).to_delta(
            bronze,
            app_id,
            ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            on_data_loss="resnapshot",
            **kw,
        )
        q.awaitTermination()
        end = finalization.end_offset_from_progress(q.lastProgress)
        finalization.advance(spark, control, bronze, end)
        done = apply_changes(
            spark, bronze, silver, ci, [key], control_table=control, facts_table=facts
        )
        return end, done

    def changes():  # every change row bronze holds, by its identity in CDC
        rows = spark.table(bronze).where("_operation != 0")
        return rows.groupBy("_start_lsn", "_seqval", "_operation").count().collect()

    def source_table():
        with open(os.path.join(src, "tables", f"{ci}.json"), encoding="utf-8") as fh:
            return sorted((r[key], r[value]) for r in json.load(fh).values())

    def snapshots():
        return spark.table(bronze).where("_operation = 0").select("_start_lsn").distinct().count()

    n = _generation(ckpt)
    live = os.path.join(ckpt, "_generations", str(n))
    batches, before, snaps = _batches(live), changes(), snapshots()
    assert batches and all(r["count"] == 1 for r in before)

    # the release's checkpoint, offsets and generation state carry on: only the new commits
    commit((2, 100, "new"))
    commit((3, 100, "new"), (4, 100, "paid"), (2, 101, "new"))
    commit((1, 100, "paid"))
    end, done = run()
    assert _generation(ckpt) == n and snapshots() == snaps
    assert _batches(live) == batches + list(range(batches[-1] + 1, batches[-1] + 4))
    after = changes()
    assert all(r["count"] == 1 for r in after) and len(after) == len(before) + 5
    assert end["lsn"] == reader.max_lsn()
    for kind, name in tables.items():  # every table migrated to this release's schema
        properties = spark.sql(f"DESCRIBE DETAIL {name}").first()["properties"]
        assert int(properties[migrations.SCHEMA_VERSION_PROPERTY]) == (
            migrations.current_version(kind)
        ), kind
    verdict = finalization.finalized_until(spark, control, bronze)
    assert verdict == finalization.truncate(datetime.fromisoformat(end["commit_ts"]))
    assert done["finalized_until"] == verdict and done["applied_lsn"] == end["lsn"]
    assert sorted((r[key], r[value]) for r in spark.table(silver).collect()) == source_table()
    batch_ids = spark.table(facts).where(f"app_id = '{app_id}.g{n}' AND batch_id IS NOT NULL")
    assert batch_ids.count() == batch_ids.select("batch_id").distinct().count()

    # a loss now: the release's generation state leads to the next generation
    commit((2, 102, "new"))
    db.cleanup(ci, db.idle(at=at()))
    commit((2, 103, "new"))
    run(resnapshot_interval_days=0)  # the release's re-snapshot was less than 7 days ago
    assert _generation(ckpt) == n + 1 and snapshots() == snaps + 1
    events = spark.table(facts).where("event = 'resnapshot'").orderBy("written_at").collect()
    assert [e["app_id"] for e in events][-2:] == [f"{app_id}.g{n}", f"{app_id}.g{n + 1}"]
    assert sorted((r[key], r[value]) for r in spark.table(silver).collect()) == source_table()
