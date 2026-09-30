"""apply_changes: bronze change log -> current-state Delta table (ADR 0019)."""

import os
from datetime import datetime, timedelta

import pytest

from mssql_cdc import apply_changes, finalization, stream
from mssql_cdc.fake import FakeCdcClient, FakeCdcDatabase

pytestmark = pytest.mark.delta
CI = "dbo_orders"
COLUMNS = "order_id INT, status STRING"
T0 = datetime(2026, 9, 30, 9, 0)


class Orders:
    """A keyed fake, the stream into bronze, and apply_changes into silver."""

    def __init__(self, spark, workdir):
        self.spark, self.src = spark, os.path.join(workdir, "src")
        self.db = FakeCdcDatabase(self.src, [CI], keys={CI: "order_id"})
        self.options = {
            "backend": "fake",
            "fakePath": self.src,
            "captureInstance": CI,
            "columns": COLUMNS,
            "maxCommitsPerBatch": "2",
        }
        self.bronze, self.silver, self.facts, self.control, self.ckpt = (
            os.path.join(workdir, n) for n in ("bronze", "silver", "facts", "control", "ckpt")
        )
        self.at = T0

    def commit(self, *changes, minutes=1):
        self.at += timedelta(minutes=minutes)
        return self.db.commit(CI, list(changes), at=self.at)

    def run(self, **kw):
        q = stream(self.spark, self.options).to_delta(
            self.bronze, "orders-v1", self.ckpt, trigger={"availableNow": True}, **kw
        )
        q.awaitTermination()
        return q

    def apply(self, **kw):
        return apply_changes(
            self.spark, self.bronze, self.silver, CI, ["order_id"], control_table=self.control, **kw
        )

    def rows(self):
        rows = self.spark.read.format("delta").load(self.silver).collect()
        return sorted(((r["order_id"], r["status"]) for r in rows), key=str)

    def source(self):  # the fake's table now: what silver must equal
        return sorted(
            ((r["order_id"], r["status"]) for r in FakeCdcClient(self.src)._table(CI)), key=str
        )

    def commits(self):
        return self.spark.sql(f"DESCRIBE HISTORY delta.`{self.silver}`").count()


def _row(order_id, status):
    return {"order_id": order_id, "status": status}


def test_changes_apply_incrementally_and_a_rerun_or_a_position_left_behind_changes_nothing(
    delta_spark, workdir
):
    o = Orders(delta_spark, workdir)
    # before the stream writes its first batch there is no bronze: nothing to apply yet
    assert o.apply() == {"rebuilt": False, "applied_lsn": None, "finalized_until": None}
    o.commit((2, _row(1, "new")), (2, _row(2, "new")))
    o.commit((3, _row(1, "new")), (4, _row(1, "paid")))
    o.commit((2, _row(None, "new")))  # a unique index admits one NULL key
    o.commit((1, _row(2, "new")))
    o.run()
    first = o.apply()
    assert o.rows() == o.source() == [(1, "paid"), (None, "new")]
    assert first["rebuilt"] and first["finalized_until"] is None  # no bronze verdict yet

    # one transaction: 4 inserted, updated and deleted; 5 deleted and inserted again
    o.commit((2, _row(4, "new")), (3, _row(4, "new")), (4, _row(4, "paid")), (1, _row(4, "paid")))
    o.commit((2, _row(5, "new")))
    o.commit((1, _row(5, "new")), (2, _row(5, "again")))
    o.commit((3, _row(None, "new")), (4, _row(None, "paid")))
    last = o.commit((1, _row(1, "paid")))  # the newest change is a delete
    o.run()
    second = o.apply()
    assert not second["rebuilt"] and second["applied_lsn"] == last
    assert o.rows() == o.source() == [(5, "again"), (None, "paid")]

    commits = o.commits()
    assert o.apply()["applied_lsn"] == last and o.commits() == commits  # nothing new: no MERGE

    # a crash between the MERGE and its position: the next call applies from behind again
    from mssql_cdc.silver import _record

    _record(delta_spark, o.control, o.silver, "0x" + "0" * 20, None)
    again = o.apply()
    assert not again["rebuilt"] and again["applied_lsn"] == last
    assert o.rows() == o.source()  # no duplicate, 1 and 2 not resurrected, 5 not reverted

    fields = {f.name: f for f in delta_spark.read.format("delta").load(o.silver).schema}
    assert fields["_commit_ts"].dataType.simpleString() == "timestamp_ntz"
    assert fields["_start_lsn"].metadata["comment"] and "_operation" not in fields
    detail = delta_spark.sql(f"DESCRIBE DETAIL delta.`{o.silver}`").first()
    assert "Current state" in detail["description"]
    assert detail["properties"]["mssql_cdc.schema_version"] == "0"


def test_snapshot_then_changes_and_the_verdict_is_the_bronze_one_read_before_the_apply(
    delta_spark, workdir, monkeypatch
):
    o = Orders(delta_spark, workdir)
    for i in range(5):
        o.commit((2, _row(i, "new")))
    o.commit((1, _row(3, "new")))
    o.run(bootstrap=True)  # the snapshot: 0, 1, 2, 4
    assert o.apply()["finalized_until"] is None
    assert o.rows() == o.source() == [(0, "new"), (1, "new"), (2, "new"), (4, "new")]

    o.commit((3, _row(1, "new")), (4, _row(1, "paid")), minutes=60)
    o.commit((2, _row(9, "new")))
    q = o.run(bootstrap=True)
    end = finalization.end_offset_from_progress(q.lastProgress)
    bronze_fu = finalization.advance(delta_spark, o.control, o.bronze, end)
    applied = o.apply()
    assert applied["finalized_until"] == bronze_fu == datetime(2026, 9, 30, 10, 0)
    assert o.rows() == o.source()

    def advance_bronze():
        q = o.run(bootstrap=True)
        end = finalization.end_offset_from_progress(q.lastProgress)
        return finalization.advance(delta_spark, o.control, o.bronze, end)

    o.commit((1, _row(0, "new")), minutes=120)
    later = advance_bronze()
    assert later > bronze_fu

    # bronze gets a batch and a newer verdict during the call, after it read the verdict and
    # pinned bronze (it opens silver for the MERGE then): silver keeps the verdict it read
    from mssql_cdc import silver

    real, raced = silver.delta_table, []

    def racing(spark, name):
        if name == o.silver and not raced:
            o.commit((2, _row(7, "new")), minutes=120)
            raced.append(advance_bronze())
        return real(spark, name)

    monkeypatch.setattr(silver, "delta_table", racing)
    assert o.apply()["finalized_until"] == later < raced[0]
    assert (7, "new") not in o.rows() and o.rows() != o.source()
    assert not finalization.is_final(delta_spark, o.control, o.silver, raced[0])
    assert o.apply()["finalized_until"] == raced[0]  # the next call applies it
    assert o.rows() == o.source()


def test_a_resnapshot_rebuilds_silver_without_the_key_deleted_in_the_purged_gap(
    delta_spark, workdir
):
    o = Orders(delta_spark, workdir)
    for i in range(3):
        o.commit((2, _row(i, "new")))

    def run():
        o.run(facts_table=o.facts, bootstrap=True, on_data_loss="resnapshot")

    run()
    assert o.apply()["rebuilt"] and o.rows() == o.source()
    o.commit((2, _row(3, "new")))
    run()
    assert not o.apply()["rebuilt"]
    o.commit((3, _row(1, "new")), (4, _row(1, "paid")))
    o.commit((1, _row(2, "new")))
    o.db.cleanup(CI, o.db.idle(at=o.at + timedelta(minutes=2)))  # purged before the stream read it
    run()  # generation 1: a new snapshot, and no delete row for 2
    rebuilt = o.apply()
    assert rebuilt["rebuilt"] and o.rows() == o.source() == [(0, "new"), (1, "paid"), (3, "new")]

    o.commit((2, _row(7, "new")), minutes=5)
    run()
    assert not o.apply()["rebuilt"] and o.rows() == o.source()


def test_a_bootstrap_at_the_lsn_silver_has_applied_still_rebuilds_it(delta_spark, workdir):
    o = Orders(delta_spark, workdir)
    o.commit((2, _row(1, "new")))
    last = o.commit((2, _row(2, "new")))
    o.db.cleanup(CI, last)  # 1 is only in the table now
    o.run()
    assert o.apply()["applied_lsn"] == last and o.rows() == [(2, "new")]
    # bootstrap added to the same checkpoint on a quiet database: the snapshot is stamped
    # with max_lsn, the LSN silver has already applied
    o.run(bootstrap=True)
    rebuilt = o.apply()
    assert rebuilt["rebuilt"] and rebuilt["applied_lsn"] == last
    assert o.rows() == o.source() == [(1, "new"), (2, "new")]
    assert not o.apply()["rebuilt"]


def test_the_resnapshot_of_an_emptied_table_empties_silver_once(delta_spark, workdir):
    o = Orders(delta_spark, workdir)
    for i in range(3):
        o.commit((2, _row(i, "new")))

    def run():
        o.run(facts_table=o.facts, bootstrap=True, on_data_loss="resnapshot")

    run()
    o.apply()
    assert len(o.rows()) == 3
    o.commit(*[(1, _row(i, "new")) for i in range(3)])
    o.db.cleanup(CI, o.db.idle(at=o.at + timedelta(minutes=2)))
    run()  # the snapshot has no rows: only its event marks it
    emptied = o.apply(facts_table=o.facts)
    assert emptied["rebuilt"] and o.rows() == o.source() == []
    commits = o.commits()
    again = o.apply(facts_table=o.facts)  # the event is applied: no rebuild on every call
    assert not again["rebuilt"] and again["applied_lsn"] == emptied["applied_lsn"]
    assert o.commits() == commits


def test_keys_come_from_the_capture_instance_when_not_given(delta_spark, workdir):
    o = Orders(delta_spark, workdir)
    o.commit((2, _row(1, "new")))
    o.run()
    with pytest.raises(ValueError, match="pass keys"):
        apply_changes(delta_spark, o.bronze, o.silver, CI, control_table=o.control)
    with pytest.raises(ValueError, match="not captured columns"):
        apply_changes(delta_spark, o.bronze, o.silver, CI, ["id"], control_table=o.control)
    apply_changes(delta_spark, o.bronze, o.silver, CI, control_table=o.control, options=o.options)
    assert o.rows() == o.source() == [(1, "new")]
    unkeyed = FakeCdcDatabase(os.path.join(workdir, "unkeyed"), ["dbo_x"])
    with pytest.raises(ValueError, match="no unique index"):
        apply_changes(
            delta_spark,
            o.bronze,
            o.silver,
            "dbo_x",
            control_table=o.control,
            options={"backend": "fake", "fakePath": unkeyed.path},
        )


def test_a_control_table_at_version_0_gains_applied_lsn(delta_spark, workdir):
    from mssql_cdc import migrations, tables
    from mssql_cdc.finalization import CONTROL_COLUMNS
    from mssql_cdc.migrations.control import APPLIED_COLUMNS

    old = os.path.join(workdir, "control_v0")
    tables.create_if_not_exists(
        delta_spark,
        old,
        [c for c in CONTROL_COLUMNS if c not in APPLIED_COLUMNS],
        properties={migrations.SCHEMA_VERSION_PROPERTY: "0"},
    )
    assert migrations.migrate(delta_spark, old, "control") == 1
    schema = delta_spark.read.format("delta").load(old).schema
    for name in ("applied_lsn", "snapshot_lsn"):
        assert schema[name].dataType.simpleString() == "string" and schema[name].metadata["comment"]
