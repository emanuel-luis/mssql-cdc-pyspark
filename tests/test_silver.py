"""apply_changes: bronze change log -> current-state Delta table (ADR 0019)."""

import json
import os
from datetime import date, datetime, timedelta

import pytest

from mssql_cdc import apply_changes, finalization, stream
from mssql_cdc.fake import FakeCdcClient, FakeCdcDatabase

pytestmark = pytest.mark.delta
CI = "dbo_orders"
COLUMNS = "order_id INT, status STRING"
T0 = datetime(2026, 9, 30, 9, 0)


class Orders:
    """A keyed fake, the stream into bronze, and apply_changes into silver."""

    def __init__(self, spark, workdir, infer=False):
        """``infer``: the fake reports the captured columns, and the stream infers them."""
        self.spark, self.src = spark, os.path.join(workdir, "src")
        self.db = FakeCdcDatabase(
            self.src, [CI], keys={CI: "order_id"}, **({"columns": {CI: COLUMNS}} if infer else {})
        )
        self.options = {
            "backend": "fake",
            "fakePath": self.src,
            "captureInstance": CI,
            "maxCommitsPerBatch": "2",
            **({} if infer else {"columns": COLUMNS}),
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
        kw.setdefault("facts_table", self.facts)  # the verdict needs it, even when it is empty
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
    # without the facts, a snapshot open in them alone could not be seen: no verdict
    assert o.apply(facts_table=None)["finalized_until"] is None
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


def test_a_chunked_resnapshot_deletes_the_keys_lost_in_the_gap_wave_by_wave(delta_spark, workdir):
    o = Orders(delta_spark, workdir)
    o.options["numPartitions"] = "1"  # one chunk per wave
    for i in range(6):
        o.commit((2, _row(i, "new")))

    def run(**kw):
        o.run(facts_table=o.facts, bootstrap=True, on_data_loss="resnapshot", **kw)

    run()
    o.apply()
    o.commit((1, _row(1, "new")))  # deleted in the gap: in chunk 0, [-, 3)
    o.commit((1, _row(4, "new")))  # and in chunk 1, [3, 6)
    o.db.cleanup(CI, o.db.idle(at=o.at + timedelta(minutes=2)))
    run(snapshot="chunked")  # generation 1 opens a re-snapshot: nothing read yet
    assert not o.apply()["rebuilt"] and len(o.rows()) == 6

    def wave():
        stream(o.spark, o.options).backfill(
            o.bronze, app_id="orders-v1", facts_table=o.facts, chunk_rows=2, max_waves=1
        )
        return o.apply()

    assert not wave()["rebuilt"]
    assert [k for k, _ in o.rows()] == [0, 2, 3, 4, 5]  # 1 gone before the completion
    assert wave()["rebuilt"] and o.rows() == o.source() == [(i, "new") for i in (0, 2, 3, 5)]


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
    # in another case than bronze's rows and the fake's instance: both match ignoring it
    ci = CI.upper()
    apply_changes(delta_spark, o.bronze, o.silver, ci, control_table=o.control, options=o.options)
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


def test_silver_follows_the_switch_to_a_newer_capture_instance_and_gains_its_column(
    delta_spark, workdir
):
    o = Orders(delta_spark, workdir, infer=True)
    o.commit((2, _row(1, "new")), (2, _row(2, "new")))
    o.run()
    o.apply(options=o.options)
    v2 = o.db.add_capture_instance(CI, COLUMNS + ", note STRING")
    o.commit((3, _row(1, "new")), (4, {**_row(1, "paid"), "note": "gift"}))
    o.commit((1, _row(2, "new")))
    o.run()  # a restart: the new column is inferred, and the rows from v2's start are v2's

    def rows():
        got = o.spark.read.format("delta").load(o.silver).collect()
        return sorted((r["order_id"], r["status"], r["note"]) for r in got)

    with pytest.raises(ValueError, match=f"'{v2}'.*pass options"):
        o.apply()  # without the options it cannot tell v2 is the same table's: never skipped
    o.apply(options=o.options)
    assert rows() == [(1, "paid", "gift")]
    o.db.drop_capture_instance(CI)  # the stream and silver carry on with v2 alone
    o.commit((2, {**_row(3, "new"), "note": "rush"}))
    o.run()
    assert not o.apply(options=o.options)["rebuilt"]
    assert rows() == [(1, "paid", "gift"), (3, "new", "rush")]


def test_a_control_table_at_version_0_gains_applied_lsn_and_snapshot_wave(delta_spark, workdir):
    from mssql_cdc import migrations, tables
    from mssql_cdc.finalization import CONTROL_COLUMNS
    from mssql_cdc.migrations.control import APPLIED_COLUMNS, OPEN_COMMENTS, WAVE_COLUMNS

    old = os.path.join(workdir, "control_v0")
    tables.create_if_not_exists(
        delta_spark,
        old,
        [c for c in CONTROL_COLUMNS if c not in APPLIED_COLUMNS + WAVE_COLUMNS],
        properties={migrations.SCHEMA_VERSION_PROPERTY: "0"},
    )
    assert migrations.migrate(delta_spark, old, "control") == 3
    schema = delta_spark.read.format("delta").load(old).schema
    assert schema["open_snapshot_lsn"].metadata["comment"] == OPEN_COMMENTS["open_snapshot_lsn"]
    types = {"applied_lsn": "string", "snapshot_lsn": "string", "open_snapshot_lsn": "string"}
    for name, data_type in {**types, "snapshot_wave": "int"}.items():
        assert (
            schema[name].dataType.simpleString() == data_type and schema[name].metadata["comment"]
        )


# Chunked snapshots: bronze and the facts written directly, as the stream and the backfill
# write them, at LSNs numbered by hand.
def _lsn(n):
    return "0x" + format(n, "020X")


def _ts(n):
    return T0 + timedelta(minutes=n)


def _ordered(rows):  # by key, the NULL key first
    return sorted(rows, key=lambda r: (r[0] is not None, r))


CHANGE = (
    "_capture_instance STRING, _start_lsn STRING, _seqval STRING, _operation INT, "
    "_command_id INT, _commit_ts TIMESTAMP_NTZ, _batch_id BIGINT"
)


class Log:
    """Bronze, facts and the bronze verdict for apply_changes, written row by row."""

    def __init__(self, spark, workdir, key_type="INT"):
        self.spark, self.key_type = spark, key_type
        self.bronze, self.silver, self.facts, self.control = (
            os.path.join(workdir, n) for n in ("bronze", "silver", "facts", "control")
        )

    def _append(self, rows, schema):
        (
            self.spark.createDataFrame(rows, f"order_id {self.key_type}, status STRING, {schema}")
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.bronze)
        )

    def change(self, n, *changes):
        """One transaction at LSN ``n``: (operation, key, status) in statement order. An
        update's 3 and 4 share _seqval and _command_id, as on SQL Server."""
        ids = [
            i - 1 if op == 4 and i > 1 and changes[i - 2][0] == 3 else i
            for i, (op, _, _) in enumerate(changes, start=1)
        ]
        rows = [
            (key, status, CI, _lsn(n), _lsn(n * 100 + j), op, j, _ts(n), 0)
            for j, (op, key, status) in zip(ids, changes)
        ]
        self._append(rows, CHANGE)

    def legacy_snapshot(self, n, image):
        """A snapshot as stream().to_delta wrote it before chunks: rows at its LSN alone."""
        self._append([(k, s, CI, _lsn(n), None, 0, None, _ts(n), None) for k, s in image], CHANGE)

    def wave(self, s, wave, stamp, chunks):
        """Wave ``wave`` of the chunked snapshot at ``s``, read at ``stamp``: one bronze
        commit, then a 'snapshot_chunk' row per (chunk, lo, hi, image)."""
        rows = [
            (k, st, CI, _lsn(stamp), None, 0, None, _ts(stamp), None, _lsn(s), chunk)
            for chunk, _, _, image in chunks
            for k, st in image
        ]
        if rows:
            self._append(rows, f"{CHANGE}, _snapshot STRING, _chunk INT")
        for chunk, lo, hi, image in chunks:
            detail = {"snapshot": _lsn(s), "chunk": chunk, "wave": wave, "lo": lo, "hi": hi}
            self.fact("snapshot_chunk", stamp, detail, rows=len(image), high=stamp + 1)

    def fact(self, event, n, detail=None, rows=0, high=None):
        from mssql_cdc import migrations
        from mssql_cdc.sink import _FACT_FIELDS, FACTS_COLUMNS, FACTS_COMMENT, FACTS_SCHEMA

        row = {
            "app_id": "orders-v1",
            "event": event,
            "rows": rows,
            "min_lsn": _lsn(n),
            "max_lsn": _lsn(high or n),
            "detail": json.dumps(detail, default=str) if detail else None,
            "target": self.bronze,
            "written_at": datetime(2026, 10, 2),
        }
        migrations.ensure(self.spark, self.facts, "facts", FACTS_COLUMNS, FACTS_COMMENT)
        df = self.spark.createDataFrame([tuple(row.get(k) for k in _FACT_FIELDS)], FACTS_SCHEMA)
        df.write.format("delta").mode("append").save(self.facts)

    def open(self, s, kind="bootstrap", keys=("order_id",)):
        """The 'snapshot_open' row, as CdcStream._open writes it."""
        generation = int(kind == "resnapshot")
        detail = {"mode": "chunked", "kind": kind, "keys": list(keys), "generation": generation}
        self.fact("snapshot_open", s, detail)

    def verdict(self, n):
        offset = {"lsn": _lsn(n), "commit_ts": _ts(n).isoformat(timespec="milliseconds")}
        return finalization.advance(self.spark, self.control, self.bronze, offset)

    def apply(self, facts=True, keys=("order_id",)):
        return apply_changes(
            self.spark,
            self.bronze,
            self.silver,
            CI,
            list(keys),
            control_table=self.control,
            facts_table=self.facts if facts else None,
        )

    def rows(self):
        rows = self.spark.read.format("delta").load(self.silver).collect()
        return _ordered((r["order_id"], r["status"]) for r in rows)

    def position(self):
        row = (
            self.spark.read.format("delta")
            .load(self.control)
            .where(f"table_name = '{self.silver}'")
            .select("applied_lsn", "snapshot_lsn", "open_snapshot_lsn", "snapshot_wave")
            .first()
        )
        return tuple(row)

    def commits(self):
        return self.spark.sql(f"DESCRIBE HISTORY delta.`{self.silver}`").count()


def test_an_open_bootstrap_applies_its_waves_without_resurrecting_deletes_and_holds_the_verdict(
    delta_spark, workdir
):
    from mssql_cdc.silver import _record

    g, S = Log(delta_spark, workdir), 100
    # the source at S: 1..8; the stream starts at S, the chunks are read later
    g.open(S)
    g.change(101, (1, 2, "new"))  # before chunk 0's stamp: chunk 0 has no 2
    g.change(102, (2, 10, "new"))  # after S: in the stream, and chunk 2 holds it too
    g.change(111, (1, 5, "new"))  # after chunk 1's stamp: it still has 5
    g.verdict(111)
    first = g.apply()  # no chunk yet: the changes alone
    assert g.rows() == [(10, "new")]
    assert first["finalized_until"] is None  # held, though bronze has a verdict
    assert g.position() == (_lsn(111), None, _lsn(S), None)

    # wave 0 lands after silver applied the delete of 5: its row must not bring 5 back
    chunk1 = [(4, "new"), (5, "new"), (6, "new")]
    g.wave(S, 0, 110, [(0, None, 4, [(1, "new"), (3, "new")]), (1, 4, 7, chunk1)])
    g.change(112, (3, 4, "new"), (4, 4, "paid"))  # an update: its 4 outranks its 3
    g.verdict(112)
    second = g.apply()
    assert g.rows() == [(1, "new"), (3, "new"), (4, "paid"), (6, "new"), (10, "new")]
    assert second["finalized_until"] is None and not second["rebuilt"]
    assert g.position() == (_lsn(112), None, _lsn(S), 0)

    g.wave(S, 1, 120, [(2, 7, None, [(7, "new"), (8, "new"), (10, "new")])])
    g.change(121, (1, 1, "new"))
    g.change(122, (3, 6, "new"), (4, 66, "new"))  # a key update as 3/4: 6 is gone
    g.verdict(122)
    g.apply()
    final = [(3, "new"), (4, "paid"), (7, "new"), (8, "new"), (10, "new"), (66, "new")]
    assert g.rows() == final and g.position() == (_lsn(122), None, _lsn(S), 1)

    commits = g.commits()
    assert g.apply()["applied_lsn"] == _lsn(122) and g.commits() == commits  # nothing new
    _record(delta_spark, g.control, g.silver, _lsn(122), None, _lsn(S), 0)  # a crash before it
    g.apply()  # wave 1 again
    assert g.rows() == final and g.position() == (_lsn(122), None, _lsn(S), 1)

    g.fact("bootstrap", S, {"snapshot": _lsn(S), "chunks": 3, "rows": 8, "last_lsn": _lsn(120)})
    released = g.verdict(122)
    done = g.apply()
    assert done["rebuilt"] and g.rows() == final
    assert done["finalized_until"] == released is not None
    assert g.position() == (_lsn(122), _lsn(S), None, None)
    assert not g.apply()["rebuilt"]


def _t(second, micro=0):
    return datetime(2026, 1, 1, 0, 0, second, micro)


# keys k1..k6, the bounds b1 and b2 of chunks [-, b1) [b1, b2) [b2, -), and a stale key no range
# may delete. A datetime2(7) bound has 100 ns digits; Spark keeps microseconds, so the keys of
# b1's microsecond fall on either side of it on SQL Server: left to the rebuild.
RANGES = {
    "INT": ([1, 2, 3, 4, 5, 6], 3, 5, None),
    "DATE": ([date(2026, 1, d) for d in range(1, 7)], "2026-01-03", "2026-01-05", None),
    "TIMESTAMP_NTZ": (
        [_t(1), _t(2), _t(3, 123457), _t(4), _t(5, 500000), _t(6)],
        "2026-01-01 00:00:03.1234567",
        "2026-01-01T00:00:05.0000000",
        _t(3, 123456),
    ),
    "STRING": (["a", "b", "c", "d", "e", "f"], "c", "e", None),  # Spark orders bytes, SQL not
}


@pytest.mark.parametrize(
    ("key_type", "mode"),
    [(k, "resnapshot") for k in RANGES] + [("INT", "bootstrap")],
)
def test_each_wave_deletes_the_stale_keys_of_its_ranges_and_completion_any_absent_key(
    delta_spark, workdir, key_type, mode
):
    from mssql_cdc.silver import _record

    g, S = Log(delta_spark, workdir, key_type), 100
    (k1, k2, k3, k4, k5, k6), b1, b2, odd = RANGES[key_type]
    by_range = key_type != "STRING"
    # silver from changes whose history lost the deletes of 2, 6, the NULL key and the odd one
    stale = [k2, k6, None] + ([odd] if odd else [])
    g.change(10, *[(2, k, "new") for k in [k1, k3, k4, k5, *stale]])
    g.apply()
    g.open(S, mode)
    g.change(112, (1, k4, "new"))
    g.change(113, (2, k4, "again"))  # chunk 1 read in between: no k4; this outranks its delete
    g.wave(S, 0, 110, [(0, None, b1, [(k1, "snap")]), (1, b1, b2, [(k3, "snap")])])
    g.verdict(113)
    first = g.apply()
    live = [(k1, "snap"), (k3, "snap"), (k4, "again")]
    # k5 and k6 wait for chunk 2; the NULL key sorts first, but in no range unless one is whole
    gone = [k2] if by_range else []
    assert g.rows() == _ordered(live + [(k5, "new")] + [(k, "new") for k in stale if k not in gone])
    assert first["finalized_until"] is None  # held while the snapshot is open
    at, rows = g.position(), g.rows()
    _record(delta_spark, g.control, g.silver, at[0], at[1], _lsn(S), None)  # a crash before it
    assert g.apply()["applied_lsn"] == at[0] and g.rows() == rows and g.position() == at

    g.wave(S, 1, 120, [(2, b2, None, [(k5, "snap")])])
    g.apply()
    gone += [k6] if by_range else []
    assert g.rows() == _ordered(
        live + [(k5, "snap")] + [(k, "new") for k in stale if k not in gone]
    )
    g.fact(mode, S, {"snapshot": _lsn(S)})
    done = g.apply()
    assert done["rebuilt"] and done["finalized_until"] is not None
    assert g.rows() == _ordered(live + [(k5, "snap")])  # absent from both: deleted


def test_an_integer_plans_last_bound_past_bigint_still_deletes_up_to_the_last_key(
    delta_spark, workdir
):
    g, S, top = Log(delta_spark, workdir, "BIGINT"), 100, 2**63 - 1
    g.change(10, (2, 1, "new"), (2, top, "new"))
    g.apply()
    g.open(S, "resnapshot")
    g.wave(S, 0, 110, [(0, None, top + 1, [(1, "snap")])])  # plan_chunks ends at MAX + 1
    g.apply()
    assert g.rows() == [(1, "snap")]


@pytest.mark.parametrize(
    ("keys", "cut"),
    [(["order_id", "status"], ["order_id", "status"]), (["order_id"], ["status"])],
)
def test_no_range_deletes_unless_silver_has_the_snapshots_one_column_key(
    delta_spark, workdir, keys, cut
):
    g, S = Log(delta_spark, workdir), 100
    g.change(10, (2, 1, "new"), (2, 2, "new"))  # 2's delete is lost in a purged gap
    g.apply(keys=keys)
    g.open(S, "resnapshot", keys=cut)  # its bounds are not cut on silver's one key column
    g.wave(S, 0, 110, [(0, None, None, [(1, "snap")])])  # a whole-table range without 2
    g.apply(keys=keys)
    assert (2, "new") in g.rows()  # left to the rebuild
    g.fact("resnapshot", S, {"snapshot": _lsn(S)})
    g.apply(keys=keys)
    assert g.rows() == [(1, "snap")]


def test_a_legacy_snapshot_rebuilds_and_an_open_resnapshot_holds_the_verdict_until_complete(
    delta_spark, workdir
):
    g = Log(delta_spark, workdir)
    g.legacy_snapshot(10, [(1, "new"), (2, "new"), (3, "new")])  # no _snapshot column yet
    g.fact("bootstrap", 10)
    before = g.verdict(11)
    first = g.apply()
    assert first["rebuilt"] and first["finalized_until"] == before
    assert g.rows() == [(1, "new"), (2, "new"), (3, "new")]

    g.change(12, (2, 4, "new"))
    # 2 deleted in a purged gap; the re-snapshot at 50 opens and the stream goes on from it
    g.open(50, "resnapshot")
    g.change(51, (3, 3, "new"), (4, 3, "paid"))
    g.wave(50, 0, 60, [(0, None, None, [(1, "new"), (3, "paid"), (4, "new")])])
    g.verdict(61)
    with pytest.raises(ValueError, match="pass facts_table"):
        g.apply(facts=False)
    held = g.apply()
    # the chunk rows are no complete snapshot yet: the legacy rows (NULL _snapshot now) still
    # are the newest one. The wave applies, and 2, in its range (the whole table) without a
    # row and older than its stamp, goes before the completion
    assert not held["rebuilt"] and held["finalized_until"] == before
    assert g.rows() == [(1, "new"), (3, "paid"), (4, "new")]
    assert g.position() == (_lsn(51), _lsn(10), _lsn(50), 0)

    g.fact("resnapshot", 50, {"snapshot": _lsn(50)})
    released = g.verdict(62)
    done = g.apply()
    assert done["rebuilt"] and done["finalized_until"] == released > before
    assert g.rows() == [(1, "new"), (3, "paid"), (4, "new")]
    assert g.position() == (_lsn(51), _lsn(50), None, None)
