"""reconcile: silver against its SQL Server table, by key range (Tier 1) and row by row (Tier 2)."""

import json
import os
from datetime import date, datetime, timedelta
from decimal import Decimal as D

import pytest

from mssql_cdc import apply_changes, reconcile, stream
from mssql_cdc.fake import FakeCdcClient, FakeCdcDatabase

CI = "dbo_orders"
T0 = datetime(2026, 10, 1, 9, 0)


class Orders:
    """A keyed fake whose captured columns the stream infers, bronze, and silver."""

    def __init__(
        self, spark, workdir, key="order_id", columns="order_id INT, status STRING", computed=None
    ):
        self.spark, self.key, self.src = spark, key, os.path.join(workdir, "src")
        self.db = FakeCdcDatabase(
            self.src, [CI], keys={CI: key}, columns={CI: columns}, computed=computed
        )
        self.options = {"backend": "fake", "fakePath": self.src, "captureInstance": CI}
        self.bronze, self.silver, self.control, self.ckpt, self.report = (
            os.path.join(workdir, n) for n in ("bronze", "silver", "control", "ckpt", "report")
        )
        self.at = T0

    def commit(self, *changes):
        self.at += timedelta(minutes=1)
        return self.db.commit(CI, list(changes), at=self.at)

    def stream(self):
        q = stream(self.spark, self.options).to_delta(
            self.bronze, "orders-v1", self.ckpt, trigger={"availableNow": True}
        )
        q.awaitTermination()

    def apply(self):
        apply_changes(
            self.spark,
            self.bronze,
            self.silver,
            capture_instance=CI,
            keys=[self.key],
            control_table=self.control,
        )

    def reconcile(self, **kw):
        kw = {"bucket_rows": 10, "seed": 7, "report_table": self.report, **kw}
        return reconcile(
            self.spark,
            self.options,
            self.silver,
            bronze=self.bronze,
            control_table=self.control,
            **kw,
        )

    def failures(self, result):
        rows = result["report"].where("key IS NOT NULL").collect()
        return {r["key"]: (r["failure_type"], r["bucket_lo"], r["bucket_hi"], r) for r in rows}

    def silver_table(self):
        from delta.tables import DeltaTable

        return DeltaTable.forPath(self.spark, self.silver)


def _orders(o, n=100):
    o.commit(*[(2, {"order_id": i, "status": "new"}) for i in range(n // 2)])
    o.commit(*[(2, {"order_id": i, "status": "new"}) for i in range(n // 2, n)])
    o.stream()
    o.apply()


def test_equal_tables_match_into_a_typed_report_then_changes_not_applied_are_classified(
    delta_spark, workdir
):
    o = Orders(delta_spark, workdir)
    _orders(o)
    result = o.reconcile(sample=1.0)
    # keys 0..99 in buckets of 10 rows, every one also compared row by row
    assert (result["buckets"], result["match"], result["hashed"]) == (10, 10, 10)
    assert result["failures"] == {} and result["source_lsn"] >= result["silver_lsn"]

    report = delta_spark.read.format("delta").load(o.report)
    rows = report.where(f"run_id = '{result['run_id']}'").orderBy("source_key_sum").collect()
    assert [(r["bucket_lo"], r["bucket_hi"], r["status"]) for r in rows[:2]] == [
        ("0", "10", "MATCH"),
        ("10", "20", "MATCH"),
    ]
    first = rows[0]
    assert (
        (first["source_rows"], first["source_key_sum"])
        == (10, 45)
        == (
            first["silver_rows"],
            first["silver_key_sum"],
        )
    )
    assert first["hashed"] and first["key"] is None and first["silver"] == o.silver
    fields = {f.name: f for f in report.schema}
    assert fields["source_key_sum"].dataType.simpleString() == "decimal(38,0)"
    assert fields["run_at"].dataType.simpleString() == "timestamp_ntz"
    assert all(f.metadata.get("comment") for f in fields.values())
    detail = delta_spark.sql(f"DESCRIBE DETAIL delta.`{o.report}`").first()
    assert detail["description"].startswith("Report of mssql-cdc-pyspark's reconcile()")
    assert detail["properties"]["mssql_cdc.schema_version"] == "0"

    again = o.reconcile(sample=0.0, report_table=None)  # no sample: nothing read row by row
    assert (again["match"], again["hashed"]) == (10, 0) and again["run_id"] != result["run_id"]
    assert report.count() == 10  # appended only when asked

    silver = o.silver_table()
    silver.delete("order_id = 3")  # an insert silver never got
    silver.update("order_id = 15", {"status": "'lost'"})  # an update it missed
    stale = delta_spark.read.format("delta").load(o.silver).where("order_id = 5")
    stale.selectExpr("1000 AS order_id", "status", "_start_lsn", "_commit_ts").write.format(
        "delta"
    ).mode("append").save(o.silver)  # a delete it missed, past the source's last key

    counted = o.reconcile(sample=0.0)  # the counts find the first two; equal counts hide 15
    assert (counted["mismatch"], counted["match"], counted["hashed"]) == (2, 9, 2)
    found = o.failures(counted)
    assert {k: v[:3] for k, v in found.items()} == {
        '{"order_id":3}': ("MISSING_TARGET", "0", "10"),
        '{"order_id":1000}': ("MISSING_SOURCE", "1000", "1001"),
    }
    assert json.loads(found['{"order_id":1000}'][3]["detail"])["silver_start_lsn"].startswith("0x")
    buckets = counted["report"].where("key IS NULL AND status = 'MISMATCH'").collect()
    assert sorted((r["source_rows"], r["silver_rows"]) for r in buckets) == [(0, 1), (10, 9)]

    every = o.reconcile(sample=1.0)
    found = o.failures(every)
    assert found['{"order_id":15}'][:3] == ("RECORD_DIFF", "10", "20")
    assert json.loads(found['{"order_id":15}'][3]["detail"])["columns"] == "status"
    assert every["failures"] == {"MISSING_TARGET": 1, "MISSING_SOURCE": 1, "RECORD_DIFF": 1}


def test_a_computed_column_listed_in_columns_is_not_compared(delta_spark, workdir):
    declared = "order_id INT, status STRING, total INT"
    o = Orders(delta_spark, workdir, columns=declared, computed={CI: ["total"]})
    o.options["columns"] = declared  # kept in the schema, NULL in every row
    o.commit(*[(2, {"order_id": i, "status": "new", "total": i}) for i in range(20)])
    o.stream()
    o.apply()
    # silver as an older version left it: the values its snapshot read on keys not changed since
    o.silver_table().update("order_id < 5", {"total": "1"})
    result = o.reconcile(sample=1.0)
    assert result["failures"] == {} and result["match"] == result["buckets"] == 2


def test_differences_bronze_explains_are_in_flight(delta_spark, workdir, monkeypatch):
    o = Orders(delta_spark, workdir)
    _orders(o)
    # silver behind bronze: changes the stream read and apply_changes has not applied yet
    o.commit((2, {"order_id": 100, "status": "new"}), (1, {"order_id": 30, "status": "new"}))
    o.commit((3, {"order_id": 20, "status": "new"}), (4, {"order_id": 20, "status": "paid"}))
    o.stream()
    behind = o.reconcile(sample=1.0)
    statuses = {
        r["bucket_lo"]: r["status"] for r in behind["report"].where("key IS NULL").collect()
    }
    assert statuses["100"] == statuses["30"] == "IN_FLIGHT"  # counts differ
    assert statuses["20"] == "MATCH" and behind["mismatch"] == 0  # an update keeps them
    assert behind["failures"] == {"IN_FLIGHT": 1}  # 20, found row by row
    assert o.failures(behind)['{"order_id":20}'][0] == "IN_FLIGHT"

    o.apply()
    equal = o.reconcile(sample=0.0)  # applied: equal again
    assert equal["match"] == equal["buckets"] == 10  # 30 is gone: 100 keys

    # a commit after max_lsn was read, which the count sees and silver has not applied
    real, raced = FakeCdcClient.key_buckets, []

    def racing(self, *args):
        if not raced:
            raced.append(o.commit((2, {"order_id": 200, "status": "new"})))
            o.stream()
        return real(self, *args)

    monkeypatch.setattr(FakeCdcClient, "key_buckets", racing)
    after = o.reconcile(sample=0.0)
    assert raced[0] > after["source_lsn"] and (after["in_flight"], after["mismatch"]) == (1, 0)


def test_changes_the_stream_has_not_read_yet_are_in_flight_and_a_real_difference_is_not(
    delta_spark, workdir, monkeypatch
):
    o = Orders(delta_spark, workdir)
    _orders(o)
    # the source moved past what the stream read: only the change table holds these
    o.commit((2, {"order_id": 100, "status": "new"}), (1, {"order_id": 30, "status": "new"}))
    o.commit((3, {"order_id": 20, "status": "new"}), (4, {"order_id": 20, "status": "paid"}))
    lagging = o.reconcile(sample=1.0)
    statuses = {
        r["bucket_lo"]: r["status"] for r in lagging["report"].where("key IS NULL").collect()
    }
    assert statuses["100"] == statuses["30"] == "IN_FLIGHT" and lagging["mismatch"] == 0
    assert {k: v[0] for k, v in o.failures(lagging).items()} == {'{"order_id":20}': "IN_FLIGHT"}

    # bronze holds some, silver none, the change table one more; and a real difference
    o.stream()
    o.commit((1, {"order_id": 55, "status": "new"}))
    o.silver_table().update("order_id = 45", {"status": "'lost'"})  # an update it missed
    mixed = o.reconcile(sample=1.0)
    statuses = {r["bucket_lo"]: r["status"] for r in mixed["report"].where("key IS NULL").collect()}
    assert statuses["50"] == "IN_FLIGHT" and mixed["mismatch"] == 0  # 55, the change table's
    assert {k: v[0] for k, v in o.failures(mixed).items()} == {
        '{"order_id":20}': "IN_FLIGHT",
        '{"order_id":45}': "RECORD_DIFF",
    }

    o.stream()
    o.apply()
    o.silver_table().update("order_id = 45", {"status": "'new'"})
    equal = o.reconcile(sample=0.0)  # applied: equal again
    assert equal["match"] == equal["buckets"]
    # a commit after max_lsn was read, which the count sees: read up to max_lsn after it
    real, raced = FakeCdcClient.key_buckets, []

    def racing(self, *args):
        if not raced:
            raced.append(o.commit((2, {"order_id": 200, "status": "new"})))  # no stream
        return real(self, *args)

    monkeypatch.setattr(FakeCdcClient, "key_buckets", racing)
    after = o.reconcile(sample=0.0)
    assert raced[0] > after["source_lsn"] and (after["in_flight"], after["mismatch"]) == (1, 0)


def test_a_snapshot_above_the_streams_position_leaves_the_changes_below_it_in_flight(
    delta_spark, workdir
):
    # snapshot_on_switch's: a whole snapshot at max_lsn after the batch, while the stream's
    # checkpoint stays at the batch's end. The insert in between is in neither bronze's
    # changes nor what the stream has read
    o = Orders(delta_spark, workdir)
    _orders(o)
    o.commit((2, {"order_id": 100, "status": "new"}))
    stream(delta_spark, o.options)._take_snapshot(o.bronze, CI)  # at the insert's LSN
    found = o.reconcile(sample=1.0)
    assert (found["mismatch"], found["in_flight"], found["failures"]) == (0, 1, {})


def test_unread_keys_are_each_instances_changes_after_bronze_up_to_max_lsn(workdir):
    import pyarrow as pa

    from mssql_cdc.reconcile import _unread

    db = FakeCdcDatabase(
        workdir, [CI], keys={CI: "order_id"}, columns={CI: "order_id INT, status STRING"}
    )
    read = db.commit(CI, [(2, {"order_id": 1, "status": "new"})])  # bronze's newest
    db.commit(CI, [(2, {"order_id": None, "status": "new"}), (2, {"order_id": 2, "status": "a"})])
    v2 = db.add_capture_instance(CI, "order_id INT, status STRING")  # takes over at S
    s = db.commit(CI, [(3, {"order_id": 2, "status": "a"}), (4, {"order_id": 2, "status": "b"})])
    upper = db.commit(CI, [(1, {"order_id": 3, "status": "new"})])  # max_lsn, read after
    db.commit(CI, [(2, {"order_id": 4, "status": "new"})])  # after it: the next run's
    client, pieces = FakeCdcClient(workdir), []
    iter_changes = client.iter_changes

    def spy(ci, lo, hi, *args):
        pieces.append((ci, lo, hi))
        return iter_changes(ci, lo, hi, *args)

    client.iter_changes = spy
    target = pa.schema([("order_id", pa.int32())])  # cast to bronze's type
    instances = client.capture_instances(CI)
    assert _unread(client, instances, ["order_id"], read, upper, target) == {(None,), (2,), (3,)}
    # the older instance below S, the newer from S, as the stream reads them
    after = client.increment_lsn(read)
    assert pieces == [(CI, after, client.decrement_lsn(s)), (v2, s, upper)]
    pieces.clear()
    assert _unread(client, instances, ["order_id"], upper, upper, target) == set()
    assert pieces == []  # nothing after bronze: no read


def test_a_null_key_is_compared_and_named_and_its_change_in_flight(delta_spark, workdir):
    o = Orders(delta_spark, workdir, key="code", columns="code STRING, qty INT")
    o.commit(*[(2, {"code": f"k{i}", "qty": i}) for i in range(5)], (2, {"code": None, "qty": 0}))
    o.stream()
    o.apply()
    assert o.reconcile(sample=1.0)["failures"] == {}
    # bronze holds an update of the NULL key that silver has not applied
    o.commit((3, {"code": None, "qty": 0}), (4, {"code": None, "qty": 9}))
    o.stream()
    behind = o.reconcile(sample=1.0)
    assert {k: v[0] for k, v in o.failures(behind).items()} == {'{"code":null}': "IN_FLIGHT"}
    o.apply()
    o.silver_table().update("code IS NULL", {"qty": "-1"})  # an update it missed
    assert {k: v[0] for k, v in o.failures(o.reconcile(sample=1.0)).items()} == {
        '{"code":null}': "RECORD_DIFF"
    }


def test_a_string_key_is_counted_whole_and_compared_from_the_source_rows(delta_spark, workdir):
    o = Orders(delta_spark, workdir, key="code", columns="code STRING, qty INT")
    o.commit(*[(2, {"code": f"k{i:02d}", "qty": i}) for i in range(30)])
    o.stream()
    o.apply()
    silver = o.silver_table()
    silver.delete("code = 'k07'")
    silver.update("code = 'k21'", {"qty": "-1"})
    result = o.reconcile(sample=1.0)
    whole = result["report"].where("key IS NULL").collect()
    assert [(r["bucket_lo"], r["source_rows"], r["silver_rows"], r["status"]) for r in whole] == [
        (None, 30, 29, "MISMATCH")
    ]
    assert {k: v[0] for k, v in o.failures(result).items()} == {
        '{"code":"k07"}': "MISSING_TARGET",
        '{"code":"k21"}': "RECORD_DIFF",
    }
    # keyed on another column than the capture instance's index: ranges are cut on it
    by_qty = o.reconcile(sample=1.0, keys=["qty"])
    assert {k: v[0] for k, v in o.failures(by_qty).items()} == {
        '{"qty":7}': "MISSING_TARGET",
        '{"qty":21}': "MISSING_TARGET",
        '{"qty":-1}': "MISSING_SOURCE",
    }
    with pytest.raises(ValueError, match=r"keys \['nope'\] are not captured columns"):
        o.reconcile(keys=["nope"])


@pytest.mark.parametrize("bad", [{"bucket_rows": 0}, {"sample": 1.5}, {"sample": -0.1}])
def test_reconcile_rejects_buckets_below_one_row_and_a_sample_outside_0_to_1(bad):
    # checked before any Spark call: no session is needed to reach it
    with pytest.raises(ValueError, match="bucket_rows must be at least 1, and sample between"):
        reconcile(None, {}, "silver", bronze="bronze", control_table="control", **bad)


def test_a_report_row_with_a_key_that_is_no_column_fails_rather_than_write_null():
    from mssql_cdc.reconcile import _report_row

    with pytest.raises(ValueError, match=r"\['source_row'\]"):
        _report_row({"run_id": "r", "source_row": 3})  # a typo of source_rows
    assert _report_row({"run_id": "r", "source_rows": 3})[:1] == ("r",)


def test_a_date_buckets_ends_past_the_date_range_stay_in_it():
    from mssql_cdc.reconcile import _bound

    top = (date.max - date(1970, 1, 1)).days  # 9999-12-31, a sentinel's key
    assert (_bound("date", top), _bound("date", top + 1)) == ("9999-12-31", None)
    assert (_bound("date", -(10**7)), _bound("date", 0), _bound("int", -3)) == (
        "0001-01-01",
        "1970-01-01",
        -3,
    )


def _bucket(lo, hi, fine, source, silver, moved=False):
    return {"lo": lo, "hi": hi, "fine": fine, "moved": moved, "source": source, "silver": silver}


@pytest.mark.parametrize(
    "source, silver, moved, width, expected",
    [
        pytest.param(
            # fine bucket 2 is silver's only, 1 has more rows in the source: a bucket closes at
            # bucket_rows of the larger side (6 + 3 + 2), the 25 rows of 3 stay one bucket, and
            # the sparse tail 5..9 is one bucket up to the last fine id
            {0: (6, D(10)), 1: (3, D(15)), 3: (25, D(800)), 5: (1, D(55)), 9: (2, D(190))},
            {0: (6, D(10)), 1: (1, D(5)), 2: (2, D(41)), 3: (25, D(800)), 5: (1, D(55))},
            [],
            10,
            [
                _bucket(0, 30, [0, 1, 2], [9, D(25)], [9, D(56)]),
                _bucket(30, 50, [3], [25, D(800)], [25, D(800)]),
                _bucket(50, 100, [5, 9], [3, D(245)], [1, D(55)]),
            ],
            id="skewed",
        ),
        pytest.param(
            {0: (2, D(1)), 4: (3, D(14))},
            {},
            [],
            1,
            [_bucket(0, 5, [0, 4], [5, D(15)], [0, D(0)])],
            id="silver empty",
        ),
        pytest.param(
            {},
            {7: (4, D(30))},
            [],
            1,
            [_bucket(7, 8, [7], [0, D(0)], [4, D(30)])],
            id="source empty",
        ),
        pytest.param(
            # starts [2, 4], end 6: a fine id below the first or at the end is no bucket's,
            # and a NULL key has none
            {2: (5, D(10)), 3: (5, D(15)), 4: (5, D(20)), 5: (5, D(25))},
            {2: (5, D(10)), 3: (5, D(15)), 4: (5, D(20)), 5: (5, D(25))},
            [(None,), (1,), (3,), (6,)],
            1,
            [
                _bucket(2, 4, [2, 3], [10, D(25)], [10, D(25)], moved=True),
                _bucket(4, 6, [4, 5], [10, D(45)], [10, D(45)]),
            ],
            id="moved",
        ),
    ],
)
def test_fine_buckets_merge_by_the_larger_side_and_a_change_moves_only_its_bucket(
    source, silver, moved, width, expected
):
    from mssql_cdc.reconcile import _merge_buckets

    sides = {"source": source, "silver": silver}
    assert _merge_buckets(sides, moved, width, bucket_rows=10) == expected


def test_a_sample_rounds_up_and_takes_none_at_0_and_all_at_1():
    import random

    from mssql_cdc.reconcile import _sample

    rng, items = random.Random(7), list(range(10))
    assert _sample(rng, items, 0) == [] and sorted(_sample(rng, items, 1)) == items
    some = _sample(rng, items, 0.25)  # 2.5 buckets: 3
    assert len(set(some)) == 3 and set(some) <= set(items)
    assert _sample(rng, [4], 0.01) == [4]  # any sample of a bucket compares one


S, BELOW, ABOVE = (f"0x{n:020X}" for n in (16, 5, 32))


def _c(i, lo, hi, last=False, rows=3, lsn=ABOVE):
    """A 'snapshot_chunk' facts row's detail, with its rows and min_lsn, as _chunk_checks has it."""
    detail = {"snapshot": S, "chunk": i, "wave": 0, "lo": lo, "hi": hi, "last": last}
    return detail | {"rows": rows, "lsn": lsn}


def _fail(kind, i, lo, hi, **what):
    detail = {"snapshot": S, "chunk": i, **what}
    return {"failure_type": kind, "bucket_lo": lo, "bucket_hi": hi, "detail": detail}


TILED = [_c(0, None, 3), _c(1, 3, 6), _c(2, 6, None, last=True)]
HELD = {i: (3, ABOVE) for i in range(3)}


@pytest.mark.parametrize(
    "complete, found, held, expected",
    [
        pytest.param(True, TILED, HELD, [], id="tiled"),
        pytest.param(False, TILED[:2], {0: HELD[0], 1: HELD[1]}, [], id="open: more to come"),
        pytest.param(
            True,
            [_c(i, c["lo"], c["hi"], c["last"], lsn=None) for i, c in enumerate(TILED)],
            {i: (3, None) for i in range(3)},
            [],
            id="no stamp: S",
        ),
        pytest.param(
            True,
            [TILED[0], TILED[2]],
            {0: HELD[0], 2: HELD[2]},
            [_fail("CHUNK_TILING", 1, None, None, problem="no 'snapshot_chunk' facts row")],
            id="a gap",
        ),
        pytest.param(
            True,
            [*TILED, TILED[1]],
            HELD,
            [_fail("CHUNK_TILING", 1, "3", "6", problem="2 'snapshot_chunk' facts rows")],
            id="a chunk twice",
        ),
        pytest.param(
            True,
            [_c(0, 0, 3), *TILED[1:]],
            HELD,
            [_fail("CHUNK_TILING", 0, "0", "3", problem="the first chunk is not open below")],
            id="first not open",
        ),
        pytest.param(
            True,
            [TILED[0], _c(1, 2, 6), TILED[2]],
            HELD,
            [
                _fail(
                    "CHUNK_TILING",
                    1,
                    "2",
                    "6",
                    problem="it does not start where the one before ended",
                )
            ],
            id="an overlap",
        ),
        pytest.param(
            True,
            [TILED[0], _c(1, 3, 6, last=True), TILED[2]],
            HELD,
            [
                _fail(
                    "CHUNK_TILING",
                    2,
                    "6",
                    None,
                    problem="it does not start where the one before ended",
                )
            ],
            id="a chunk after the last",
        ),
        pytest.param(
            True,
            [*TILED[:2], _c(2, 6, None)],
            HELD,
            [_fail("CHUNK_TILING", 2, "6", None, problem="the last chunk is not the plan's final")],
            id="complete, the last not final",
        ),
        pytest.param(
            False, [*TILED[:2], _c(2, 6, None)], HELD, [], id="open, the last not final yet"
        ),
        pytest.param(
            True,
            TILED,
            {**HELD, 3: (2, ABOVE)},
            [_fail("CHUNK_ROWS", 3, None, None, facts_rows=None, bronze_rows=2)],
            id="complete, bronze holds a chunk with no facts row",
        ),
        pytest.param(
            False,
            TILED[:2],
            {**HELD, 3: (2, ABOVE)},
            [],
            id="open, a wave's facts not written yet",
        ),
        pytest.param(
            True,
            [_c(0, None, 3, lsn=BELOW), *TILED[1:]],
            HELD,
            [_fail("CHUNK_STAMP", 0, None, "3", lsn=BELOW)],
            id="facts stamped below S",
        ),
        pytest.param(
            True,
            TILED,
            {**HELD, 1: (3, BELOW)},
            [_fail("CHUNK_STAMP", 1, "3", "6", lsn=BELOW)],
            id="bronze stamped below S",
        ),
        pytest.param(
            True,
            [TILED[0], _c(1, 3, 6, rows=4), TILED[2]],
            {**HELD, 2: (1, ABOVE)},
            [
                _fail("CHUNK_ROWS", 1, "3", "6", facts_rows=4, bronze_rows=3),
                _fail("CHUNK_ROWS", 2, "6", None, facts_rows=3, bronze_rows=1),
            ],
            id="rows bronze does not hold",
        ),
        pytest.param(
            True,
            TILED,
            {0: HELD[0], 1: HELD[1]},
            [_fail("CHUNK_ROWS", 2, "6", None, facts_rows=3, bronze_rows=0)],
            id="a chunk bronze lacks",
        ),
    ],
)
def test_chunk_checks_find_every_gap_overlap_count_and_stamp(complete, found, held, expected):
    from mssql_cdc.reconcile import _tiling_failures

    got = _tiling_failures(S, complete, found, held)
    assert [r | {"detail": json.loads(r["detail"])} for r in got] == expected


def test_a_chunked_bootstrap_applied_by_wave_reconciles_through_snapshot_chunks(
    delta_spark, workdir
):
    # the parts together (ADR 0028): to_delta opens the snapshot and streams from S, backfill
    # reads it in waves, apply_changes applies them and rebuilds at completion, and reconcile
    # reads the source back with snapshotChunks
    o = Orders(delta_spark, workdir)
    o.options["numPartitions"] = "2"  # two chunks per wave
    o.commit(*[(2, {"order_id": i, "status": "new"}) for i in range(12)])
    facts = os.path.join(workdir, "facts")
    cdc = stream(delta_spark, o.options)

    def run():
        cdc.to_delta(
            o.bronze,
            "orders-v1",
            o.ckpt,
            facts,
            trigger={"availableNow": True},
            bootstrap=True,
            snapshot="chunked",
        ).awaitTermination()

    def apply():
        result = apply_changes(
            delta_spark,
            o.bronze,
            o.silver,
            capture_instance=CI,
            keys=[o.key],
            control_table=o.control,
            facts_table=facts,
        )
        keys = sorted(r[o.key] for r in delta_spark.read.format("delta").load(o.silver).collect())
        return result, keys

    run()  # S: the key spans 0..11; the first backfill() plans chunks of 3 keys
    o.commit((1, {"order_id": 3, "status": "new"}), (2, {"order_id": 20, "status": "new"}))
    o.at += timedelta(minutes=1)  # after the wave's stamp, before its read
    o.db.commit_before_read(CI, [(1, {"order_id": 1, "status": "new"})], at=o.at)
    backfill = {"app_id": "orders-v1", "facts_table": facts, "chunk_rows": 3}
    assert cdc.backfill(o.bronze, max_waves=1, **backfill)["chunks_done"] == 2
    run()
    # wave 0's chunks [-, 4) and [4, 7), planned after 3 was deleted, and the changes
    _, keys = apply()
    assert keys == [0, 2, 4, 5, 6, 20]
    assert cdc.backfill(o.bronze, **backfill)["done"]
    run()
    done, keys = apply()
    assert done["rebuilt"] and keys == [0, 2, *range(4, 12), 20]

    result = o.reconcile(sample=1.0)
    assert (result["mismatch"], result["failures"]) == (0, {})
    assert result["hashed"] == result["buckets"] == result["match"] > 0
    assert result["silver_lsn"] == done["applied_lsn"]  # E: what silver applied, no stamp


def test_chunk_checks_read_a_pipeline_built_snapshot_whose_rows_are_no_change_in_flight(
    delta_spark, workdir
):
    # every failure _tiling_failures finds is in its table above; here, the facts and bronze
    # fields it gets through the reads, and the bounds the report writes
    from delta.tables import DeltaTable

    o = Orders(delta_spark, workdir)
    o.options["numPartitions"] = "2"
    o.commit(*[(2, {"order_id": i, "status": "new"}) for i in range(12)])
    facts = os.path.join(workdir, "facts")
    cdc = stream(delta_spark, o.options)
    cdc.to_delta(
        o.bronze,
        "orders-v1",
        o.ckpt,
        facts,
        trigger={"availableNow": True},
        bootstrap=True,
        snapshot="chunked",
    ).awaitTermination()
    o.db.idle(at=o.at + timedelta(minutes=1))  # the chunks' stamp, above S
    status = cdc.backfill(o.bronze, app_id="orders-v1", facts_table=facts, chunk_rows=3)
    assert status["done"] and status["chunks_done"] == 4  # [-, 3) [3, 6) [6, 9) [9, 12)
    apply_changes(
        delta_spark,
        o.bronze,
        o.silver,
        capture_instance=CI,
        keys=[o.key],
        control_table=o.control,
        facts_table=facts,
    )

    def chunk_rows(result):
        found = result["report"].where("failure_type LIKE 'CHUNK%'").collect()
        return {(r["failure_type"], json.loads(r["detail"])["chunk"]): r for r in found}

    clean = o.reconcile(sample=0.0, facts_table=facts)
    assert clean["failures"] == {} and chunk_rows(clean) == {}
    table = DeltaTable.forPath(delta_spark, facts)

    def chunk(i):
        return f"event = 'snapshot_chunk' AND get_json_object(detail, '$.chunk') = {i}"

    table.update(chunk(0), {"min_lsn": "'0x00000000000000000000'"})  # stamped below S
    table.delete(chunk(1))  # a gap, and bronze holds 3 rows of a chunk with no facts row
    table.update(chunk(3), {"detail": "replace(detail, '\"lo\": 9', '\"lo\": 8')"})  # overlap
    # silver applied up to S, its rows are stamped above it: a snapshot row is no change in
    # flight, so a row silver lost is a MISMATCH
    o.silver_table().delete("order_id = 4")
    found = o.reconcile(sample=0.0, facts_table=facts)
    rows = chunk_rows(found)
    assert set(rows) == {
        ("CHUNK_STAMP", 0),
        ("CHUNK_TILING", 1),
        ("CHUNK_ROWS", 1),
        ("CHUNK_TILING", 3),
    }
    assert found["failures"] == {
        "CHUNK_STAMP": 1,
        "CHUNK_TILING": 2,
        "CHUNK_ROWS": 1,
        "MISSING_TARGET": 1,
    }
    assert (found["mismatch"], found["in_flight"]) == (1, 0)
    assert json.loads(rows[("CHUNK_STAMP", 0)]["detail"])["lsn"] == "0x00000000000000000000"
    assert json.loads(rows[("CHUNK_ROWS", 1)]["detail"]) | {"snapshot": None} == {
        "snapshot": None,
        "chunk": 1,
        "facts_rows": None,
        "bronze_rows": 3,
    }
    assert found["match"] == found["buckets"] - 1  # the chunks' failures are no bucket's
    written = delta_spark.read.format("delta").load(o.report)
    written = chunk_rows({"report": written.where(f"run_id = '{found['run_id']}'")})
    assert {
        k: (r["bucket_lo"], r["bucket_hi"], r["key"], r["status"]) for k, r in written.items()
    } == {
        ("CHUNK_STAMP", 0): (None, "3", None, None),
        ("CHUNK_TILING", 1): (None, None, None, None),
        ("CHUNK_ROWS", 1): (None, None, None, None),
        ("CHUNK_TILING", 3): ("8", "12", None, None),
    }
