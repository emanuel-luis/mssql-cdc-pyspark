"""reconcile: silver against its SQL Server table, by key range (Tier 1) and row by row (Tier 2)."""

import json
import os
from datetime import datetime, timedelta

import pytest

from mssql_cdc import apply_changes, reconcile, stream
from mssql_cdc.fake import FakeCdcClient, FakeCdcDatabase

pytestmark = pytest.mark.delta
CI = "dbo_orders"
T0 = datetime(2026, 10, 1, 9, 0)


class Orders:
    """A keyed fake whose captured columns the stream infers, bronze, and silver."""

    def __init__(self, spark, workdir, key="order_id", columns="order_id INT, status STRING"):
        self.spark, self.key, self.src = spark, key, os.path.join(workdir, "src")
        self.db = FakeCdcDatabase(self.src, [CI], keys={CI: key}, columns={CI: columns})
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
            self.spark, self.bronze, self.silver, CI, [self.key], control_table=self.control
        )

    def reconcile(self, **kw):
        kw = {"bucket_rows": 10, "seed": 7, "report_table": self.report, **kw}
        return reconcile(self.spark, self.options, self.silver, bronze=self.bronze, **kw)

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


def test_equal_tables_match_in_every_bucket_and_the_report_table_is_typed_and_commented(
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


def test_an_insert_a_delete_and_an_update_not_applied_are_found_and_classified(
    delta_spark, workdir
):
    o = Orders(delta_spark, workdir)
    _orders(o)
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
