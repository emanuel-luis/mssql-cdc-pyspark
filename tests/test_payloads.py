"""The payload types keep every key ADR 0021 lists as state: keys are only added. The writers
build their payloads as these types, so mypy fails one that drops a required key. No Spark."""

import mssql_cdc
from mssql_cdc.payloads import WaveChunk

STATE_KEYS = {
    "SnapshotOpenDetail": {
        "mode",
        "kind",
        "generation",
        "lost_from_ts",
        "lost_to_ts",
        "keys",
        "plan",
        "switch_batch_id",  # 0.8.0
    },
    "SnapshotPlanDetail": {"snapshot", "kind", "keys", "chunk_rows", "chunks", "chunk_count"},
    "SnapshotChunkDetail": {"snapshot", "chunk", "wave", "lo", "hi", "last"},
    "SnapshotCompletionDetail": {"snapshot", "chunks", "rows", "last_lsn"},
    "DataSkippedDetail": {"from", "to", "certain", "reason"},
    "CaptureInstanceSwitchedDetail": {"old", "new", "start_lsn", "not_captured", "text"},  # 1.0
    "BatchDetail": {"warnings"},
    "WaveMetadata": {"backfill", "wave", "lsn", "attempt", "chunks"},
}


def test_the_payload_types_keep_every_key_the_state_contract_lists():
    for name, keys in STATE_KEYS.items():
        assert name in mssql_cdc.__all__
        t = getattr(mssql_cdc, name)
        assert keys <= t.__required_keys__ | t.__optional_keys__, name
    chunk = {"chunk", "lo", "hi", "last", "rows", "high_lsn", "read_seconds", "read_mb"}
    assert chunk <= WaveChunk.__required_keys__
    assert type(mssql_cdc.BatchDetail(warnings=[])) is dict


def test_a_facts_row_without_key_values_is_completed_from_the_plans_table_or_fails():
    import pytest

    from mssql_cdc import events

    assert events.plans_table("cat.sch.orders") == "cat.sch.orders_snapshot_plans"
    assert events.plans_table("/lake/bronze/orders/") == "/lake/bronze/orders_snapshot_plans"
    assert events.plans_table("sch.`my orders`") == "sch.`my orders_snapshot_plans`"
    old = {"snapshot": "S", "chunk": 1, "wave": 0, "lo": 3, "hi": 6, "last": False}
    new = events.public(events.SNAPSHOT_CHUNK, old)
    assert new == {"snapshot": "S", "chunk": 1, "wave": 0, "last": False}
    kept = events.Plans("t_snapshot_plans", "S", {(events.SNAPSHOT_CHUNK, 1, 0): old})
    assert kept.whole(events.SNAPSHOT_CHUNK, new) == old
    assert kept.whole(events.SNAPSHOT_CHUNK, {**old, "lo": 2}) == {**old, "lo": 2}  # pre-0.7.0
    with pytest.raises(
        ValueError, match="no 'snapshot_chunk' row of the snapshot at S for chunk 1"
    ):
        kept.whole(events.SNAPSHOT_CHUNK, {**new, "wave": 1})  # never a range without its bounds
    opened = {"mode": "chunked", "kind": "bootstrap", "keys": ["id"], "plan": {"kind": "keyset"}}
    assert events.public(events.SNAPSHOT_OPEN, opened) == {"mode": "chunked", "kind": "bootstrap"}


def test_a_switch_detail_is_json_and_the_reader_takes_the_line_rows_held_before_1_0():
    import json

    from mssql_cdc import events

    s = "0x0000002A000001000031"
    plain = events.switch_detail("dbo_t", "dbo_t_v2", s, [])
    lost = events.switch_detail("dbo_t", "dbo_t_v2", s, ["a", "b"])
    assert json.loads(plain) == {
        "old": "dbo_t",
        "new": "dbo_t_v2",
        "start_lsn": s,
        "not_captured": [],
        "text": "dbo_t -> dbo_t_v2",  # the line such a row held before
    }
    assert json.loads(lost)["text"] == "dbo_t -> dbo_t_v2; no longer captured, read as NULL: a, b"
    for detail in (plain, lost):  # JSON, or the line a release before 1.0.0rc1 wrote
        assert events.switch_of(detail, s) == json.loads(detail)
        assert events.switch_of(json.loads(detail)["text"], s) == json.loads(detail)
