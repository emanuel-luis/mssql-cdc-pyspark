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
    },
    "SnapshotPlanDetail": {"snapshot", "kind", "keys", "chunk_rows", "chunks"},
    "SnapshotChunkDetail": {"snapshot", "chunk", "wave", "lo", "hi", "last"},
    "SnapshotCompletionDetail": {"snapshot", "chunks", "rows", "last_lsn"},
    "DataSkippedDetail": {"from", "to", "certain", "reason"},
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
