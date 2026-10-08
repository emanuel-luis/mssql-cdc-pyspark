"""The 0.3 API shape at run time: the call forms it made keyword-only raise TypeError before
anything runs, a wrong mode raises ValueError naming the allowed values, and the typed
results are plain dicts; every name the 0.4.0 modules bound still imports from the packages
that replaced them. No Spark (tests/typing_api.py checks the same for mypy)."""

import importlib

import pytest

import mssql_cdc
from mssql_cdc import apply_changes, await_all, finalization, reconcile
from mssql_cdc.pipeline import CdcStream
from mssql_cdc.sink import delta_sink
from mssql_cdc.spark import get_spark


def _stream():  # no session: every call below fails before using one
    cdc = object.__new__(CdcStream)
    cdc.spark, cdc.options = None, {"captureInstance": "dbo_t"}
    return cdc


OLD_FORMS = {
    "to_delta": lambda: _stream().to_delta("t", "a", "/c", "f", {"availableNow": True}),
    "snapshot": lambda: _stream().snapshot("t", True),
    "apply_changes": lambda: apply_changes(None, "b", "s", "dbo_t", ["k"], control_table="c"),
    "reconcile": lambda: reconcile(None, {}, "s", ["k"], bronze="b", control_table="c"),
    "advance": lambda: finalization.advance(None, "c", "t", None, "day"),
    "track": lambda: finalization.track(None, None, "c", "t", "day"),
    "candidate": lambda: finalization.candidate(None, "day"),
    "FinalizationListener": lambda: finalization.FinalizationListener(None, "r", "c", "t", "day"),
    "join": lambda: object.__new__(finalization.FinalizationListener).join(1),
    "end_offset_from_progress": lambda: finalization.end_offset_from_progress(None, 0),
    "await_all": lambda: await_all({}, 1),
    "delta_sink": lambda: delta_sink("t", "a", "f", "/metrics"),
    "get_spark": lambda: get_spark("app", "local[1]", False),
}


@pytest.mark.parametrize("name", OLD_FORMS)
def test_the_positional_forms_0_3_made_keyword_only_raise_type_error(name):
    with pytest.raises(TypeError, match="positional argument"):
        OLD_FORMS[name]()


@pytest.mark.parametrize(
    ("kwargs", "allowed"),
    [
        ({"on_data_loss": "skip"}, "'fail' or 'resnapshot'"),
        ({"snapshot": "chunks"}, "'full' or 'chunked'"),
    ],
)
def test_a_wrong_mode_raises_value_error_naming_the_allowed_ones(kwargs, allowed):
    with pytest.raises(ValueError, match=allowed):
        _stream().to_delta("t", "a", "/c", "f", **kwargs)


def test_the_typed_results_are_exported_and_plain_dicts():
    offset = mssql_cdc.Offset(lsn="0x" + "0" * 20, commit_ts="")
    assert type(offset) is dict and offset == {"lsn": "0x" + "0" * 20, "commit_ts": ""}
    for name in ("Offset", "BackfillStatus", "ApplyResult", "ReconcileResult"):
        assert name in mssql_cdc.__all__ and getattr(mssql_cdc, name).__total__
    assert finalization.Granularity is mssql_cdc.Granularity  # its home before 0.3


# Every name 0.4.0's client.py and pipeline.py bound at run time, but their stdlib and pyarrow
# imports: frozen here, since CI's shallow checkout has no 0.4.0 to read them from.
NAMES_0_4_0 = {
    "mssql_cdc.client": [
        "ArrowOdbcBackend",
        "Backend",
        "CaptureInstance",
        "CdcClient",
        "DataLossError",
        "DdlChange",
        "Lsn",
        "MssqlPythonBackend",
        "SchemaChangedError",
        "SourceTable",
        "SqlCdcClient",
        "is_data_loss",
        "is_schema_changed",
        "last_bound",
        "make_client",
        "plan_chunks",
        "snapshot_plan",
        "_IDENT_RE",
        "_MAX_BATCH_BYTES",
        "_MAX_SLICES",
        "_SHIFT_HOURS",
        "_SLICES",
        "_SPARK_TYPES",
        "_TYPE_RE",
        "_TZ_RE",
        "_check_capture_instance",
        "_check_column",
        "_check_ident",
        "_check_type",
        "_check_tz",
        "_int_chunks",
        "_int_range",
        "_isolated",
        "_json_key",
        "_key_select",
        "_key_tuple",
        "_log",
        "_lsn",
        "_non_negative",
        "_raised",
        "_spark_type",
        "_sql_type",
        "_text",
        "_timestamps_in_us",
    ],
    "mssql_cdc.pipeline": [
        "BackfillState",
        "BackfillStatus",
        "CdcStream",
        "Isolation",
        "Offset",
        "OnDataLoss",
        "SnapshotMode",
        "stream",
        "_ChunkRead",
        "_Opened",
        "_SNAPSHOT_FACTS",
        "_STATE",
        "_URI",
        "_chunk_detail",
        "_earlier_wave",
        "_family",
        "_generation",
        "_instances",
        "_iso",
        "_last_offset",
        "_log",
        "_lost",
        "_mode",
        "_opt",
        "_plan_of",
        "_read_state",
        "_snapshot_after_switch",
        "_ts",
        "_unfinished",
        "_version",
        "_write_state",
    ],
}


@pytest.mark.parametrize("module", NAMES_0_4_0)
def test_every_name_0_4_0_bound_still_imports_and_star_imports_the_public_ones(module):
    mod = importlib.import_module(module)
    assert [n for n in NAMES_0_4_0[module] if not hasattr(mod, n)] == []
    assert [n for n in NAMES_0_4_0[module] if n[0] != "_" and n not in mod.__all__] == []
