"""Properties checked over generated inputs (Hypothesis): LSN math and its canonical form, chunk
plans tiling the key space, each key's latest image whatever the order of its change rows, and
the boolean options' spellings.

Every test but the latest image's starts no JVM, so the fast loop (``-m "not spark and not
sqlserver"``) runs them. The examples are derandomized (the same ones on every run and machine)
and capped, and no example database is kept.

Commit times across a fall-back are not here: their conversion is T-SQL only
(``SqlCdcClient._utc``), which ``tests/integration`` runs on SQL Server."""

import re
from itertools import pairwise
from typing import Any

import pytest
from hypothesis import Phase, example, given, settings
from hypothesis import strategies as st

from mssql_cdc import lsn
from mssql_cdc.client import SourceTable, plan_chunks, snapshot_plan
from mssql_cdc.fake import FakeCdcClient
from mssql_cdc.source import _bool

FIXED = settings(derandomize=True, database=None, deadline=None, max_examples=200)

LSN_END = 1 << 80  # 10 bytes
lsn_ints = st.integers(0, LSN_END - 1)


# -- LSNs ----------------------------------------------------------------------------------------
@FIXED
@given(lsn_ints)
def test_an_lsn_round_trips_through_its_canonical_form(n):
    text = lsn.from_int(n)
    assert re.fullmatch(r"0x[0-9A-F]{20}", text)
    assert lsn.to_int(text) == n
    assert lsn.normalize(text) == text
    assert lsn.normalize(n.to_bytes(10, "big")) == text


@FIXED
@given(
    lsn_ints,
    st.sampled_from(["", "0x", "0X"]),
    st.booleans(),
    st.booleans(),
    st.sampled_from(["", " ", "\t", "\n "]),
)
def test_every_spelling_of_an_lsn_normalizes_to_one(n, prefix, short, lower, pad):
    digits = format(n, "020X")
    digits = (digits.lstrip("0") or "0") if short else digits
    text = pad + prefix + (digits.lower() if lower else digits) + pad
    assert lsn.normalize(text) == lsn.from_int(n)


@FIXED
@given(st.one_of(st.integers(max_value=-1), st.integers(min_value=LSN_END)))
def test_an_integer_outside_ten_bytes_is_no_lsn(n):
    with pytest.raises(ValueError, match="LSN integer out of range"):
        lsn.from_int(n)


@FIXED
@given(st.binary(max_size=12).filter(lambda b: len(b) != 10))
def test_only_ten_bytes_are_an_lsn(value):
    with pytest.raises(ValueError, match="LSN must be 10 bytes"):
        lsn.normalize(value)


@FIXED
@given(lsn_ints, lsn_ints)
def test_string_order_is_lsn_order(a, b):
    assert (lsn.from_int(a) < lsn.from_int(b)) == (a < b)
    assert (lsn.from_int(a) == lsn.from_int(b)) == (a == b)


@FIXED
@given(st.integers(0, LSN_END - 2))
def test_the_fakes_increment_and_decrement_are_neighbours(n):
    client = FakeCdcClient("unused")  # neither reads its files
    here = lsn.from_int(n)
    after = client.increment_lsn(here)
    assert lsn.to_int(after) == n + 1 and here < after
    assert client.decrement_lsn(after) == here
    assert client.decrement_lsn(lsn.ZERO_LSN) == lsn.ZERO_LSN


# -- chunk plans (ADR 0028) ----------------------------------------------------------------------
class _Rows(FakeCdcClient):
    """The fake's reads of a source table, over rows held in memory instead of its files."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        super().__init__("unused")
        self.rows = rows

    def _table(self, table: str) -> list[dict[str, Any]]:
        return self.rows


def _inside(key, lo, hi) -> bool:
    return (lo is None or key >= lo) and (hi is None or key < hi)


# a dense cluster, a sparse region and the whole BIGINT range, ids repeated now and then
int_keys = st.one_of(
    st.integers(-20, 60), st.integers(-(10**6), 10**6), st.integers(-(2**63), 2**63 - 1)
)


@FIXED
@given(
    keys=st.lists(int_keys, min_size=1, max_size=200),
    later=st.lists(int_keys, max_size=5),
    estimate=st.integers(0, 400),
    chunk_rows=st.integers(1, 40),
)
# a slice 2 wide over chunk_rows, counted again: no chunk holds both its values
@example(keys=[0, 0, 1, 1], later=[], estimate=0, chunk_rows=1)
# slice [100, 150) counted again on a grid of 3: its first finer slice starts at 100, not 99
@example(keys=[98, 99, 100, 101, 120, 147], later=[], estimate=0, chunk_rows=3)
def test_an_integer_plan_tiles_the_keys_up_to_max(keys, later, estimate, chunk_rows):
    source = SourceTable("dbo", "t", ["id"], None)
    table = _Rows([{"id": k} for k in keys])
    extent = snapshot_plan(table, "ci", source)
    top = max(keys)
    assert extent == {"kind": "int", "lo": min(keys), "hi": top, "rows": len(keys)}
    extent["rows"] = estimate  # sp_spaceused's, not a count
    table.rows += [{"id": k} for k in later]  # inserted after S, below MIN or above MAX too
    plan = plan_chunks(table, "ci", source, extent, chunk_rows)
    # open below, each from where the previous ends, the last at MAX + 1: no gap, no overlap
    assert plan[0][0] is None and plan[-1][1] == top + 1
    assert all(a[1] == b[0] for a, b in pairwise(plan))
    assert all(lo < hi for lo, hi in plan[1:])
    assert all(type(b) is int for chunk in plan for b in chunk if b is not None)
    held = [[k for k in keys + later if _inside(k, lo, hi)] for lo, hi in plan]
    assert sorted(k for c in held for k in c) == sorted(k for k in keys + later if k <= top)
    # none starts empty; one over chunk_rows holds a single value, which no bound can split
    assert all(c and (len(c) <= chunk_rows or len(set(c)) == 1) for c in held)


pairs = st.tuples(st.integers(-5, 5), st.integers(-50, 50))


@FIXED
@given(
    keys=st.lists(pairs, min_size=1, max_size=60, unique=True),
    later=st.lists(pairs, max_size=4, unique=True),
    chunk_rows=st.integers(1, 10),
)
def test_a_keyset_plan_tiles_the_keys_up_to_the_first_after_max(keys, later, chunk_rows):
    source = SourceTable("dbo", "t", ["a", "b"], None)
    table = _Rows([{"a": a, "b": b} for a, b in keys])
    extent = snapshot_plan(table, "ci", source)
    top = max(keys)
    assert extent == {"kind": "keyset", "max": list(top)}
    new = [k for k in later if k not in keys]  # a unique index: inserted after S
    table.rows += [{"a": a, "b": b} for a, b in new]
    plan = [  # bounds as JSON lists; None: open
        tuple(tuple(b) if b is not None else None for b in chunk)
        for chunk in plan_chunks(table, "ci", source, extent, chunk_rows)
    ]
    # open below, each from where the previous ends, the last at the first key after MAX or
    # open: the keys after MAX are the stream's
    above = sorted(k for k in new if k > top)
    assert plan[0][0] is None and plan[-1][1] == (above[0] if above else None)
    assert all(a[1] == b[0] for a, b in pairwise(plan))
    assert all(hi is None or lo < hi for lo, hi in plan[1:])
    every = keys + new
    held = [[k for k in every if _inside(k, lo, hi)] for lo, hi in plan]
    assert sorted(k for c in held for k in c) == sorted(k for k in every if k <= top)
    # chunk_rows keys a chunk; the last also holds MAX
    assert all(0 < len(c) <= chunk_rows + 1 for c in held)


# -- change ordering (ADR 0019) ------------------------------------------------------------------
LSNS = [lsn.from_int(16 * i) for i in (1, 2)]
SEQVALS = [lsn.from_int(i) for i in (1, 2)]
# (key, _start_lsn, _command_id, _seqval, _operation) over few values, so a key's rows often
# share a transaction: a snapshot row (0) has no _command_id and no _seqval, and an update's
# 3 and 4 share both, only _operation orders them
changes = st.lists(
    st.tuples(
        st.integers(0, 3),
        st.sampled_from(LSNS),
        st.none() | st.integers(1, 2),
        st.none() | st.sampled_from(SEQVALS),
        st.integers(0, 4),
    ),
    min_size=1,
    max_size=80,
    unique_by=lambda c: (c[0], c[1], c[3], c[4]),  # one row per position, as in a change table
)


def _position(row, command_id):  # later is larger; NULL before any value (desc, nulls last)
    *_, start, cmd, seq, op = row
    return (start, (cmd is not None, cmd) if command_id else 0, (seq is not None, seq), op)


# several tables of changes an example, key (t, k), so that one Spark job checks them all. No
# shrinking: each step is a Spark job, and Hypothesis shrinks for up to 5 minutes, which CI's
# per-test timeout ends before the failure is reported; the example as generated is small enough
@settings(FIXED, max_examples=6, phases=[Phase.explicit, Phase.generate])
@given(tables=st.lists(changes, min_size=1, max_size=8), data=st.data())
def test_any_order_of_the_same_changes_gives_each_key_its_latest_image(spark, tables, data):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from mssql_cdc.silver import _latest

    rows = [(t, *row) for t, table in enumerate(tables) for row in table]
    expected = {}  # (ordered by _command_id, t, k) -> the row's index
    for by_id in (True, False):
        for v, row in enumerate(rows):
            last = expected.get((by_id, *row[:2]))
            if last is None or _position(row, by_id) > _position(rows[last], by_id):
                expected[(by_id, *row[:2])] = v
    order = data.draw(st.permutations(range(len(rows))))
    columns = ["t", "k", "_start_lsn", "_command_id", "_seqval", "_operation", "v"]
    types = [IntegerType()] * 2 + [StringType(), IntegerType(), StringType()] + [IntegerType()] * 2
    schema = StructType([StructField(c, ty) for c, ty in zip(columns, types)])
    df = spark.createDataFrame([(*rows[v], v) for v in order], schema)
    keys = ["t", "k"]
    by_id = _latest(df, keys, [*keys, "v"], True).withColumn("by_id", F.lit(True))
    # a bronze written with includeCommandId=false
    by_seqval = _latest(df.drop("_command_id"), keys, [*keys, "v"], False)
    both = by_id.unionByName(by_seqval.withColumn("by_id", F.lit(False)))
    assert {(r["by_id"], r["t"], r["k"]): r["v"] for r in both.collect()} == expected


# -- options -------------------------------------------------------------------------------------
# docs/reference/options.md: true for true, 1, yes or y and false for false, 0, no or n, in any
# case; anything else raises ValueError naming the option
SPELLINGS = {"true": True, "1": True, "yes": True, "y": True}
SPELLINGS |= {"false": False, "0": False, "no": False, "n": False}
NEAR = ["on", "off", "t", "f", "ture", "flase", "yes!", "nope", "2", "-1", "01", "1.0", "", "none"]


@st.composite
def option_values(draw):
    word = draw(st.sampled_from(sorted(SPELLINGS) + NEAR) | st.text(max_size=6))
    word = "".join(c.upper() if draw(st.booleans()) else c for c in word)
    pad = st.sampled_from(["", " ", "\t", "\n "])
    return draw(st.one_of(st.just(draw(pad) + word + draw(pad)), st.booleans(), st.integers(-2, 3)))


@FIXED
@given(option_values())
def test_a_boolean_option_takes_exactly_the_documented_spellings(value):
    expected = SPELLINGS.get(str(value).strip().lower())  # blanks around it ignored
    options = {"FAILONDATALOSS": value}  # names in any case
    if expected is None:
        with pytest.raises(ValueError, match="failOnDataLoss must be true or false"):
            _bool(options, "failOnDataLoss", "true")
    else:
        assert _bool(options, "failOnDataLoss", "true") is expected
