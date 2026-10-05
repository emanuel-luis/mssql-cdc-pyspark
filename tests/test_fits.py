"""source._fits, the type widenings a running stream absorbs and a union of capture instances
takes (ADR 0023): the same as Delta's type widening. No Spark."""

import pytest

from mssql_cdc.client import CaptureInstance, SchemaChangedError
from mssql_cdc.source import _fits, union_columns


@pytest.mark.parametrize(
    ("old", "new", "fits"),
    [
        # integers widen up the chain, never down
        ("tinyint", "smallint", True),
        ("smallint", "int", True),
        ("int", "bigint", True),
        ("tinyint", "bigint", True),
        ("bigint", "int", False),
        ("int", "smallint", False),
        ("smallint", "tinyint", False),
        # an integer into double, except bigint (53 bits of mantissa)
        ("int", "double", True),
        ("smallint", "double", True),
        ("bigint", "double", False),
        ("float", "double", True),
        ("double", "float", False),
        ("date", "timestamp_ntz", True),
        ("timestamp_ntz", "date", False),
        ("date", "timestamp", False),
        # an integer into a decimal with as many integer digits as it has
        ("bigint", "decimal(19,0)", False),
        ("bigint", "decimal(20,0)", True),
        ("int", "decimal(10,0)", True),
        ("int", "decimal(9,0)", False),
        ("int", "decimal(12,2)", True),
        ("int", "decimal(11,2)", False),
        # a decimal widens when neither its scale nor its integer digits shrink
        ("decimal(10,2)", "decimal(10,4)", False),
        ("decimal(10,2)", "decimal(12,4)", True),
        ("decimal(10,2)", "decimal(11,2)", True),
        ("decimal(10,4)", "decimal(10,2)", False),
        ("decimal(10,0)", "int", False),
        # the same type, whatever the case and spaces
        ("DECIMAL(18, 2)", "decimal(18,2)", True),
        ("INT", "int", True),
        ("string", "int", False),
        ("int", "string", False),
    ],
)
def test_fits(old, new, fits):
    assert _fits(old, new) is fits


def _instance(name, start, **columns):
    return CaptureInstance(name, start, list(columns), list(columns.values()))


def test_union_takes_the_newer_type_that_holds_the_older():
    older = _instance("dbo_orders", "0x01", id="INT", amount="DECIMAL(9,2)")
    newer = _instance("dbo_orders_v2", "0x02", id="BIGINT", amount="DECIMAL(18,4)", note="STRING")
    assert union_columns([older, newer]) == "`id` BIGINT, `amount` DECIMAL(18,4), `note` STRING"


def test_union_fails_on_a_newer_type_that_does_not_hold_the_older():
    older = _instance("dbo_orders", "0x01", amount="DECIMAL(18,2)")
    newer = _instance("dbo_orders_v2", "0x02", amount="DECIMAL(10,2)")
    with pytest.raises(
        SchemaChangedError, match=r"'amount' as DECIMAL\(18,2\) and DECIMAL\(10,2\)"
    ):
        union_columns([older, newer])
