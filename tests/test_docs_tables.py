"""docs/reference/tables.md copies the tables' comments and schema versions from the code."""

import re
from pathlib import Path

import pytest

from mssql_cdc import finalization, silver, sink
from mssql_cdc.migrations import bronze, control, facts
from mssql_cdc.migrations import reconcile as reconcile_migrations
from mssql_cdc.migrations import silver as silver_migrations
from mssql_cdc.reconcile import REPORT_COLUMNS, REPORT_COMMENT

PAGE = Path(__file__).parents[1] / "docs" / "reference" / "tables.md"
ROW = re.compile(r"^\| `(\w+)` \| ([^|]+) \| (.+) \|$")


def _sections() -> dict[str, str]:
    parts = re.split(r"^## (.+)$", PAGE.read_text(encoding="utf-8"), flags=re.MULTILINE)
    return dict(zip(parts[1::2], parts[2::2], strict=True))


def _columns(text: str) -> list[tuple[str, str, str]]:
    return [m.groups() for line in text.splitlines() if (m := ROW.match(line))]


def _comment(text: str) -> str:
    return next(line[2:] for line in text.splitlines() if line.startswith("> "))


@pytest.mark.parametrize(
    ("section", "comment", "columns"),
    [
        ("Facts", sink.FACTS_COMMENT, sink.FACTS_COLUMNS),
        ("Control", finalization.CONTROL_COMMENT, finalization.CONTROL_COLUMNS),
        ("Silver", silver.SILVER_COMMENT, silver.SILVER_COLUMNS),
        ("Reconcile report", REPORT_COMMENT, REPORT_COLUMNS),
    ],
)
def test_comments_match_the_code(section, comment, columns):
    text = _sections()[section]
    assert _comment(text) == comment
    assert _columns(text) == [tuple(c) for c in columns]


def test_bronze_comments_match_the_code():
    text = _sections()["Bronze"]
    assert _comment(text) == sink.BRONZE_COMMENT
    assert {n: c for n, _, c in _columns(text)} == sink.BRONZE_COLUMN_COMMENTS


def test_schema_versions_match_the_migrations():
    kinds = {
        "bronze": bronze,
        "facts": facts,
        "control": control,
        "silver": silver_migrations,
        "reconcile": reconcile_migrations,
    }
    rows = re.findall(r"^\| (\w+) \| (\d+) \|", _sections()["Schema versions"], flags=re.MULTILINE)
    assert {k: int(v) for k, v in rows} == {k: len(m.MIGRATIONS) for k, m in kinds.items()}
