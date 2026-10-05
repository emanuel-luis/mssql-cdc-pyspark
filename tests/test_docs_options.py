"""docs/reference/options.md documents every option the source reads. No Spark."""

from pathlib import Path

from mssql_cdc.source import KNOWN_OPTIONS

PAGE = Path(__file__).parents[1] / "docs" / "reference" / "options.md"


def test_every_option_the_source_reads_is_documented():
    page = PAGE.read_text(encoding="utf-8").lower()
    missing = sorted(o for o in KNOWN_OPTIONS - {"fakepath"} if f"### {o}\n" not in page)
    assert not missing, f"undocumented in {PAGE.name}: {missing}"
