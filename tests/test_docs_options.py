"""docs/reference/options.md documents every option the source reads, and KNOWN_OPTIONS holds
every option the code reads. No Spark."""

import re
from pathlib import Path

from mssql_cdc.source import KNOWN_OPTIONS

ROOT = Path(__file__).parents[1]
PAGE = ROOT / "docs" / "reference" / "options.md"
# _opt, _positive_int, _bool or _non_negative(options, "Name"...), opts.get("name"), opts["name"]
READ = re.compile(
    r'(?:_opt|_positive_int|_bool|_non_negative)\(\s*[\w.]+,\s*"(\w+)"|opts(?:\.get\(|\[)"(\w+)"'
)


def test_every_option_the_source_reads_is_documented():
    page = PAGE.read_text(encoding="utf-8").lower()
    missing = sorted(o for o in KNOWN_OPTIONS - {"fakepath"} if f"### {o}\n" not in page)
    assert not missing, f"undocumented in {PAGE.name}: {missing}"


def test_every_option_the_code_reads_is_known():  # else it would be warned about as ignored
    code = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "src/mssql_cdc").glob("*.py"))
    read = {(a or b).lower() for a, b in READ.findall(code)}
    assert len(read) > 15  # the pattern still finds the reads
    assert read <= KNOWN_OPTIONS, f"read but not in KNOWN_OPTIONS: {sorted(read - KNOWN_OPTIONS)}"
