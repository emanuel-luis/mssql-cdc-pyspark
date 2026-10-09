import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_every_release_but_the_newest_has_the_state_it_wrote():
    # docs/RELEASING.md step 6: tests/compat/test_compat.py resumes it with this code
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    releases = re.findall(r"^## \[(\d+\.\d+\.\d+)\]", changelog, re.MULTILINE)  # no rc
    newest = max(releases, key=lambda v: tuple(map(int, v.split("."))))
    compat = ROOT / "tests" / "compat"
    missing = [v for v in releases if v != newest and not (compat / v / "manifest.json").is_file()]
    assert not missing, f"no tests/compat/<version> for {missing}: run generate.py with each"


def test_the_package_calls_nothing_a_spark_connect_session_lacks():
    # serverless and Databricks Connect sessions have no JVM (ADR 0033), and the core imports
    # no platform API (invariant 10): this runs in every loop, the connect suite in its own job
    banned = re.compile(
        r"\._(jvm|jsc|jdf|jsparkSession|jsqm|jconf|sc)\b|\bSparkContext\b|\.(sparkContext|rdd)\b"
        r"|\bdbutils\b|^\s*(import|from) databricks\b"
    )
    tried = "cores = int(spark.sparkContext.defaultParallelism)"  # spark.py: falls back to 0
    hits = [
        f"{path.relative_to(ROOT)}:{n}: {line.strip()}"
        for path in sorted((ROOT / "src").rglob("*.py"))
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if banned.search(line) and line.strip() != tried
    ]
    assert not hits, "classic-only or platform APIs: try the call and fall back instead"


def test_import_without_pyspark_says_how_to_install_it():
    code = (
        "import sys; sys.modules['pyspark'] = None\n"
        "try:\n"
        "    import mssql_cdc\n"
        "except ImportError as e:\n"
        "    print(type(e).__name__, e)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert out.startswith("ImportError ")  # not ModuleNotFoundError: the cause is explained
    assert 'pip install "mssql-cdc-pyspark[spark]"' in out
    assert "Spark platform" in out
