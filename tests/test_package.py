import subprocess
import sys


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
