"""Shared helpers for the lab scripts (SQL Server side)."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "lab" / "results"


def load_env() -> None:
    """Minimal .env loader (no extra dependency)."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


load_env()
DATABASE = os.environ.get("MSSQL_DATABASE", "cdc_lab")
SOURCE_TZ = os.environ.get("MSSQL_SOURCE_TZ", "auto")


def connection_string(database: str | None = None) -> str:
    host = os.environ.get("MSSQL_HOST", "localhost")
    port = os.environ.get("MSSQL_PORT", "1433")
    user = os.environ.get("MSSQL_USER", "sa")
    pwd = os.environ["MSSQL_SA_PASSWORD"]
    db = database or DATABASE
    return (
        f"Server={host},{port};Database={db};UID={user};PWD={pwd};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )


def connect(database: str | None = None, autocommit: bool = True):
    import mssql_python

    return mssql_python.connect(connection_string(database), autocommit=autocommit, timeout=30)


def rows(conn, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, tuple(params))
    try:
        return cur.fetchall()
    except Exception:  # noqa: BLE001 - statement without a result set
        return []
    finally:
        cur.close()


def scalar(conn, sql: str, params=()):
    result = rows(conn, sql, params)
    return result[0][0] if result else None


def run_script(path: Path, variables: dict[str, str]) -> None:
    """Run a .sql file with sqlcmd-style ``GO`` separators and $(VAR) substitution."""
    text = path.read_text()
    for key, value in variables.items():
        text = text.replace(f"$({key})", value)
    conn = connect("master")
    try:
        for batch in re.split(r"^\s*GO\s*$", text, flags=re.MULTILINE | re.IGNORECASE):
            if batch.strip():
                for row in rows(conn, batch):
                    print("  ", *row)
    finally:
        conn.close()


def max_lsn(conn) -> str:
    return scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)")


def ct_count(conn, capture_instance: str, upto_lsn: str | None = None) -> int:
    sql = f"SELECT COUNT(*) FROM cdc.[{capture_instance}_CT]"
    if upto_lsn:
        sql += " WHERE __$start_lsn <= CONVERT(binary(10), ?, 1)"
        return scalar(conn, sql, (upto_lsn,))
    return scalar(conn, sql)


def wait_for_rows(conn, capture_instance: str, expected: int, timeout: float = 120) -> int:
    """Wait until the change table holds at least ``expected`` rows."""
    deadline = time.time() + timeout
    while True:
        n = ct_count(conn, capture_instance)
        if n >= expected or time.time() > deadline:
            return n
        time.sleep(1)


def save_result(name: str, payload: dict) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RESULTS / f"{name}-{stamp}.json"
    path.write_text(json.dumps(payload, indent=2, default=str))
    return path


def report(name: str, checks: list[tuple[str, bool | None, str]], extra: dict | None = None) -> bool:
    """Print PASS/FAIL/INFO lines, persist results, return overall success."""
    print(f"\n== {name}")
    ok = True
    for label, passed, detail in checks:
        tag = "INFO" if passed is None else ("PASS" if passed else "FAIL")
        ok = ok and passed is not False
        print(f"[{tag}] {label}: {detail}")
    path = save_result(name, {"checks": [dict(label=l, passed=p, detail=d) for l, p, d in checks],
                              "extra": extra or {}})
    print(f"-> {'OK' if ok else 'FAILED'} (saved {path.relative_to(ROOT)})")
    return ok
