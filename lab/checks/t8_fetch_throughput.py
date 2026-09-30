"""t8: how fast does one connection read a wide change table, and what slows it down?

Builds ``dbo.fetch_bench`` (91 columns: decimals, short strings, ints and two ``(max)``
strings, like a wide ERP table), fills it with CDC on, then times
``SqlCdcClient.iter_changes`` over the whole range with and without the ``(max)``
columns, with two Arrow batch sizes and with a named time zone (commit times converted
per range, not per row), next to a plain ``fetchall()`` baseline.
Informational: it reports rows/s, it does not pass or fail on a number.

    python -m lab.checks.t8_fetch_throughput --rows 200000
"""

import argparse
import sys
import time

from mssql_cdc.client import make_client

from ..common import connect, connection_string, report, rows, scalar, wait_for_rows

DEC = [f"d{i}" for i in range(1, 41)]
STR = [f"s{i}" for i in range(1, 41)]
INT = [f"i{i}" for i in range(1, 9)]
MAX = ["m1", "m2"]


def _setup(conn, n: int, per_tx: int) -> None:
    rows(
        conn,
        "IF OBJECT_ID('dbo.fetch_bench') IS NOT NULL BEGIN "
        "IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_fetch_bench') "
        "EXEC sys.sp_cdc_disable_table 'dbo', 'fetch_bench', 'all'; DROP TABLE dbo.fetch_bench; END",
    )
    cols = (
        ["id INT NOT NULL PRIMARY KEY"]
        + [f"{c} DECIMAL(15,4)" for c in DEC]
        + [f"{c} VARCHAR(40)" for c in STR]
        + [f"{c} INT" for c in INT]
        + ["m1 VARCHAR(MAX)", "m2 NVARCHAR(MAX)"]
    )
    rows(conn, f"CREATE TABLE dbo.fetch_bench ({', '.join(cols)})")
    rows(conn, "EXEC sys.sp_cdc_enable_table 'dbo', 'fetch_bench', @role_name = NULL")
    values = (
        ["n"]
        + [f"CAST(n % {97 + i} AS DECIMAL(15,4)) + 0.25" for i in range(40)]
        + [f"CONCAT('value-', n % {1000 + i})" for i in range(40)]
        + [f"n % {10 + i}" for i in range(8)]
        + ["REPLICATE('x', 200)", "REPLICATE(N'y', 100)"]
    )
    for start in range(0, n, per_tx):
        rows(
            conn,
            f"""
            WITH nums AS (SELECT TOP ({min(per_tx, n - start)}) {start}
                          + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                          FROM sys.all_objects a CROSS JOIN sys.all_objects b)
            INSERT INTO dbo.fetch_bench SELECT {", ".join(values)} FROM nums""",
        )
    wait_for_rows(conn, "dbo_fetch_bench", n, timeout=600)


def _time(label, fn):
    t0 = time.perf_counter()
    n = fn()
    s = time.perf_counter() - t0
    return label, n, s, n / s if s else 0.0


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--per-tx", type=int, default=10_000)
    p.add_argument("--skip-setup", action="store_true")
    a = p.parse_args(argv)
    conn = connect()
    if not a.skip_setup:
        _setup(conn, a.rows, a.per_tx)
    lo = scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn('dbo_fetch_bench'), 1)")
    hi = scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1)")
    client = make_client({"connectionString": connection_string(), "sourceTimeZone": "UTC"})

    named = make_client(
        {
            "connectionString": connection_string(),
            "sourceTimeZone": "E. South America Standard Time",
        }
    )

    def changes(columns, batch_size, via=client):
        return lambda: sum(
            b.num_rows
            for b in via.iter_changes("dbo_fetch_bench", lo, hi, columns, True, batch_size)
        )

    def fetchall(columns):
        def run():
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(columns)} FROM cdc.dbo_fetch_bench_CT "
                "WHERE __$start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)",
                (lo, hi),
            )
            n = len(cur.fetchall())
            cur.close()
            return n

        return run

    no_max = ["id"] + DEC + STR + INT
    runs = [
        _time("arrow, 91 columns incl. 2 (max), batch 10k", changes(no_max + MAX, 10_000)),
        _time("arrow, 89 columns without (max), batch 10k", changes(no_max, 10_000)),
        _time("arrow, 89 columns without (max), batch 50k", changes(no_max, 50_000)),
        _time("arrow, 91 columns incl. 2 (max), batch 50k", changes(no_max + MAX, 50_000)),
        _time("arrow, 91 columns, named time zone", changes(no_max + MAX, 10_000, named)),
        _time("fetchall rows, 91 columns incl. (max)", fetchall(no_max + MAX)),
        _time("fetchall rows, 89 columns without (max)", fetchall(no_max)),
    ]
    client.close()
    named.close()
    checks = [
        (label, None, f"{n} rows in {s:.1f}s = {rate:,.0f} rows/s") for label, n, s, rate in runs
    ]
    return report(
        "t8_fetch_throughput",
        checks,
        {
            "rows": a.rows,
            "server": scalar(conn, "SELECT @@VERSION").splitlines()[0],
            "runs": [
                {"label": l, "rows": n, "seconds": round(s, 2), "rows_per_s": round(r)}
                for l, n, s, r in runs
            ],
        },
    )


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
