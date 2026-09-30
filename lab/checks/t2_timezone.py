"""t2: which clock is cdc.lsn_time_mapping.tran_end_time in?

It is a timezone-less datetime. Finalization truncates it to periods, so knowing
whether it follows the server's local clock or UTC decides the sourceTimeZone
option. Run once with MSSQL_TZ=UTC and once with a non-UTC zone.

    python -m lab.checks.t2_timezone
"""

import sys

from ..common import connect, ct_count, report, rows, scalar, wait_for_rows


def main(argv=None) -> bool:
    conn = connect()
    before = ct_count(conn, "dbo_customers")
    cid = (scalar(conn, "SELECT ISNULL(MAX(customer_id), 0) FROM dbo.customers") or 0) + 1
    rows(
        conn,
        "INSERT INTO dbo.customers (customer_id, name, email) VALUES (?, 'tz probe', ?)",
        (cid, f"tz{cid}@example.com"),
    )
    local_now, utc_now, offset_now = rows(
        conn, "SELECT SYSDATETIME(), SYSUTCDATETIME(), SYSDATETIMEOFFSET()"
    )[0]
    try:
        server_tz = scalar(conn, "SELECT CURRENT_TIMEZONE_ID() + N' / ' + CURRENT_TIMEZONE()")
    except Exception as exc:  # noqa: BLE001 - older versions
        server_tz = f"n/a ({exc.__class__.__name__})"
    wait_for_rows(conn, "dbo_customers", before + 1)
    tran_end = scalar(
        conn,
        """
        SELECT TOP (1) m.tran_end_time FROM cdc.lsn_time_mapping m
        JOIN cdc.dbo_customers_CT c ON c.__$start_lsn = m.start_lsn
        WHERE c.customer_id = ? ORDER BY m.start_lsn DESC""",
        (cid,),
    )
    regressions = scalar(
        conn,
        """
        SELECT COUNT(*) FROM (SELECT tran_end_time,
               LAG(tran_end_time) OVER (ORDER BY start_lsn) AS prev FROM cdc.lsn_time_mapping) x
        WHERE tran_end_time < prev""",
    )
    d_local = abs((tran_end - local_now).total_seconds())
    d_utc = abs((tran_end - utc_now).total_seconds())
    follows = "server local clock" if d_local <= d_utc else "UTC"
    checks = [
        ("server timezone", None, f"{server_tz} / {offset_now}"),
        (
            "tran_end_time follows",
            None,
            f"{follows} (|Δlocal|={d_local:.1f}s, |Δutc|={d_utc:.1f}s)",
        ),
        ("commit-time regressions in LSN order", regressions == 0, str(regressions)),
    ]
    return report(
        "t2_timezone",
        checks,
        {
            "local_now": local_now,
            "utc_now": utc_now,
            "tran_end_time": tran_end,
            "hint": "sourceTimeZone=auto uses CURRENT_TIMEZONE_ID(), or the current UTC offset on SQL "
            "Server 2019 or older; name the zone there if it has daylight saving",
        },
    )


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
