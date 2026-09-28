"""t1: does max_lsn keep advancing while the database is idle?

The thesis of the whole design: CDC writes "dummy" rows to cdc.lsn_time_mapping
during inactivity, so sys.fn_cdc_get_max_lsn() keeps moving and a completeness
signal derived from it does not stall on quiet tables.

Nothing else may write to the lab database while this runs.

    python -m lab.checks.t1_idle_heartbeat --minutes 10 --interval 30
"""

import argparse
import sys
import time

from ..common import connect, ct_count, max_lsn, report, rows, scalar, wait_for_rows


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=10)
    p.add_argument("--interval", type=float, default=30)
    a = p.parse_args(argv)
    conn = connect()

    before = ct_count(conn, "dbo_customers")
    cid = (scalar(conn, "SELECT ISNULL(MAX(customer_id), 0) FROM dbo.customers") or 0) + 1
    rows(conn, "INSERT INTO dbo.customers (customer_id, name, email) VALUES (?, 'heartbeat', ?)",
         (cid, f"hb{cid}@example.com"))
    wait_for_rows(conn, "dbo_customers", before + 1)
    first = max_lsn(conn)
    print(f"last real change captured; max_lsn={first}. Idling for {a.minutes} min...")

    polls = []
    for _ in range(int(a.minutes * 60 / a.interval)):
        polls.append((
            scalar(conn, "SELECT SYSDATETIME()"),
            max_lsn(conn),
            scalar(conn, "SELECT sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn())"),
        ))
        time.sleep(a.interval)

    advances = [polls[i] for i in range(1, len(polls)) if polls[i][1] != polls[i - 1][1]]
    dummies = rows(conn, """
        SELECT CONVERT(varchar(22), m.start_lsn, 1), m.tran_end_time, CONVERT(varchar(22), m.tran_id, 1)
        FROM cdc.lsn_time_mapping m
        WHERE m.start_lsn > CONVERT(binary(10), ?, 1)
          AND NOT EXISTS (SELECT 1 FROM cdc.dbo_orders_CT c WHERE c.__$start_lsn = m.start_lsn)
          AND NOT EXISTS (SELECT 1 FROM cdc.dbo_customers_CT c WHERE c.__$start_lsn = m.start_lsn)
        ORDER BY m.start_lsn""", (first,))
    times = [d[1] for d in dummies]
    gaps = [(times[i] - times[i - 1]).total_seconds() for i in range(1, len(times))]
    avg_gap = round(sum(gaps) / len(gaps), 1) if gaps else None

    checks = [
        ("max_lsn advanced while idle", len(advances) > 0,
         f"{len(advances)} advances in {len(polls)} polls"),
        ("lsn_time_mapping entries with no change rows", len(dummies) > 0, f"{len(dummies)} entries"),
        ("average interval between dummy entries (s)", None, str(avg_gap)),
        ("tran_id values of dummy entries", None, str(sorted({d[2] for d in dummies}))),
    ]
    return report("t1_idle_heartbeat", checks, {
        "polls": polls, "dummy_entries": dummies,
        "server_version": scalar(conn, "SELECT @@VERSION"),
    })


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
