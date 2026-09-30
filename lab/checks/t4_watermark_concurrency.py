"""t4: is max_lsn a safe low watermark under concurrency?

A long transaction starts first and commits last, while short transactions commit
every second. An observer records (max_lsn, rows with LSN <= max_lsn) every few
seconds. Once everything is captured, each probe is re-counted.

If any probe's count grew later, rows appeared *below* an already observed
max_lsn, and a completeness signal built on it would be wrong. Expected: zero.
It also checks that LSN order is commit order, not begin order.

    python -m lab.checks.t4_watermark_concurrency --long-seconds 120
"""

import argparse
import sys
import threading
import time

from ..common import connect, ct_count, max_lsn, report, rows, scalar, wait_for_rows


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--long-seconds", type=float, default=120)
    p.add_argument("--probe-interval", type=float, default=5)
    a = p.parse_args(argv)
    admin = connect()
    base = (scalar(admin, "SELECT ISNULL(MAX(order_id), 0) FROM dbo.orders") or 0) + 10_000
    start_ct = ct_count(admin, "dbo_orders")
    long_id, n_short = base, int(a.long_seconds * 1.5)
    stop = threading.Event()
    probes, errors = [], []

    def long_tx():
        try:
            c = connect(autocommit=False)
            c.cursor().execute(
                "INSERT INTO dbo.orders (order_id, customer_id, status, amount) "
                "VALUES (?, 1, 'long_tx', 1)",
                (long_id,),
            )
            time.sleep(a.long_seconds)
            c.commit()
            c.close()
        except Exception as exc:  # noqa: BLE001 - reported by the main thread
            errors.append(exc)

    def short_tx():
        try:
            c = connect(autocommit=True)
            for i in range(n_short):
                c.cursor().execute(
                    "INSERT INTO dbo.orders (order_id, customer_id, status, amount) "
                    "VALUES (?, 1, 'short_tx', 1)",
                    (base + 1 + i,),
                )
                time.sleep(1)
            c.close()
        except Exception as exc:  # noqa: BLE001 - reported by the main thread
            errors.append(exc)

    def observer():
        c = connect()
        while not stop.is_set():
            m = max_lsn(c)
            probes.append((m, ct_count(c, "dbo_orders", m)))
            time.sleep(a.probe_interval)
        c.close()

    threads = [threading.Thread(target=f) for f in (long_tx, short_tx)]
    obs = threading.Thread(target=observer)
    obs.start()
    for t in threads:
        t.start()
        time.sleep(0.5)
    for t in threads:
        t.join()
    wait_for_rows(admin, "dbo_orders", start_ct + 1 + n_short, timeout=180)
    time.sleep(a.probe_interval * 2)
    stop.set()
    obs.join()
    if errors:
        return report("t4_watermark_concurrency", [("workload", False, repr(errors[0]))])

    divergent = [
        (m, then, ct_count(admin, "dbo_orders", m))
        for m, then in probes
        if ct_count(admin, "dbo_orders", m) != then
    ]
    order = rows(
        admin,
        """
        SELECT c.order_id, CONVERT(varchar(22), c.__$start_lsn, 1), m.tran_begin_time, m.tran_end_time
        FROM cdc.dbo_orders_CT c JOIN cdc.lsn_time_mapping m ON m.start_lsn = c.__$start_lsn
        WHERE c.order_id BETWEEN ? AND ? AND c.__$operation = 2 ORDER BY c.__$start_lsn""",
        (base, base + n_short),
    )
    long_row = next(r for r in order if r[0] == long_id)
    began_after_committed_before = sum(
        1 for r in order if r[0] != long_id and r[2] > long_row[2] and r[1] < long_row[1]
    )
    commit_order_violations = sum(
        1 for r in order if r[0] != long_id and r[1] > long_row[1] and r[3] < long_row[3]
    )
    checks = [
        (
            "probes where rows appeared below an observed max_lsn",
            len(divergent) == 0,
            f"{len(divergent)} of {len(probes)}",
        ),
        (
            "short txs that began after the long tx but got a lower LSN",
            began_after_committed_before > 0,
            f"{began_after_committed_before} (LSN order is commit order, not begin order)",
        ),
        (
            "txs with higher LSN but earlier commit time than the long tx",
            commit_order_violations == 0,
            str(commit_order_violations),
        ),
    ]
    return report(
        "t4_watermark_concurrency",
        checks,
        {"probes": probes, "divergent": divergent, "long_tx": long_row},
    )


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
