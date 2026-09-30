"""t3: read semantics of the change table cdc.<ci>_CT, which the reader queries (ADR 0009).

* operation codes and ordering inside one transaction (__$command_id);
* whether cdc.fn_cdc_get_all_changes_<ci> returns __$command_id (why the reader does not
  use it);
* a primary-key update (deferred update -> delete + insert);
* an empty interval (from > to) returns no rows;
* --destructive: cleanup moves min_lsn, and a purged range reads as empty, which is why
  the reader re-checks min_lsn after every read.

    python -m lab.checks.t3_read_semantics [--destructive]
"""

import argparse
import sys

from ..common import connect, ct_count, max_lsn, report, rows, scalar, wait_for_rows


def _changes(conn, from_lsn, to_lsn):
    return rows(conn, """
        SELECT CONVERT(varchar(22), __$start_lsn, 1), __$operation, order_id, __$command_id
        FROM cdc.dbo_orders_CT
        WHERE __$start_lsn BETWEEN CONVERT(binary(10), ?, 1) AND CONVERT(binary(10), ?, 1)
        ORDER BY __$start_lsn, __$command_id, __$seqval, __$operation""", (from_lsn, to_lsn))


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--destructive", action="store_true", help="run cleanup on the change table")
    a = p.parse_args(argv)
    conn = connect()
    checks = []

    base = (scalar(conn, "SELECT ISNULL(MAX(order_id), 0) FROM dbo.orders") or 0) + 1000
    start_ct = ct_count(conn, "dbo_orders")
    rows(conn, "INSERT INTO dbo.orders (order_id, customer_id, status, amount) VALUES (?, 1, 'victim', 1)", (base,))
    start = max_lsn(conn)
    rows(conn, f"""
        BEGIN TRAN;
          INSERT INTO dbo.orders (order_id, customer_id, status, amount) VALUES ({base + 1}, 1, 'new', 20.00);
          UPDATE dbo.orders SET status = 'paid' WHERE order_id = {base + 1};
          UPDATE dbo.orders SET amount = 25.00 WHERE order_id = {base + 1};
          DELETE FROM dbo.orders WHERE order_id = {base};
        COMMIT;""")
    rows(conn, f"UPDATE dbo.orders SET order_id = {base + 2} WHERE order_id = {base + 1}")
    # victim insert (1) + insert (1) + 2 updates (4) + delete (1) + pk update (>=2)
    wait_for_rows(conn, "dbo_orders", start_ct + 9)
    end = max_lsn(conn)

    try:
        changes = _changes(conn, start, end)
        checks.append(("__$command_id in the change table", True, "yes"))
    except Exception as exc:  # noqa: BLE001 - the error is the check result
        checks.append(("__$command_id in the change table", False, str(exc)[:200]))
        return report("t3_read_semantics", checks, {})
    try:
        rows(conn, "SELECT TOP (1) __$command_id FROM cdc.fn_cdc_get_all_changes_dbo_orders("
                   "CONVERT(binary(10), ?, 1), CONVERT(binary(10), ?, 1), N'all update old')", (start, end))
        checks.append(("__$command_id from fn_cdc_get_all_changes", None, "yes"))
    except Exception as exc:  # noqa: BLE001 - the error is the check result
        checks.append(("__$command_id from fn_cdc_get_all_changes", None, str(exc)[:200]))

    by_tx = {}
    for lsn, op, oid, *_ in changes:
        by_tx.setdefault(lsn, []).append(op)
    txs = list(by_tx.values())
    multi = next((ops for ops in txs if len(ops) >= 6), None)
    checks.append(("ops in the multi-statement transaction", multi == [2, 3, 4, 3, 4, 1],
                   f"{multi} (expected [2, 3, 4, 3, 4, 1])"))
    pk = txs[-1] if txs else None
    checks.append(("primary-key update appears as", None, f"{pk} (1/2 = delete+insert, 3/4 = update pair)"))

    nxt = scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_increment_lsn(CONVERT(binary(10), ?, 1)), 1)", (end,))
    empty = _changes(conn, nxt, end)
    checks.append(("empty interval (from > to) returns no rows", empty == [], f"{len(empty)} rows"))

    if a.destructive:
        old_min = scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn('dbo_orders'), 1)")
        rows(conn, "DECLARE @lw binary(10) = sys.fn_cdc_get_max_lsn(); "
                   "EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'dbo_orders', "
                   "@low_water_mark = @lw, @threshold = 5000;")
        new_min = scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn('dbo_orders'), 1)")
        checks.append(("cleanup moved min_lsn forward", new_min > old_min, f"{old_min} -> {new_min}"))
        below = scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_decrement_lsn(CONVERT(binary(10), ?, 1)), 1)",
                       (new_min,))
        left = _changes(conn, old_min, below)
        checks.append(("purged range reads as empty (so the reader re-checks min_lsn)", left == [],
                       f"{len(left)} rows left below the new min_lsn"))

    return report("t3_read_semantics", checks, {"changes": changes})


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
