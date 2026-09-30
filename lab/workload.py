"""Synthetic OLTP workload for the CDC lab, generated with Faker.

    python -m lab.workload setup
    python -m lab.workload seed --customers 500 --orders 2000
    python -m lab.workload stream --tps 5 --duration 120
    python -m lab.workload bulk --transactions 2000 --rows-per-tx 100
    python -m lab.workload long-tx --seconds 120

Every run is reproducible with --seed. Transactions mix inserts, updates and
deletes so the change tables contain all four CDC operation codes.
"""

from __future__ import annotations

import argparse
import random
import time
from decimal import Decimal

from faker import Faker

from .common import DATABASE, ROOT, connect, run_script, scalar

STATUSES = ["new", "paid", "shipped", "delivered", "cancelled"]


class Workload:
    def __init__(self, seed: int = 42, locale: str = "en_US"):
        self.fake = Faker(locale)
        Faker.seed(seed)
        self.rnd = random.Random(seed)
        self.conn = connect(autocommit=False)
        self.next_customer = (scalar(self.conn, "SELECT ISNULL(MAX(customer_id), 0) FROM dbo.customers") or 0) + 1
        self.next_order = (scalar(self.conn, "SELECT ISNULL(MAX(order_id), 0) FROM dbo.orders") or 0) + 1
        self.conn.commit()

    # -- single statements ------------------------------------------------------
    def insert_customer(self, cur) -> int:
        cid = self.next_customer
        self.next_customer += 1
        cur.execute(
            "INSERT INTO dbo.customers (customer_id, name, email, city) VALUES (?, ?, ?, ?)",
            (cid, self.fake.name(), self.fake.unique.email(), self.fake.city()),
        )
        return cid

    def insert_order(self, cur, customer_id: int | None = None) -> int:
        oid = self.next_order
        self.next_order += 1
        amount = Decimal(self.rnd.randint(500, 250_000)) / 100
        cid = customer_id or self.rnd.randint(1, max(1, self.next_customer - 1))
        cur.execute(
            "INSERT INTO dbo.orders (order_id, customer_id, status, amount) VALUES (?, ?, 'new', ?)",
            (oid, cid, amount),
        )
        return oid

    def update_order(self, cur) -> None:
        cur.execute(
            "UPDATE dbo.orders SET status = ?, amount = amount + ?, updated_at = SYSUTCDATETIME() "
            "WHERE order_id = (SELECT TOP 1 order_id FROM dbo.orders WITH (READPAST) "
            "WHERE order_id >= ? ORDER BY order_id)",
            (self.rnd.choice(STATUSES[1:]), Decimal(self.rnd.randint(0, 500)) / 100,
             self.rnd.randint(1, max(1, self.next_order - 1))),
        )

    def delete_order(self, cur) -> None:
        cur.execute(
            "DELETE FROM dbo.orders WHERE order_id = (SELECT TOP 1 order_id FROM dbo.orders "
            "WITH (READPAST) WHERE order_id >= ? ORDER BY order_id)",
            (self.rnd.randint(1, max(1, self.next_order - 1)),),
        )

    # -- scenarios ----------------------------------------------------------------
    def seed_data(self, customers: int, orders: int, batch: int = 500) -> None:
        cur = self.conn.cursor()
        for i in range(customers):
            self.insert_customer(cur)
            if (i + 1) % batch == 0:
                self.conn.commit()
        self.conn.commit()
        for i in range(orders):
            self.insert_order(cur)
            if (i + 1) % batch == 0:
                self.conn.commit()
        self.conn.commit()
        cur.close()
        print(f"seeded {customers} customers, {orders} orders")

    def transaction(self, mix: dict[str, float], max_statements: int) -> int:
        cur = self.conn.cursor()
        n = self.rnd.randint(1, max_statements)
        ops = self.rnd.choices(list(mix), weights=list(mix.values()), k=n)
        for op in ops:
            if op == "insert":
                if self.rnd.random() < 0.2:
                    self.insert_order(cur, self.insert_customer(cur))
                else:
                    self.insert_order(cur)
            elif op == "update":
                self.update_order(cur)
            else:
                self.delete_order(cur)
        self.conn.commit()
        cur.close()
        return n

    def stream(self, tps: float, duration: float, mix: dict[str, float], max_statements: int) -> None:
        end, count, stmts = time.time() + duration, 0, 0
        interval = 1.0 / tps
        while time.time() < end:
            t = time.time()
            stmts += self.transaction(mix, max_statements)
            count += 1
            time.sleep(max(0.0, interval - (time.time() - t)))
        print(f"committed {count} transactions ({stmts} statements)")

    def bulk(self, transactions: int, rows_per_tx: int) -> None:
        cur = self.conn.cursor()
        for _ in range(transactions):
            for _ in range(rows_per_tx):
                self.insert_order(cur)
            self.conn.commit()
        cur.close()
        print(f"committed {transactions} transactions x {rows_per_tx} rows")

    def long_transaction(self, seconds: float) -> int:
        cur = self.conn.cursor()
        oid = self.insert_order(cur)
        time.sleep(seconds)
        self.conn.commit()
        cur.close()
        return oid

    def close(self) -> None:
        self.conn.close()


def _mix(text: str) -> dict[str, float]:
    out = {}
    for part in text.split(","):
        k, v = part.split("=")
        if k not in ("insert", "update", "delete"):
            raise argparse.ArgumentTypeError(f"unknown op {k}")
        out[k] = float(v)
    return out


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--locale", default="en_US")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", help="create database, tables and enable CDC")
    s = sub.add_parser("seed")
    s.add_argument("--customers", type=int, default=500)
    s.add_argument("--orders", type=int, default=2000)
    s = sub.add_parser("stream")
    s.add_argument("--tps", type=float, default=5)
    s.add_argument("--duration", type=float, default=60)
    s.add_argument("--mix", type=_mix, default=_mix("insert=0.5,update=0.35,delete=0.15"))
    s.add_argument("--max-statements", type=int, default=4)
    s = sub.add_parser("bulk")
    s.add_argument("--transactions", type=int, default=2000)
    s.add_argument("--rows-per-tx", type=int, default=100)
    s = sub.add_parser("long-tx")
    s.add_argument("--seconds", type=float, default=120)
    args = p.parse_args(argv)

    if args.cmd == "setup":
        run_script(ROOT / "sql" / "00_setup.sql", {"DATABASE": DATABASE})
        return
    w = Workload(args.seed, args.locale)
    try:
        if args.cmd == "seed":
            w.seed_data(args.customers, args.orders)
        elif args.cmd == "stream":
            w.stream(args.tps, args.duration, args.mix, args.max_statements)
        elif args.cmd == "bulk":
            w.bulk(args.transactions, args.rows_per_tx)
        elif args.cmd == "long-tx":
            print("order_id", w.long_transaction(args.seconds))
    finally:
        w.close()


if __name__ == "__main__":
    main()
