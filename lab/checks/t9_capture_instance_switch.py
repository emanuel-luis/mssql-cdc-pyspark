"""t9: a table switched to a new capture instance under a continuous writer (ADR 0023).

A writer thread commits into ``dbo.t9_switch`` (an insert, an update and every fifth time a
delete, one transaction each) while ``stream().to_delta`` reads it into local Delta paths as
a least-privilege login, after a bootstrap snapshot. Meanwhile the table gets a column and
``sql/switch_capture_instance.sql`` runs as written, with this check's names: a second
capture instance capturing the column and its grant, then, once the stream has written its
``capture_instance_switched`` event and one batch more, the old instance dropped.

1. the running query stops at the new instance's start S with ``SchemaChangedError`` (its
   schema lacks the column); restarted, it infers the column and goes on;
2. once the old instance is dropped, the stream keeps running;
3. bronze: no duplicate ``(_start_lsn, _seqval, _operation)``; below S every row from the old
   instance, from S on from the new one, as many as each change table holds; its latest image
   (``apply_changes``) equals the table, the new column included;
4. facts: one bootstrap, the ``schema_change`` of the ADD, and ``capture_instance_switched``
   at S.

Needs ``python -m lab.workload setup`` (the database). Recreates ``dbo.t9_switch`` each run.

    python -m lab.checks.t9_capture_instance_switch
"""

import argparse
import os
import random
import re
import sys
import tempfile
import threading
import time

from mssql_cdc import apply_changes, finalization, stream
from mssql_cdc.lsn import normalize
from mssql_cdc.spark import get_spark

from ..common import ROOT, SOURCE_TZ, connect, connection_string, ct_count, report, rows, scalar

TABLE, V1, V2, READER = "t9_switch", "dbo_t9_switch", "dbo_t9_switch_v2", "t9_reader"


def _execute(conn, sql: str, params=()):
    """``rows``, rerun when SQL Server picks it as a deadlock victim (the enable and the writer
    can collide; each rolls back whole)."""
    for attempt in range(5):
        try:
            return rows(conn, sql, params)
        except Exception as exc:
            if "deadlock victim" not in str(exc) or attempt == 4:
                raise
            time.sleep(1)


def _setup(conn, n: int) -> str:
    """``dbo.t9_switch`` with ``n`` rows, CDC on it, and a login that may read only the table
    and the change table (invariant 11). Returns the login's connection string."""
    rows(
        conn,
        f"IF OBJECT_ID('dbo.{TABLE}') IS NOT NULL BEGIN "
        f"IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.{TABLE}')) "
        f"EXEC sys.sp_cdc_disable_table 'dbo', '{TABLE}', 'all'; DROP TABLE dbo.{TABLE}; END",
    )
    rows(conn, f"CREATE TABLE dbo.{TABLE} (id INT NOT NULL PRIMARY KEY, v VARCHAR(20) NOT NULL)")
    rows(
        conn,
        f"INSERT INTO dbo.{TABLE} SELECT TOP ({int(n)}) "
        "ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'old' FROM sys.all_objects",
    )
    _execute(
        conn,
        f"EXEC sys.sp_cdc_enable_table 'dbo', '{TABLE}', @capture_instance = '{V1}', "
        "@role_name = NULL, @supports_net_changes = 0",
    )
    pwd = os.environ["MSSQL_SA_PASSWORD"]
    rows(conn, f"IF USER_ID('{READER}') IS NOT NULL DROP USER [{READER}]")
    rows(conn, f"IF SUSER_ID('{READER}') IS NOT NULL DROP LOGIN [{READER}]")
    rows(
        conn,
        f"CREATE LOGIN [{READER}] WITH PASSWORD = '{pwd.replace(chr(39), chr(39) * 2)}', "
        "CHECK_POLICY = OFF",
    )
    rows(conn, f"CREATE USER [{READER}] FOR LOGIN [{READER}]")
    rows(conn, f"GRANT SELECT ON dbo.{TABLE} TO [{READER}]")
    rows(conn, f"GRANT SELECT ON cdc.[{V1}_CT] TO [{READER}]")
    return re.sub(r"UID=[^;]*;", f"UID={READER};", connection_string())


def _procedure() -> list[str]:
    """The batches of the DBA procedure with this check's names: enable, grant, check, disable."""
    text = (ROOT / "sql" / "switch_capture_instance.sql").read_text()
    text = text.replace("orders", TABLE).replace("cdc_reader", READER)
    return [
        b for b in re.split(r"^\s*GO\s*$", text, flags=re.MULTILINE | re.IGNORECASE) if b.strip()
    ]


def _writer(first_id: int):
    """Commit continuously on its own connection; ``note`` set: write the new column too.
    Returns ``(note, finish)``; ``finish()`` stops it and returns its last id and commits."""
    stop, note, state = threading.Event(), threading.Event(), {"error": None, "commits": 0}
    state["last"] = first_id - 1

    def loop():
        conn = connect(autocommit=False)
        cur, rnd, i = conn.cursor(), random.Random(9), first_id
        try:
            while not stop.is_set():
                try:
                    if note.is_set():
                        cur.execute(
                            f"INSERT INTO dbo.{TABLE} (id, v, note) VALUES (?, 'new', ?)",
                            (i, f"n{i}"),
                        )
                        cur.execute(
                            f"UPDATE dbo.{TABLE} SET v = ?, note = ? WHERE id = ?",
                            (f"u{i}", f"n{i}", rnd.randrange(1, i)),
                        )
                    else:
                        cur.execute(f"INSERT INTO dbo.{TABLE} (id, v) VALUES (?, 'new')", (i,))
                        cur.execute(
                            f"UPDATE dbo.{TABLE} SET v = ? WHERE id = ?",
                            (f"u{i}", rnd.randrange(1, i)),
                        )
                    if i % 5 == 0:
                        cur.execute(f"DELETE FROM dbo.{TABLE} WHERE id = ?", (rnd.randrange(1, i),))
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    if "deadlock victim" in str(exc):  # the enable won: commit it again
                        continue
                    raise
                state["last"], i = i, i + 1
                state["commits"] += 1
                time.sleep(0.03)
        except Exception as exc:  # noqa: BLE001 - reported by finish()
            state["error"] = exc
        finally:
            conn.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def finish() -> tuple[int, int]:
        stop.set()
        thread.join()
        if state["error"] is not None:
            raise state["error"]
        return state["last"], state["commits"]

    return note, finish


def _wait(what: str, cond, q=None, timeout: float = 300) -> None:
    """Poll ``cond`` until true; fail when the query ``q`` fails or time runs out."""
    deadline = time.time() + timeout
    while not cond():
        if q is not None and q.exception() is not None:
            raise RuntimeError(f"waiting for {what}, the query failed: {q.exception()}")
        if time.time() > deadline:
            raise TimeoutError(f"timed out waiting for {what}")
        time.sleep(2)


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=200, help="rows in the table before CDC")
    p.add_argument(
        "--after-drop", type=int, default=3, help="batches to wait for once the old one is gone"
    )
    a = p.parse_args(argv)

    conn = connect()
    options = {
        "connectionString": _setup(conn, a.rows),
        "captureInstance": V1,
        "sourceTimeZone": SOURCE_TZ,
    }
    base = tempfile.mkdtemp(prefix="t9-")
    bronze, facts, ckpt, silver, control = (
        os.path.join(base, n) for n in ("bronze", "facts", "ckpt", "silver", "control")
    )
    spark = get_spark("t9-switch")

    def start(**trigger):
        return stream(spark, options).to_delta(
            bronze, "t9", ckpt, facts, trigger=trigger, bootstrap=True
        )

    def facts_df():
        return spark.read.format("delta").load(facts)

    def last_batch() -> int:
        return facts_df().selectExpr("coalesce(max(batch_id), -1)").first()[0]

    checks = []
    note, finish = _writer(a.rows + 1)
    try:
        q = start(processingTime="2 seconds")  # the snapshot first, then the old instance
        _wait("two batches of the old instance", lambda: last_batch() >= 1, q)

        rows(conn, f"ALTER TABLE dbo.{TABLE} ADD note VARCHAR(20) NULL")
        enable, grant, check, drop = _procedure()
        for step in (enable, grant, check):
            _execute(conn, step)
        note.set()  # from here on commits are past S: the new instance holds the note values
        s = normalize(
            scalar(
                conn,
                "SELECT CONVERT(varchar(22), start_lsn, 1) FROM cdc.change_tables "
                "WHERE capture_instance = ?",
                (V2,),
            )
        )

        # its schema lacks note, which the new instance captures: it stops before reading S
        try:
            q.awaitTermination(300)
            stopped = "still running" if q.isActive else "stopped without an error"
        except Exception as exc:  # noqa: BLE001 - the error is the check result
            stopped = str(exc)
        q.stop()
        line = next((x for x in stopped.splitlines() if "reaches capture instance" in x), stopped)
        checks.append(
            (
                "the running query stops at S, schema lacking the new column",
                f"reaches capture instance '{V2}'" in line,
                line.strip()[:200],
            )
        )

        q = start(processingTime="2 seconds")  # infers note and goes on from its checkpoint

        def switched_and_one_more() -> bool:
            ev = facts_df().where("event = 'capture_instance_switched'").collect()
            return bool(ev) and last_batch() > ev[0]["batch_id"]

        _wait("the switch event and one batch more", switched_and_one_more, q)
        snap = facts_df().where("event = 'bootstrap'").first()["max_lsn"]
        v1_rows = scalar(
            conn,
            f"SELECT COUNT(*) FROM cdc.[{V1}_CT] WHERE __$start_lsn > CONVERT(binary(10), ?, 1) "
            "AND __$start_lsn < CONVERT(binary(10), ?, 1)",
            (snap, s),
        )
        _execute(conn, drop)
        dropped_at = last_batch()
        _wait(
            f"{a.after_drop} batches after the drop",
            lambda: last_batch() >= dropped_at + a.after_drop,
            q,
        )
        checks.append(
            (
                "the stream keeps running once the old instance is dropped",
                q.isActive and q.exception() is None,
                f"batches {dropped_at} -> {last_batch()}",
            )
        )
        q.stop()
    finally:
        last, commits = finish()

    _wait(
        "capture of the last commit",
        lambda: scalar(conn, f"SELECT COUNT(*) FROM cdc.[{V2}_CT] WHERE id = ?", (last,)),
    )
    q = start(availableNow=True)
    q.awaitTermination()
    end = finalization.end_offset_from_progress(q.lastProgress)

    changes = spark.read.format("delta").load(bronze).where("_operation != 0")
    dups = (
        changes.count() - changes.select("_start_lsn", "_seqval", "_operation").distinct().count()
    )
    checks.append(("no duplicate (_start_lsn, _seqval, _operation)", dups == 0, str(dups)))
    sides = sorted(
        tuple(r)
        for r in changes.selectExpr(f"_start_lsn >= '{s}'", "_capture_instance")
        .distinct()
        .collect()
    )
    checks.append(
        (
            "below S from the old instance, from S on from the new one",
            sides == [(False, V1), (True, V2)],
            f"S={s}: {sides}",
        )
    )
    got = {ci: changes.where(f"_capture_instance = '{ci}'").count() for ci in (V1, V2)}
    want = {V1: v1_rows, V2: ct_count(conn, V2, end["lsn"])}
    checks.append(
        (
            "bronze rows per instance == change-table rows (snapshot, S) and [S, end]",
            got == want,
            f"{got} vs {want}",
        )
    )

    apply_changes(
        spark,
        bronze,
        silver,
        capture_instance=V1,
        keys=["id"],
        control_table=control,
        facts_table=facts,
        options=options,
    )
    image = {
        tuple(r)
        for r in spark.read.format("delta").load(silver).select("id", "v", "note").collect()
    }
    table = {tuple(r) for r in rows(conn, f"SELECT id, v, note FROM dbo.{TABLE}")}
    noted = sum(1 for r in table if r[2] is not None)
    checks.append(
        (
            "latest image of bronze (apply_changes) == the table, note included",
            image == table,
            f"{len(table)} rows ({noted} with note), {len(image ^ table)} differ",
        )
    )

    events = facts_df().where("event IS NOT NULL").select("event", "detail", "min_lsn").collect()
    kinds = sorted(e["event"] for e in events)
    checks.append(
        (
            "facts events: one snapshot (open, bootstrap), the ADD, one switch",
            kinds == ["bootstrap", "capture_instance_switched", "schema_change", "snapshot_open"],
            str(kinds),
        )
    )
    add = [e["detail"] for e in events if e["event"] == "schema_change"]
    checks.append(
        ("schema_change event names the ADD", any("ADD note" in d for d in add), str(add))
    )
    sw = [(e["detail"], e["min_lsn"]) for e in events if e["event"] == "capture_instance_switched"]
    checks.append(("capture_instance_switched at S", sw == [(f"{V1} -> {V2}", s)], str(sw)))
    return report(
        "t9_capture_instance_switch",
        checks,
        {
            "server": scalar(conn, "SELECT @@VERSION").splitlines()[0],
            "writer_commits": commits,
            "start_lsn_v2": s,
            "end_lsn": end["lsn"],
            "bronze": bronze,
        },
    )


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
