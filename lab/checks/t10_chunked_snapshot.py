"""t10: a chunked snapshot read next to the running stream under a continuous writer (ADR 0028).

``dbo.t10_chunked`` gets ``--rows`` rows before CDC, so only a snapshot has them. A writer
thread then commits continuously (an insert, an update, a delete or a key update, sometimes
several in one transaction) while ``stream().to_delta(snapshot="chunked")`` reads the change
table from the snapshot's LSN S into local Delta paths as a least-privilege login, and
``backfill()`` reads the table in small chunks next to it, wave by wave, each followed by
``apply_changes``. Its first call plans every chunk from row counts per slice of the key (the
'snapshot_plan' facts row); the result records the rows each chunk read. One transaction
holds the locks of a range of keys until a chunk read waits on them (READ COMMITTED), then
commits.

1. backfill completes: chunks tile the key space, each stamped at or after S, its facts row
   counting its bronze rows (``reconcile(facts_table=...)``), no key twice in the snapshot;
2. non-stale: every chunk row holds the key's image as of the chunk's stamp L, or one a commit
   after L wrote, never an older one (replayed from the change table);
3. a chunk read waited on the transaction holding its range's locks;
4. bronze holds every change after S once; after quiescing, silver equals the table (full
   diff) and ``reconcile`` finds every bucket MATCH.

``--resnapshot``: first a chunked bootstrap, read whole; then, with the stream stopped, the
writer's changes (deletes among them) and a forced cleanup up to ``max_lsn`` purge what the
stream has not read. ``to_delta(on_data_loss="resnapshot", snapshot="chunked")`` opens a new
chunked snapshot in generation 1 and the checks above run on it, plus: the keys deleted in the
purged gap have no delete row in bronze and are gone from silver at the end; after each wave
before the completion, silver holds none of them below the end of the chunks applied (each
wave's range deletes remove them, since the key is one integer) and fewer than at first;
facts hold its 'resnapshot' row.

Needs ``python -m lab.workload setup`` (the database). Recreates ``dbo.t10_chunked`` each run.

    python -m lab.checks.t10_chunked_snapshot
    python -m lab.checks.t10_chunked_snapshot --resnapshot
"""

import argparse
import json
import os
import random
import re
import sys
import tempfile
import threading
import time

from mssql_cdc import apply_changes, reconcile, stream
from mssql_cdc.lsn import normalize
from mssql_cdc.spark import get_spark

from ..common import SOURCE_TZ, connect, connection_string, max_lsn, report, rows, scalar

TABLE, CI, READER, APP = "t10_chunked", "dbo_t10_chunked", "t10_reader", "t10"


def _setup(conn, n: int) -> str:
    """``dbo.t10_chunked`` with ``n`` rows, CDC on it, and a login that may read only the table
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
        "ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'old' "
        "FROM sys.all_objects a CROSS JOIN sys.all_objects b",
    )
    rows(
        conn,
        f"EXEC sys.sp_cdc_enable_table 'dbo', '{TABLE}', @capture_instance = '{CI}', "
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
    rows(conn, f"GRANT SELECT ON cdc.[{CI}_CT] TO [{READER}]")
    return re.sub(r"UID=[^;]*;", f"UID={READER};", connection_string())


def _writer(top: int, skip: range, seed: int):
    """Commit continuously on its own connection: an update, a delete, a key update (``id`` to
    ``id + 1``) or an insert of a random key up to ``top`` (outside ``skip``, the held range),
    or an insert above it; every tenth transaction makes three changes. Values never repeat.
    Returns ``finish()``, which stops it and returns the number of commits."""
    stop, state = threading.Event(), {"error": None, "commits": 0}

    def change(cur, rnd, i: int) -> None:
        x = rnd.randrange(1, top + 1)
        while x in skip or x + 1 in skip:
            x = rnd.randrange(1, top + 1)
        op, t = rnd.random(), f"dbo.{TABLE}"
        if op < 0.35:
            cur.execute(f"UPDATE {t} SET v = ? WHERE id = ?", (f"u{i}", x))
        elif op < 0.55:
            cur.execute(f"DELETE FROM {t} WHERE id = ?", (x,))
        elif op < 0.7:  # CDC records a key update as the old key's delete and the new one's insert
            cur.execute(f"UPDATE {t} SET id = id + 1 WHERE id = ?", (x,))
        else:
            key = x if op < 0.85 else top + 100_000 + i
            cur.execute(f"INSERT INTO {t} VALUES (?, ?)", (key, f"i{i}"))

    def loop():
        conn = connect(autocommit=False)
        cur, rnd, i = conn.cursor(), random.Random(seed), 0
        try:
            while not stop.is_set():
                try:
                    for j in range(3 if i % 10 == 0 else 1):
                        change(cur, rnd, i * 3 + j)
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    if "PRIMARY KEY" not in str(exc) and "deadlock victim" not in str(exc):
                        raise
                i += 1
                state["commits"] += 1
                time.sleep(0.02)
        except Exception as exc:  # noqa: BLE001 - reported by finish()
            state["error"] = exc
        finally:
            conn.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def finish() -> int:
        stop.set()
        thread.join()
        if state["error"] is not None:
            raise state["error"]
        return state["commits"]

    return finish


def _holder(held: range, timeout: float):
    """Update the keys in ``held`` in a transaction that keeps their locks until a session of
    the reader login waits on it (a chunk read), then commits. Returns ``finish()``: the wait
    types seen and the commit's LSN, once it has committed."""
    conn = connect(autocommit=False)
    cur = conn.cursor()
    cur.execute("SELECT @@SPID")
    spid = cur.fetchone()[0]
    cur.execute(
        f"UPDATE dbo.{TABLE} SET v = 'held' WHERE id >= ? AND id < ?", (held.start, held.stop)
    )
    state: dict = {"waits": {}, "error": None}
    watch = connect()

    def loop():
        try:
            deadline = time.time() + timeout
            while not state["waits"] and time.time() < deadline:
                state["waits"] = dict(
                    rows(
                        watch,
                        "SELECT r.session_id, r.wait_type FROM sys.dm_exec_requests r "
                        "JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id "
                        "WHERE r.blocking_session_id = ? AND s.login_name = ?",
                        (spid, READER),
                    )
                )
                time.sleep(0.5)
            time.sleep(2)  # the read keeps waiting meanwhile
            conn.commit()
        except Exception as exc:  # noqa: BLE001 - reported by finish()
            state["error"] = exc
        finally:
            conn.close()
            watch.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def finish() -> dict:
        thread.join()
        if state["error"] is not None:
            raise state["error"]
        return state["waits"]

    return finish


def _quiesce(conn, key: int, value: str) -> str:
    """Insert one last row and wait for capture to reach it; returns ``max_lsn`` then."""
    rows(conn, f"INSERT INTO dbo.{TABLE} VALUES (?, ?)", (key, value))
    deadline = time.time() + 120
    while not scalar(conn, f"SELECT COUNT(*) FROM cdc.[{CI}_CT] WHERE v = ?", (value,)):
        if time.time() > deadline:
            raise TimeoutError("capture did not reach the last commit")
        time.sleep(1)
    return normalize(max_lsn(conn))


def _stale(conn, chunk_rows: list, base: dict, after: str | None) -> list:
    """The chunk rows ``(id, v, L)`` whose image is older than their stamp L: not the key's
    image as of L (``base``, the table at ``after`` or before CDC, then the change table's
    commits up to L) nor one a commit after L wrote. The writer's values never repeat."""
    sql = (
        f"SELECT id, v, __$operation, CONVERT(varchar(22), __$start_lsn, 1) FROM cdc.[{CI}_CT] "
        "WHERE __$operation IN (1, 2, 4) ORDER BY __$start_lsn, __$seqval, __$operation"
    )
    history: dict = {}
    for k, v, op, lsn in rows(conn, sql):
        lsn = normalize(lsn)
        if after is None or lsn > after:
            history.setdefault(k, []).append((lsn, op, v))
    stale = []
    for k, v, stamp in chunk_rows:
        image, later = base.get(k), set()
        for lsn, op, value in history.get(k, []):
            if lsn <= stamp:
                image = None if op == 1 else value
            elif op != 1:
                later.add(value)
        if v != image and v not in later:
            stale.append((k, v, stamp))
    return stale


def _behind(spark, facts: str, silver: str, status: dict, gone: list) -> tuple[int, int]:
    """Of the keys deleted in the purged gap (``gone``), how many silver still holds with an
    image older than the snapshot's S: below the end of the chunks applied so far (which
    their range deletes must have removed), and in all."""
    from pyspark.sql import functions as F

    s, n = status["snapshot"], status["chunks_done"]
    plan = (
        spark.read.format("delta")
        .load(facts)
        .where(
            (F.col("event") == "snapshot_plan") & (F.get_json_object("detail", "$.snapshot") == s)
        )
        .first()
    )
    old = (
        spark.read.format("delta")
        .load(silver)
        .where((F.col("_start_lsn") < s) & F.col("id").isin(gone))
    )
    hi = json.loads(plan["detail"])["chunks"][n - 1][1] if plan and n else -(2**31)
    return (old.count() if hi is None else old.where(F.col("id") < hi).count(), old.count())


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=3000, help="rows in the table before CDC")
    p.add_argument("--chunk-rows", type=int, default=200, help="backfill(chunk_rows=...)")
    p.add_argument("--partitions", type=int, default=2, help="numPartitions: chunks per wave")
    p.add_argument("--hold-timeout", type=float, default=600, help="seconds the locks wait at most")
    p.add_argument(
        "--resnapshot",
        action="store_true",
        help="purge a gap the stream has not read; the chunked re-snapshot recovers it",
    )
    a = p.parse_args(argv)

    conn = connect()
    options = {
        "connectionString": _setup(conn, a.rows),
        "captureInstance": CI,
        "sourceTimeZone": SOURCE_TZ,
        "numPartitions": str(a.partitions),
    }
    base_dir = tempfile.mkdtemp(prefix="t10-")
    bronze, silver, facts, control, ckpt = (
        os.path.join(base_dir, n) for n in ("bronze", "silver", "facts", "control", "ckpt")
    )
    spark = get_spark("t10-chunked")
    cdc = stream(spark, options)

    def start(**trigger):
        return cdc.to_delta(
            bronze,
            APP,
            ckpt,
            facts,
            trigger=trigger,
            bootstrap=True,
            on_data_loss="resnapshot" if a.resnapshot else "fail",
            snapshot="chunked",
        )

    def apply():
        return apply_changes(
            spark,
            bronze,
            silver,
            capture_instance=CI,
            keys=["id"],
            control_table=control,
            facts_table=facts,
        )

    def facts_df():
        return spark.read.format("delta").load(facts)

    checks: list = []
    extra: dict = {"server": scalar(conn, "SELECT @@VERSION").splitlines()[0], "bronze": bronze}
    base, after, gap_deleted = {k: "old" for k in range(1, a.rows + 1)}, None, []
    if a.resnapshot:  # a whole chunked bootstrap, then a gap the stream never reads
        start(availableNow=True).awaitTermination()
        first = cdc.backfill(bronze, app_id=APP, facts_table=facts, chunk_rows=a.chunk_rows)
        start(availableNow=True).awaitTermination()
        apply()
        read = normalize(max_lsn(conn))  # the stream has read up to here: the gap starts after
        gap_deleted = [
            r[0] for r in rows(conn, f"DELETE FROM dbo.{TABLE} OUTPUT deleted.id WHERE id % 50 = 0")
        ]
        rows(conn, f"UPDATE dbo.{TABLE} SET v = CONCAT('gap', id) WHERE id % 7 = 0")
        rows(conn, f"INSERT INTO dbo.{TABLE} VALUES (?, 'gap')", (a.rows + 50_000,))
        low = _quiesce(conn, -2, "gap-end")
        rows(
            conn,
            "DECLARE @lw binary(10) = CONVERT(binary(10), ?, 1); "
            f"EXEC sys.sp_cdc_cleanup_change_table @capture_instance = N'{CI}', "
            "@low_water_mark = @lw, @threshold = 5000",
            (low,),
        )
        # the writer is stopped: the table holds every commit of it up to this max_lsn
        base = dict(rows(conn, f"SELECT id, v FROM dbo.{TABLE}"))
        after = normalize(max_lsn(conn))
        purged = normalize(
            scalar(conn, "SELECT CONVERT(varchar(22), sys.fn_cdc_get_min_lsn(?), 1)", (CI,))
        )
        # The cleanup may leave min_lsn below the low water mark asked for (CI saw it one
        # commit lower), so the check is what the stream needs: min_lsn past what it read, and
        # no delete row of the gap left in the change table.
        left = scalar(
            conn, f"SELECT COUNT_BIG(*) FROM cdc.[{CI}_CT] WHERE [__$operation] = 1 AND id % 50 = 0"
        )
        extra |= {"first_snapshot": first["snapshot"], "gap_deleted": len(gap_deleted)}
        checks.append(
            (
                "cleanup purged the gap the stream had not read",
                bool(gap_deleted) and purged > read and left == 0,
                (
                    f"{len(gap_deleted)} keys deleted in the gap, {left} delete rows left; "
                    f"min_lsn {purged}, read up to {read}, low water {low}"
                ),
            )
        )

    mid = a.rows // 2
    held = range(mid, mid + 10)
    finish = _writer(a.rows, held, seed=10)
    statuses, applied, waits, behind = [], [], {}, []
    try:
        q = start(processingTime="2 seconds")  # opens S (a re-snapshot: generation 1) at once
        release = _holder(held, a.hold_timeout)
        try:
            while not (statuses and statuses[-1]["done"]):
                statuses.append(
                    cdc.backfill(
                        bronze, app_id=APP, facts_table=facts, chunk_rows=a.chunk_rows, max_waves=1
                    )
                )
                applied.append(apply())
                if a.resnapshot and not statuses[-1]["done"]:  # the last call also rebuilds
                    behind.append(_behind(spark, facts, silver, statuses[-1], gap_deleted))
                if q.exception() is not None:
                    raise RuntimeError(f"the stream failed: {q.exception()}")
                if len(statuses) > 10 * a.rows // a.chunk_rows:
                    raise RuntimeError(f"backfill does not finish: {statuses[-1]}")
        finally:
            waits = release()
        q.stop()
    finally:
        commits = finish()
    end = _quiesce(conn, -1, "end")
    start(availableNow=True).awaitTermination()
    applied.append(apply())

    opens = facts_df().where("event = 'snapshot_open'").orderBy("min_lsn").collect()
    s = opens[-1]["min_lsn"]
    kind = "resnapshot" if a.resnapshot else "bootstrap"
    done = facts_df().where(f"event = '{kind}' AND min_lsn = '{s}' AND max_lsn = '{s}'").count()
    waves = len(statuses)
    checks.append(
        (
            f"backfill completes in waves; one '{kind}' row at S",
            statuses[-1]["done"] and done == 1,
            f"S={s}: {statuses[-1]['chunks_done']} chunks in {waves} waves, {done} completion row",
        )
    )
    if a.resnapshot:
        detail = opens[-1]["detail"]
        checks.append(
            (
                "the loss opened a newer chunked snapshot in generation 1",
                len(opens) == 2
                and opens[-1]["app_id"] == f"{APP}.g1"
                and '"kind": "resnapshot"' in detail
                and opens[-1]["lost_from_ts"] is not None,
                f"{[(o['app_id'], o['min_lsn']) for o in opens]}",
            )
        )
    result = reconcile(
        spark,
        options,
        silver,
        bronze=bronze,
        control_table=control,
        facts_table=facts,
        bucket_rows=500,
        sample=0.2,
    )
    checks.append(
        (
            "chunks tile the key space, counts match the facts, every L >= S (reconcile)",
            not any(t.startswith("CHUNK") for t in result["failures"]),
            str(result["failures"]),
        )
    )
    snap = (
        spark.read.format("delta")
        .load(bronze)
        .where(f"_operation = 0 AND _snapshot = '{s}'")
        .select("id", "v", "_start_lsn")
        .collect()
    )
    twice = len(snap) - len({r["id"] for r in snap})
    checks.append(("no key twice in the snapshot", twice == 0, f"{len(snap)} rows, {twice} twice"))
    stale = _stale(conn, [tuple(r) for r in snap], base, after)
    checks.append(
        (
            "non-stale: each chunk row is the image as of its stamp L or a later one",
            not stale,
            f"{len(stale)} stale of {len(snap)}: {stale[:5]}",
        )
    )
    checks.append(
        (
            "a chunk read waited on the transaction holding its range (READ COMMITTED)",
            bool(waits) and all(w.startswith("LCK_M_S") for w in waits.values()),
            str(waits),
        )
    )
    changes = (
        spark.read.format("delta").load(bronze).where(f"_operation != 0 AND _start_lsn > '{s}'")
    )
    dups = (
        changes.count() - changes.select("_start_lsn", "_seqval", "_operation").distinct().count()
    )
    in_ct = scalar(
        conn,
        f"SELECT COUNT(*) FROM cdc.[{CI}_CT] WHERE __$start_lsn > CONVERT(binary(10), ?, 1) "
        "AND __$start_lsn <= CONVERT(binary(10), ?, 1)",
        (s, end),
    )
    checks.append(
        (
            "bronze holds every change after S once",
            dups == 0 and changes.count() == in_ct,
            f"{changes.count()} rows, change table {in_ct}, {dups} duplicates",
        )
    )
    image = {tuple(r) for r in spark.read.format("delta").load(silver).select("id", "v").collect()}
    table = {tuple(r) for r in rows(conn, f"SELECT id, v FROM dbo.{TABLE}")}
    checks.append(
        (
            "silver == the table (full diff)",
            image == table,
            f"{len(table)} rows, {len(image ^ table)} differ: {sorted(image ^ table)[:5]}",
        )
    )
    checks.append(
        (
            "reconcile: every bucket MATCH, no failures",
            result["match"] == result["buckets"] and not result["failures"],
            f"{result['buckets']} buckets, {result['hashed']} hashed, {result['failures']}",
        )
    )
    if a.resnapshot:
        deleted = set(gap_deleted)
        before_s = f"_operation = 1 AND _start_lsn < '{s}'"  # the writer's come after S
        rows_of = spark.read.format("delta").load(bronze).where(before_s).select("id")
        no_delete_row = deleted.isdisjoint({r["id"] for r in rows_of.collect()})
        back = deleted & {k for k, _ in image}  # a key the writer inserted again is no failure
        back -= {k for k, _ in table}
        checks.append(
            (
                "keys deleted in the purged gap: no delete row, gone from silver",
                bool(deleted) and no_delete_row and not back,
                f"{len(deleted)} deleted in the gap, {len(back)} still in silver",
            )
        )
        checks.append(
            (
                "range deletes: each wave removed the gap's keys below its chunks, before completion",
                bool(behind) and all(b == 0 for b, _ in behind) and behind[-1][1] < len(deleted),
                f"(below the waves applied, left in silver) after each wave: {behind}",
            )
        )
    sizes = [
        r["rows"]
        for r in facts_df()
        .where(f"event = 'snapshot_chunk' AND get_json_object(detail, '$.snapshot') = '{s}'")
        .collect()
    ]
    return report(
        "t10_chunked_snapshot",
        checks,
        extra
        | {
            "mode": kind,
            "writer_commits": commits,
            "snapshot_lsn": s,
            "end_lsn": end,
            "chunks": statuses[-1]["chunks_done"],
            "waves": waves,
            "rows": a.rows,
            "chunk_rows": a.chunk_rows,
            "chunk_rows_read": {"min": min(sizes), "max": max(sizes)} if sizes else None,
            "rebuilt": any(x["rebuilt"] for x in applied),
        },
    )


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
