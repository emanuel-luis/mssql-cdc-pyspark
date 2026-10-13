"""t11: what each stream of ``start_many`` costs the driver (no SQL Server needed).

For 1, 5, 10 and 20 tables of the file-backed fake, each committing a row every second,
starts one ``to_delta`` stream per table with ``start_many`` (a ``processingTime`` trigger,
one shared facts table), lets them run, and measures on the driver:

* the Python processes Spark started there, by kind: a Python streaming data source plans
  (``latestOffset``, ``partitions``) in a process of its own per query
  (``python_streaming_source_runner``); on Spark Connect, ``foreachBatch`` runs in another
  (``foreach_batch_worker``); ``pyspark.daemon`` forks pooled workers;
* the PSS and threads of the driver JVM (its heap after a full GC, classic only), of those
  processes, and of this Python process, where a classic ``foreachBatch`` runs;
* per trigger, from the queries' progress: ``latestOffset`` and ``queryPlanning`` (planning),
  ``addBatch`` (the sink: bronze and the facts) and ``triggerExecution`` (all of it).

The executors run in JVMs of their own (``--master local-cluster[...]``), so the driver JVM
holds the driver alone. A run of one stream warms the JVM first; then each size is measured
against the session before its first start, once every query has run three batches, and
its streams are stopped before the next size starts. Linux only (it reads ``/proc``). The
fake's ``latestOffset`` reads local files, where SQL Server takes a round trip: planning
against a real source adds the latency the facts show as ``source_rtt_ms``.

Locally:        python -m lab.checks.t11_fanout_cost [--streams 1,5,10,20] [--seconds 90]
Spark Connect:  SPARK_REMOTE=sc://localhost:15002 python -m lab.checks.t11_fanout_cost,
                on the server's host: it measures the JVM that runs SparkConnectServer.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import threading
import time
from collections import Counter
from datetime import datetime

from mssql_cdc import start_many, stop_all
from mssql_cdc.fake import FakeCdcDatabase
from mssql_cdc.spark import get_spark

from ..common import progress, report

KINDS = ("python_streaming_source_runner", "foreach_batch_worker", "pyspark.daemon")


def _procs() -> dict[int, tuple[int, str]]:
    """pid -> (parent pid, command line) of every process."""
    out = {}
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat") as f:
                stat = f.read()
            with open(f"/proc/{name}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\0", b" ").decode(errors="replace").strip()
        except OSError:  # exited meanwhile
            continue
        out[int(name)] = (int(stat.rsplit(")", 1)[1].split()[1]), cmd)
    return out


def _java(cmd: str) -> bool:
    return cmd.split(" ")[0].endswith("java")


def _below(procs: dict[int, tuple[int, str]], root: int) -> list[int]:
    """``root``'s descendants, but those of a JVM below it: with ``local-cluster``, the
    executors' JVMs, with their Python workers, are the driver JVM's children."""
    kids: dict[int, list[int]] = {}
    for pid, (ppid, _) in procs.items():
        kids.setdefault(ppid, []).append(pid)
    out, todo = [], list(kids.get(root, ()))
    while todo:
        pid = todo.pop()
        out.append(pid)
        if not _java(procs[pid][1]):
            todo += kids.get(pid, ())
    return out


def _status(pid: int) -> tuple[int, int]:
    """(PSS in MB, threads); (0, 0) once it exited. PSS splits the pages forked workers
    share among them, so it sums; RSS would count them once per process."""
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            pss = next(int(line.split()[1]) for line in f if line.startswith("Pss:"))
        with open(f"/proc/{pid}/status") as f:
            threads = next(int(line.split()[1]) for line in f if line.startswith("Threads:"))
    except (OSError, StopIteration):
        return 0, 0
    return pss // 1024, threads


def _jvm(remote: bool) -> int:
    """The driver JVM: this process's child, or the Spark Connect server's."""
    procs = _procs()
    if remote:
        found = [
            p for p, (_, cmd) in procs.items() if "SparkConnectServer" in cmd and "java" in cmd
        ]
    else:
        found = [p for p in _below(procs, os.getpid()) if _java(procs[p][1])]
    if len(found) != 1:
        raise RuntimeError(f"expected one driver JVM, found {found}")
    return found[0]


def sample(jvm: int, spark=None) -> dict:
    """The driver's processes now: the JVM's PSS and threads (and, given a classic
    ``spark``, its heap in use after a full GC), its Python children by kind with their
    PSS, and this process's PSS and threads."""
    procs = _procs()
    kinds: Counter[str] = Counter()
    mb: Counter[str] = Counter()
    for pid in _below(procs, jvm):
        if "python" in procs[pid][1] and not _java(procs[pid][1]):
            kind = next((k for k in KINDS if k in procs[pid][1]), "other")
            kinds[kind] += 1
            mb[kind] += _status(pid)[0]
    pss, threads = _status(jvm)
    me_pss, me_threads = _status(os.getpid())
    names: Counter[str] = Counter()  # the JVM's threads by name, digits dropped
    for tid in os.listdir(f"/proc/{jvm}/task"):
        try:
            with open(f"/proc/{jvm}/task/{tid}/comm") as f:
                names[f.read().strip().rstrip("0123456789-# ")] += 1
        except OSError:
            continue
    heap = None
    if spark is not None and hasattr(spark, "_jvm"):  # classic only
        runtime = spark._jvm.java.lang.Runtime.getRuntime()
        spark._jvm.java.lang.System.gc()
        heap = (runtime.totalMemory() - runtime.freeMemory()) // 2**20
    return {
        "jvm_pss_mb": pss,
        "jvm_threads": threads,
        "jvm_heap_used_mb": heap,
        "jvm_thread_names": dict(names.most_common(15)),
        "python": dict(kinds),
        "python_pss_mb": dict(mb),
        "client_pss_mb": me_pss,
        "client_threads": me_threads,
    }


def _stats(values: list[float]) -> dict:
    if not values:
        return {}
    values = sorted(values)
    p95 = values[min(len(values) - 1, round(0.95 * (len(values) - 1)))]
    return {"median": statistics.median(values), "p95": p95, "n": len(values)}


def _batch(query) -> int:
    """The id of the query's last batch; -1 before the first."""
    last = query.lastProgress
    if last is None:
        return -1
    return json.loads(last.json)["batchId"] if hasattr(last, "json") else last["batchId"]


def _per_stream(before: dict, running: dict, n: int) -> dict:
    """What ``n`` running streams added to the driver, per stream."""
    out = {}
    for key in ("jvm_threads", "jvm_heap_used_mb", "jvm_pss_mb", "client_threads", "client_pss_mb"):
        if running[key] is not None and before[key] is not None:
            out[key] = round((running[key] - before[key]) / n, 1)
    for kind in KINDS[:2]:
        count = running["python"].get(kind, 0) - before["python"].get(kind, 0)
        if count:
            out[f"{kind} processes"] = round(count / n, 2)
            out[f"{kind} pss_mb each"] = round(running["python_pss_mb"].get(kind, 0) / count)
    return out


def run(spark, jvm: int, n: int, a, workdir: str) -> dict:
    tables = [f"dbo_t{i:02d}" for i in range(n)]
    src = os.path.join(workdir, "src")
    db = FakeCdcDatabase(src, tables, columns={t: "id INT, note STRING" for t in tables})
    stop = threading.Event()

    def writer() -> None:  # one transaction a second on every table
        i = 0
        while not stop.wait(1):
            i += 1
            for t in tables:
                db.commit(t, [(2, {"id": i, "note": "x" * 50})])

    thread = threading.Thread(target=writer, daemon=True)
    before = sample(jvm, spark)
    started = time.monotonic()
    thread.start()
    try:
        queries = start_many(
            spark,
            {"backend": "fake", "fakePath": src, "numPartitions": "1"},
            tables,
            target=os.path.join(workdir, "bronze_{ci}"),
            app_id="{ci}-v1",
            checkpoint=os.path.join(workdir, "ckpt", "{ci}"),
            facts_table=os.path.join(workdir, "facts"),
            trigger={"processingTime": a.trigger},
        )
    except Exception as e:  # noqa: BLE001 - a start that raises is a result: start_many stopped all
        stop.set()
        return {"streams": n, "start_error": str(e).splitlines()[0][:300], "before": before}
    start_seconds = time.monotonic() - started
    try:
        deadline = time.monotonic() + 600  # every query past its first batches
        while any(_batch(q) < 2 for q in queries.values()):
            if time.monotonic() > deadline or not all(q.isActive for q in queries.values()):
                break
            time.sleep(1)
        warm_seconds = time.monotonic() - started
        window = time.time()
        samples = []
        while time.time() - window < a.seconds:
            time.sleep(min(10, a.seconds))
            samples.append(sample(jvm))
        running = sample(jvm, spark)
        failed = [ci for ci, q in queries.items() if q.exception() or not q.isActive]
        durations: dict[str, list[float]] = {}
        late = 0
        interval_ms = float(a.trigger.split()[0]) * 1000
        for q in queries.values():
            for p in progress(q):
                ts = datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")).timestamp()
                if ts < window or not p["numInputRows"]:
                    continue
                late += p["durationMs"].get("triggerExecution", 0) > interval_ms
                for key in ("latestOffset", "queryPlanning", "addBatch", "triggerExecution"):
                    durations.setdefault(key, []).append(p["durationMs"].get(key, 0))
    finally:
        stop.set()
        thread.join()
        stop_all(queries)
    deadline = time.monotonic() + 60  # the planning processes exit with their queries
    while True:
        after = sample(jvm)
        gone = after["python"].get(KINDS[0], 0) <= before["python"].get(KINDS[0], 0)
        if gone or time.monotonic() > deadline:
            break
        time.sleep(1)
    triggers = len(durations.get("triggerExecution", ()))
    return {
        "streams": n,
        "start_seconds": round(start_seconds, 1),
        "warm_seconds": round(warm_seconds, 1),
        "failed": failed,
        "before": before,
        "running": running,
        "after_stop": after,
        "peak_jvm_pss_mb": max(s["jvm_pss_mb"] for s in samples),
        "per_stream": _per_stream(before, running, n),
        "triggers_with_rows_per_stream_minute": round(triggers / n / (a.seconds / 60), 1),
        "triggers_over_interval": late,
        "durations_ms": {k: _stats(v) for k, v in durations.items()},
    }


def main(argv=None) -> bool:
    p = argparse.ArgumentParser()
    p.add_argument("--streams", default="1,5,10,20")
    p.add_argument("--seconds", type=float, default=60, help="measured window per size")
    p.add_argument("--trigger", default="5 seconds", help="processingTime, in seconds")
    p.add_argument(
        "--master", default="local-cluster[2,4,2048]", help="executors in JVMs of their own"
    )
    p.add_argument("--path", default=None, help="work directory (default: a new temporary one)")
    a = p.parse_args(argv)
    remote = "SPARK_REMOTE" in os.environ
    spark = get_spark("t11-fanout-cost", a.master)
    # one task per Delta log replay, as the test suite runs: the default 50 tasks for tables of
    # a few files make every local batch take 15 s or more, whatever the stream does
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "1")
    jvm = _jvm(remote)
    warm = argparse.Namespace(**{**vars(a), "seconds": 10})
    run(spark, jvm, 1, warm, tempfile.mkdtemp(prefix="t11-warm-", dir=a.path))  # JIT, Delta
    results = []
    for n in (int(x) for x in a.streams.split(",")):
        r = run(spark, jvm, n, a, tempfile.mkdtemp(prefix=f"t11-{n}-", dir=a.path))
        results.append(r)
        print(json.dumps(r, default=str), flush=True)
    runner = KINDS[0]
    where = "Spark Connect" if remote else spark.conf.get("spark.master")
    if hasattr(spark, "_jvm"):
        where += f", max heap {spark._jvm.java.lang.Runtime.getRuntime().maxMemory() // 2**20} MB"
    checks: list[tuple[str, bool | None, str]] = [("spark", None, f"{spark.version}, {where}")]
    for r in results:
        if "start_error" in r:
            checks.append((f"{r['streams']} streams: started", False, r["start_error"]))
            continue
        n, b, run_, d = r["streams"], r["before"], r["running"], r["durations_ms"]
        checks += [
            (
                f"{n} streams: every query ran",
                not r["failed"] and r["triggers_with_rows_per_stream_minute"] > 0,
                (
                    f"failed {r['failed']}; {r['triggers_with_rows_per_stream_minute']} triggers "
                    f"with rows per stream-minute, {r['triggers_over_interval']} longer than the "
                    f"interval; started in {r['start_seconds']} s, warm in {r['warm_seconds']} s"
                ),
            ),
            (
                f"{n} streams: one planning process per query, gone once it stops",
                run_["python"].get(runner, 0) - b["python"].get(runner, 0) == n
                and r["after_stop"]["python"].get(runner, 0) == b["python"].get(runner, 0),
                f"python {b['python']} -> {run_['python']} -> {r['after_stop']['python']}",
            ),
            (f"{n} streams: per stream", None, json.dumps(r["per_stream"])),
            (
                f"{n} streams: driver",
                None,
                (
                    f"JVM {b['jvm_pss_mb']} -> {run_['jvm_pss_mb']} MB PSS (peak "
                    f"{r['peak_jvm_pss_mb']}), heap after GC {b['jvm_heap_used_mb']} -> "
                    f"{run_['jvm_heap_used_mb']} MB, {b['jvm_threads']} -> "
                    f"{run_['jvm_threads']} threads; python {run_['python_pss_mb']} MB PSS"
                ),
            ),
            (
                f"{n} streams: ms per trigger with rows (median/p95)",
                None,
                ", ".join(f"{k} {v.get('median')}/{v.get('p95')}" for k, v in d.items()),
            ),
        ]
    return report("t11_fanout_cost", checks, {"results": results, "trigger": a.trigger})


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
