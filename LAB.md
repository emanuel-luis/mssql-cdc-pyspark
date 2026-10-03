# Lab: validating the design on a local SQL Server

Everything runs locally: SQL Server 2022 in Docker, PySpark 4.2 and Delta from
pip, synthetic data from Faker. Once it passes here, the same checks run on
Databricks (see [docs/DATABRICKS.md](docs/DATABRICKS.md)).

Each check prints `PASS`, `FAIL` or `INFO` lines and saves a JSON result under
`lab/results/`. Those files are the evidence behind any claim written about this
project.

## 0. Environment

```bash
cp .env.example .env                 # set MSSQL_SA_PASSWORD
docker compose up -d                 # wait until "healthy": docker compose ps
uv sync && . .venv/bin/activate      # per-platform setup: docs/DEVELOPMENT.md
python -m lab.workload setup         # database cdc_lab, dbo.customers, dbo.orders, CDC on both
python -m lab.workload seed --customers 500 --orders 2000
```

* SQL Server Agent must run (`MSSQL_AGENT_ENABLED=true` in the compose file): CDC
  capture and cleanup are Agent jobs.
* The SQL Server image is linux/amd64 only. On Apple Silicon, enable Rosetta in
  Docker Desktop.
* The first Spark run with Delta downloads jars from Maven Central.
* On Linux, `mssql-python` needs `libltdl7 libkrb5-3 libgssapi-krb5-2`.

## 1. Workload generator (Faker)

```bash
python -m lab.workload stream --tps 5 --duration 120 \
       --mix insert=0.5,update=0.35,delete=0.15 --max-statements 4
python -m lab.workload bulk --transactions 2000 --rows-per-tx 100
python -m lab.workload long-tx --seconds 120
```

Reproducible with `--seed`. Transactions mix statements, so change tables contain
all four operation codes.

## 2. Checks

Run them in this order. `t1` needs the database idle (no `sql/heartbeat.sql` job either), so don't run anything else
at the same time.

| Check | Hypothesis | Needs | Supports |
|---|---|---|---|
| `t1_idle_heartbeat` | `max_lsn` keeps advancing while the DB is idle (dummy entries in `cdc.lsn_time_mapping`), and how often | SQL Server | the core claim; how far finalization lags on a quiet database |
| `t2_timezone` | which clock `tran_end_time` follows; no commit-time regressions | SQL Server (run with `MSSQL_TZ=UTC` and with a non-UTC zone) | the `sourceTimeZone` option; finalization periods |
| `t3_read_semantics` | op codes and `__$command_id` ordering; PK update shape; `from > to` returns no rows; `--destructive`: a purged range reads as empty (why the reader re-checks `min_lsn`) | SQL Server | reader design; retention guard |
| `t4_watermark_concurrency` | no rows ever appear below an already observed `max_lsn`; LSN order is commit order | SQL Server | `max_lsn` as a safe low watermark |
| `t5_engine` | the runtime supports Python streaming sources with admission control and `AvailableNow` | Spark only | platform requirements |
| `t6_delta_semantics` | `userMetadata` on MERGE; idempotent append and MERGE | Spark + Delta | sink idempotency; facts |
| `t8_fetch_throughput` | rows/s of one connection over a wide change table: with/without `(max)` columns, Arrow batch size, named time zone, `fetchall()` baseline | SQL Server | read performance; informational |
| `t7_end_to_end` | bronze == change table up to the end LSN; restart and incremental runs; `--idle-minutes`: finalization advances without rows; `--destructive`: guard stops the stream | everything | the full pipeline, with per-batch timings |
| `t9_capture_instance_switch` | a table moved to a new capture instance with a new column (`sql/switch_capture_instance.sql` as written) under a continuous writer: the running query stops at the new start S and resumes on restart; bronze holds every change once, below S from the old instance and from S on from the new; its latest image equals the table; one `capture_instance_switched` event; the stream keeps running once the old instance is dropped | everything (its own table `dbo.t9_switch` and login) | ADR 0023; the DBA procedure |
| `t10_chunked_snapshot` | a chunked bootstrap read by `backfill()` next to the running stream (its first call plans every chunk from row counts per slice of the key), under a continuous writer (inserts, updates, deletes, key updates) and a transaction holding one range's locks: the chunks tile the key space, stamped at or after S, counted in the facts; no chunk row older than its stamp; a chunk waits for the held range under READ COMMITTED; bronze holds every change after S once; silver equals the table and `reconcile` matches. `--resnapshot`: a gap with deletes purged by a forced cleanup, recovered by a chunked re-snapshot in generation 1: the gap's deleted keys, which have no delete row, leave silver wave by wave (range deletes), none left below the chunks applied, and are all gone at the end | everything (its own table `dbo.t10_chunked` and login) | ADR 0028 |

```bash
python -m lab.checks.t2_timezone
python -m lab.checks.t3_read_semantics
python -m lab.checks.t4_watermark_concurrency --long-seconds 120
python -m lab.checks.t1_idle_heartbeat --minutes 11 --interval 10
python -m lab.checks.t5_engine
python -m lab.checks.t6_delta_semantics
python -m lab.checks.t8_fetch_throughput --rows 200000   # informational; creates dbo.fetch_bench
python -m lab.checks.t7_end_to_end --idle-minutes 6    # idle entries come about every 5 min
python -m lab.checks.t9_capture_instance_switch        # recreates dbo.t9_switch; ~7 min
python -m lab.checks.t10_chunked_snapshot              # recreates dbo.t10_chunked; ~15 min
python -m lab.checks.t10_chunked_snapshot --resnapshot # cleans up its own change table
# destructive, last:
python -m lab.checks.t3_read_semantics --destructive
python -m lab.checks.t7_end_to_end --destructive
```

For timezone behaviour, recreate the container with `MSSQL_TZ=America/Sao_Paulo` in
`.env`, then rerun `t2` and `t7`. `sourceTimeZone=auto` (the default) picks the zone up
from `CURRENT_TIMEZONE_ID()`; on SQL Server 2019 or older it applies the server's current
UTC offset, so set `MSSQL_SOURCE_TZ` to the Windows zone name when the zone has daylight
saving.

Another SQL Server version runs from `MSSQL_IMAGE` in a compose project of its own, so it
gets its own data volume (a newer version's databases do not open on an older one):

```bash
docker compose down                  # the 2022 container; its volume stays
MSSQL_IMAGE=mcr.microsoft.com/mssql/server:2017-latest docker compose -p cdc-lab-2017 up -d
python -m lab.workload setup
python -m lab.checks.t9_capture_instance_switch
docker compose -p cdc-lab-2017 down -v
```

## 3. If a check fails

* **`t1` or `t4` fails:** stop. The completeness signal depends on both; the
  design must change before anything else.
* **`t3` shows no `__$command_id` in the change table** (2012–2016 without the
  cumulative update): run with `includeCommandId=false`, and order by
  `(_start_lsn, _seqval, _operation)`.
* **`t6` MERGE idempotency fails:** the sink is unaffected (append + txn options).
  Downstream MERGEs must be idempotent by key instead.

## 4. Results

Evidence files are in the `lab-results` artifact of the CI run linked in each row
(GitHub keeps artifacts for 90 days; start the workflow with "Run workflow" to regenerate
them, since re-running a push run skips t1 and the destructive t7).

| Check | Platform / version | Result | Evidence (`lab/results/...`) |
|---|---|---|---|
| t1 | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS; idle entries every 305 s, `max_lsn` up to 300 s stale | [`t1_idle_heartbeat-20260928T222216Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t2 | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS | [`t2_timezone-20260928T220645Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t3 | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS (`--destructive` also passed in a local run) | [`t3_read_semantics-20260928T220651Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t4 | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS | [`t4_watermark_concurrency-20260928T220811Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t5 | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS | [`t5_engine-20260928T220829Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t6 | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS | [`t6_delta_semantics-20260928T220927Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t7 | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS | [`t7_end_to_end-20260928T221114Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t5 | DBR 18.2 (Spark 4.1.0), dedicated single node, Azure | PASS | one-off job run; result kept in the workspace copy of `lab/results` |
| t6 `--schema` | DBR 18.2 (Spark 4.1.0), dedicated single node, Unity Catalog managed tables | PASS | same run |
| t7 `--idle-minutes 6 --destructive` | SQL Server 2022 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, CI | PASS: idle offset advanced, guard stopped the stream | [`t7_end_to_end-20260928T223005Z.json`](https://github.com/emanuel-luis/mssql-cdc-pyspark/actions/runs/36490066001) |
| t8 | SQL Server 2022 CU27, local | INFO: 200k rows; Arrow with two (max) columns 22–26k rows/s vs 34–39k without (batch 10k), 28–30k vs 34–35k (batch 50k); fetchall 10–18k | local runs 2026-09-29 |
| t9 | SQL Server 2022 CU27 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, local | PASS: the query stopped at S and resumed on restart; 3 batches after the old instance was dropped; 6,967 + 4,816 change rows (old + new instance) once each; latest image == table (3,631 rows) | `t9_capture_instance_switch-20261001T203738Z.json`, local run 2026-10-01 |
| t9 | SQL Server 2017 CU31 (`2017-latest`), PySpark 4.2.0, delta-spark 4.4.0, local | PASS: the same; 7,616 + 4,187 change rows; latest image == table (3,733 rows) | `t9_capture_instance_switch-20261001T204603Z.json`, local run 2026-10-01 |
| t10 | SQL Server 2022 CU27 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, local | PASS: 3,000 rows before CDC; the first backfill call planned 14 chunks (`chunk_rows` 200), read in 7 waves next to the running stream under 16,207 writer transactions, 63 to 186 rows each as the writer churned the table; 0 of 1,930 chunk rows older than their stamp; a chunk waited (`LCK_M_S`) on the held range; 15,515 change rows after S, each once; silver == table (4,155 rows); `reconcile` 9 buckets MATCH, no chunk failures | `t10_chunked_snapshot-20261003T180451Z.json`, local run 2026-10-03 |
| t10 `--resnapshot` | SQL Server 2022 CU27 (`2022-latest`), PySpark 4.2.0, delta-spark 4.4.0, local | PASS: a forced cleanup purged a gap with 60 deleted keys; generation 1 opened a chunked snapshot, planned in 15 chunks and read in 8 waves under 16,493 writer transactions, closed by its `resnapshot` row; after each wave silver held 46, 32, 20, 14, 9, 4, then 1 of the 60 keys, none below the chunks applied, and 0 after the rebuild; 0 of 2,017 chunk rows stale; 15,695 change rows after S once; silver == table (4,203 rows), no delete row for the 60 keys | `t10_chunked_snapshot-20261003T182143Z.json`, local run 2026-10-03 |
