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

Run them in this order. `t1` needs the database idle, so don't run anything else
at the same time.

| Check | Hypothesis | Needs | Supports |
|---|---|---|---|
| `t1_idle_heartbeat` | `max_lsn` keeps advancing while the DB is idle (dummy entries in `cdc.lsn_time_mapping`) | SQL Server | the core claim; idle tables do not stall finalization |
| `t2_timezone` | which clock `tran_end_time` follows; no commit-time regressions | SQL Server (run with `MSSQL_TZ=UTC` and with a non-UTC zone) | the `sourceTimeZone` option; finalization periods |
| `t3_read_semantics` | op codes and `__$command_id` ordering; PK update shape; `from > to` errors; `--destructive`: purged range errors | SQL Server | reader design; retention guard |
| `t4_watermark_concurrency` | no rows ever appear below an already observed `max_lsn`; LSN order is commit order | SQL Server | `max_lsn` as a safe low watermark |
| `t5_engine` | the runtime supports Python streaming sources with admission control and `AvailableNow` | Spark only | platform requirements |
| `t6_delta_semantics` | `userMetadata` on MERGE; idempotent append and MERGE | Spark + Delta | sink idempotency; facts |
| `t7_end_to_end` | bronze == change table up to the end LSN; restart and incremental runs; `--idle-minutes`: finalization advances without rows; `--destructive`: guard stops the stream | everything | the full pipeline, with per-batch timings |

```bash
python -m lab.checks.t2_timezone
python -m lab.checks.t3_read_semantics
python -m lab.checks.t4_watermark_concurrency --long-seconds 120
python -m lab.checks.t1_idle_heartbeat --minutes 10 --interval 30
python -m lab.checks.t5_engine
python -m lab.checks.t6_delta_semantics
python -m lab.checks.t7_end_to_end --idle-minutes 5
# destructive, last:
python -m lab.checks.t3_read_semantics --destructive
python -m lab.checks.t7_end_to_end --destructive
```

For timezone behaviour, recreate the container with `MSSQL_TZ=America/Sao_Paulo` in
`.env`, then rerun `t2` and `t7`. `sourceTimeZone=auto` (the default) picks the zone up
from `CURRENT_TIMEZONE_ID()`; on SQL Server 2019 or older, set `MSSQL_SOURCE_TZ` to the
Windows zone name.

## 3. If a check fails

* **`t1` or `t4` fails:** stop. The completeness signal depends on both; the
  design must change before anything else.
* **`t3` shows no `__$command_id`:** run with `includeCommandId=false`, and order by
  `(_start_lsn, _seqval, _operation)`.
* **`t6` MERGE idempotency fails:** the sink is unaffected (append + txn options).
  Downstream MERGEs must be idempotent by key instead.

## 4. Results

| Check | Platform / version | Result | Evidence (`lab/results/...`) |
|---|---|---|---|
| t1 | | | |
| t2 | | | |
| t3 | | | |
| t4 | | | |
| t5 | | | |
| t6 | | | |
| t7 | | | |
