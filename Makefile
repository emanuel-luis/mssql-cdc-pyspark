.PHONY: install up down setup seed stream lint test lab-sql lab-spark lab

install:        ## local dev install
	uv sync

up:             ## start SQL Server 2022 (with Agent) in Docker
	docker compose up -d && docker compose ps

down:
	docker compose down

setup:          ## create lab database, tables, enable CDC
	uv run python -m lab.workload setup

seed:
	uv run python -m lab.workload seed --customers 500 --orders 2000

stream:         ## background OLTP traffic (Ctrl+C to stop)
	uv run python -m lab.workload stream --tps 5 --duration 600

lint:           ## what CI's lint job runs
	uv run ruff check
	uv run ruff format --check
	uv run mypy

test:           ## unit tests (no SQL Server needed)
	uv run pytest -q

lab-sql:        ## SQL Server behaviour checks (t1 takes ~10 min, run it alone)
	uv run python -m lab.checks.t2_timezone
	uv run python -m lab.checks.t3_read_semantics
	uv run python -m lab.checks.t4_watermark_concurrency
	uv run python -m lab.checks.t1_idle_heartbeat

lab-spark:      ## Spark / Delta / end-to-end checks
	uv run python -m lab.checks.t5_engine
	uv run python -m lab.checks.t6_delta_semantics
	uv run python -m lab.checks.t7_end_to_end --idle-minutes 6

lab: setup seed lab-sql lab-spark
