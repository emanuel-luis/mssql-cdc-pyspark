# bash for the diagram targets (pipefail, shopt); the other recipes are plain commands.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: install up down setup seed stream lint test test-fast lab-sql lab-spark lab d2-install diagrams diagrams-check diagrams-watch

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
	uv run pyright --verifytypes mssql_cdc --ignoreexternal

test:           ## unit tests (no SQL Server needed)
	uv run pytest -q

test-fast:      ## the tests that start no JVM: safe next to a Spark suite (-m replaces addopts')
	uv run pytest -q -m "not spark and not sqlserver"

lab-sql:        ## SQL Server behaviour checks (t1 takes ~10 min, run it alone)
	uv run python -m lab.checks.t2_timezone
	uv run python -m lab.checks.t3_read_semantics
	uv run python -m lab.checks.t4_watermark_concurrency
	uv run python -m lab.checks.t1_idle_heartbeat

lab-spark:      ## Spark / Delta / end-to-end checks
	uv run python -m lab.checks.t5_engine
	uv run python -m lab.checks.t6_delta_semantics
	uv run python -m lab.checks.t7_end_to_end --idle-minutes 6
	uv run python -m lab.checks.t9_capture_instance_switch
	uv run python -m lab.checks.t10_chunked_snapshot
	uv run python -m lab.checks.t10_chunked_snapshot --resnapshot

lab: setup seed lab-sql lab-spark

# Diagrams: docs/diagrams/<name>.d2 rendered to <name>.svg (committed). One pinned D2 everywhere.
# Layout engine, themes and padding live in docs/diagrams/_style.d2 (d2-config), so d2 runs in a
# clean environment: D2_* and SCALE variables would override that config. --scale 1 is not a
# d2-config key; it writes the natural width and height, so GitHub never upscales a small diagram.
D2_VERSION := 0.9.0
# sha256 of d2-v$(D2_VERSION)-linux-amd64.tar.gz, from the release's SHA256SUMS
D2_SHA256 := 5669ddc46b99e942cc96078f4a4e36d5e62103348f4c05179ede27802fdd87a9
D2_HOME ?= $(HOME)/.local/share/d2/v$(D2_VERSION)
D2 ?= $(D2_HOME)/bin/d2
D2_RUN = env -i PATH="$$PATH" HOME="$$HOME" "$(D2)" --scale 1
DIAGRAMS := $(filter-out docs/diagrams/_%,$(wildcard docs/diagrams/*.d2))

d2-install: ## install the pinned D2 (Linux x86_64) into D2_HOME, checksum verified
	@if [ -x "$(D2_HOME)/bin/d2" ]; then echo "D2 v$(D2_VERSION) already in $(D2_HOME)"; exit 0; fi; \
	[ "$$(uname -sm)" = "Linux x86_64" ] || { echo "d2-install covers Linux x86_64 only;" \
		"install D2 v$(D2_VERSION) yourself and pass D2=/path/to/d2" >&2; exit 1; }; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSLo "$$tmp/d2.tar.gz" \
		"https://github.com/terrastruct/d2/releases/download/v$(D2_VERSION)/d2-v$(D2_VERSION)-linux-amd64.tar.gz"; \
	echo "$(D2_SHA256)  $$tmp/d2.tar.gz" | sha256sum --check --strict; \
	tar -xzf "$$tmp/d2.tar.gz" -C "$$tmp"; \
	mkdir -p "$(D2_HOME)/bin"; \
	install -m 0755 "$$tmp/d2-v$(D2_VERSION)/bin/d2" "$(D2_HOME)/bin/d2"; \
	"$(D2_HOME)/bin/d2" --version

diagrams: ## render docs/diagrams/*.d2 to SVG with the pinned D2 (run make d2-install once)
	@v="$$($(D2_RUN) --version 2>/dev/null || true)"; [ "$$v" = "v$(D2_VERSION)" ] || { \
		echo "need D2 v$(D2_VERSION) at $(D2), found '$${v:-nothing}': run make d2-install" >&2; exit 1; }
	@for f in $(DIAGRAMS); do $(D2_RUN) "$$f" "$${f%.d2}.svg"; done

diagrams-check: diagrams ## render, then fail if a committed SVG is stale, missing or has no source
	@shopt -s nullglob; for s in docs/diagrams/*.svg; do \
		[ -f "$${s%.svg}.d2" ] || { echo "$$s has no .d2 source" >&2; exit 1; }; done
	@changed="$$(git status --porcelain --untracked-files=all -- 'docs/diagrams/*.svg')"; \
	if [ -n "$$changed" ]; then echo "$$changed" >&2; \
		echo "rendered SVGs differ from the committed ones: run make diagrams and commit" >&2; exit 1; fi

diagrams-watch: ## live preview in the browser while editing: make diagrams-watch NAME=architecture
	@# NAME only from the command line: WSL exports NAME (the hostname) into every shell.
	@[ "$(origin NAME)" = "command line" ] && [ -f "docs/diagrams/$(NAME).d2" ] || { \
		echo "usage: make diagrams-watch NAME=<one of: $(basename $(notdir $(DIAGRAMS)))>" >&2; exit 1; }
	$(D2_RUN) --watch --browser "$${BROWSER:-0}" docs/diagrams/$(NAME).d2 docs/diagrams/$(NAME).svg
