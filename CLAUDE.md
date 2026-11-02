# CLAUDE.md

`mssql-cdc-pyspark` is a platform-agnostic **PySpark DataSource V2 streaming source
for SQL Server CDC**, plus a Delta sink and a **completeness signal**
(`finalized_until`). Product: `README.md`; usage: the docs site (`docs/`, Zensical from
`mkdocs.yml`, ADR 0024); how it works: `docs/ARCHITECTURE.md`; why: `docs/DESIGN.md`;
what is next: `docs/ROADMAP.md`. Status: 1.0 release candidate; the public API and the
state contract are frozen (ADR 0021 amendment 11). Version in `pyproject.toml`.
What each area has been validated against (Spark 4.1/4.2, Databricks, Connect, SQL Server
2017/2022) is in the tests, `lab/results/` and `CHANGELOG.md`.

## Commands

Per-platform setup (Linux, WSL2, Windows native) is in `docs/DEVELOPMENT.md`. Native Windows
also needs `HADOOP_HOME` (winutils) and `PYSPARK_PYTHON` pointing at the venv interpreter.

```bash
uv sync                                             # .venv + dev group
pytest -q                                           # unit tests, no SQL Server needed
MSSQL_CDC_TEST_DELTA=0 pytest -q                    # skip resolving Delta jars (faster)
uv run pytest -q -m sqlserver                       # integration, testcontainers, needs Docker
uv run --group connect pytest -q -m connect         # Spark Connect; run alone

cp .env.example .env                                # then set MSSQL_SA_PASSWORD (left empty)
docker compose up -d                                # lab SQL Server 2022 + Agent
python -m lab.workload setup                        # db, tables, CDC; also seed|stream|bulk|long-tx
python -m lab.checks.t5_engine                      # t1..t11, see LAB.md

uv run ruff check                                   # add --fix for the safe fixes
uv run ruff format                                  # CI runs --check
uv run mypy                                         # strict: src/ and tests/typing_*.py
uv run pyright --verifytypes mssql_cdc --ignoreexternal  # public API 100% typed (ADR 0031)
uv run --group docs zensical build --strict         # CI docs job

make d2-install                                     # pinned D2 v0.9.0, sha256-checked (Linux x86_64; WSL)
make diagrams                                       # render docs/diagrams/*.d2 to SVG (ADR 0035)
make diagrams-check                                 # CI diagrams job: stale, missing or orphan SVG fails
make diagrams-watch NAME=architecture               # live preview of one diagram
```

`make` targets mirror these on Linux; `scripts/lab.ps1` is the PowerShell equivalent.
Release steps: `docs/RELEASING.md`.

## Repo map

```
src/mssql_cdc/
  source.py        DataSource + stream readers. Driver: initialOffset, latestOffset,
                   partitions, commit. Executor: read(). Spark 4.2 admission control; legacy reader.
  client/          import from mssql_cdc.client (__init__ re-exports): errors, make_client().
    _protocols.py  CdcClient and Backend protocols.
    _validators.py checks for everything inlined into T-SQL (invariant 13).
    _sql.py        type map; T-SQL builders.   _backends.py  mssql-python (default), arrow-odbc.
    _sql_client.py SqlCdcClient.               _planning.py  chunk planning for chunked snapshots.
  fake.py          File-backed CDC simulator (FakeCdcClient, FakeCdcDatabase).
  sink.py          delta_sink(): idempotent append + per-batch facts; uncached (ADR 0032).
  events.py        the facts event protocol: event names, row writers, shared readers.
                   A new event, or a new reader of one, goes here.
  pipeline/        import from mssql_cdc.pipeline; CdcStream = one mixin per part:
                   _stream (to_delta), _snapshots (ADR 0016, 0025), _recovery (resnapshot,
                   ADR 0018), _chunked (ADR 0028), _lock, _common.
  fanout.py        start_many(): one stream per capture instance (ADR 0027).
  finalization.py  finalized_until: advance() (monotonic MERGE), is_final(), track() (ADR 0026).
  silver.py        apply_changes(): bronze -> current state (ADR 0019).
  reconcile.py     reconcile(): silver vs SQL Server (ADR 0028).
  tables.py        DeltaTable API, typed tables with column comments (ADR 0012).
  migrations/      per table kind: control, facts, bronze, silver, reconcile, plans (ADR 0013).
  spark.py         get_spark(), available_cores() (ADR 0011).
  lsn.py, types.py, payloads.py, _metricsfs.py   LSN helpers, public types, state TypedDicts, metricsPath files.
lab/               workload.py, common.py, checks/t1..t11 (hypothesis checks, see LAB.md).
sql/               00_setup.sql, heartbeat.sql (ADR 0010), switch_capture_instance.sql (ADR 0023).
tests/             integration/ (-m sqlserver), test_connect.py + connect_site/ (-m connect),
                   compat/<version>/ (state each released wheel wrote, generate.py).
docs/              site pages + decisions/ (ADRs) + diagrams/ (D2 sources, committed SVGs,
                   shared _style.d2; ADR 0035). notes/ is local, gitignored, Portuguese (context only).
```

## Invariants (do not break without an ADR)

1. **Offset contract.** `{"lsn": "0x" + 20 uppercase hex, "commit_ts": "<UTC ISO-8601, ms>"}`.
   It lives in users' checkpoints: renaming keys or changing formats breaks resumes.
   `lsn` is the last *processed* commit LSN.
2. **Batches are commit-history prefixes.** A batch reads the closed interval
   `[fn_cdc_increment_lsn(start), end]` of `cdc.<ci>_CT` (not `fn_cdc_get_all_changes_*`,
   ADR 0009) with `end <= sys.fn_cdc_get_max_lsn()`, ending on a commit boundary
   (`maxCommitsPerBatch` counts `cdc.lsn_time_mapping` rows). With a newer capture instance,
   the interval is cut at its `start_lsn` S, each range naming its own instance (ADR 0023).
3. **No empty ranges.** `partitions()` returns `[]` when `end <= start`; never plan `from > to`.
4. **Retention guard, twice.** Driver: `increment(start) < fn_cdc_get_min_lsn(ci)`. Executor:
   after a read, `fn_cdc_get_min_lsn(ci)` moved past the range's `from_lsn`. Raise
   `DataLossError` unless `failOnDataLoss=false`; a skip is never silent: it leaves a
   `'data_skipped'` facts row (`certain` true from the driver, false from the executor via
   its metrics file). `to_delta(on_data_loss="resnapshot")` runs the driver test in a
   pre-flight and re-snapshots into a new generation (ADR 0018). With two capture instances
   `ci` is the one the range reads (ADR 0023).
5. **Executors are stateless.** `read()` opens and closes its own connection; the reader
   drops `_client` in `__getstate__`. Nothing live is pickled: `tokenProvider` crosses as
   its name, imported and called at each connection (ADR 0034).
6. **LSNs cross every boundary as hex strings**, bound as `CONVERT(binary(10), ?, 1)` and
   returned as `CONVERT(varchar(22), <lsn>, 1)`. Never bind or decode binary in Python.
7. **Every Arrow batch is cast to the declared Spark schema** before it is yielded.
8. **Sink idempotency.** `app_id` is stable for a checkpoint's lifetime; a new checkpoint
   needs a new `app_id` (batch ids restart at 0). Re-snapshot generation `n` gets
   `<checkpoint>/_generations/<n>` and `<app_id>.g<n>` (ADR 0018).
9. **Finalization ordering.** Data committed first, `finalized_until` advanced after,
   monotonic (`GREATEST`). Never store it in `TBLPROPERTIES`.
10. **Platform-agnostic core.** Nothing under `src/mssql_cdc` imports `dbutils`,
    `databricks.*` or other platform APIs; glue belongs in `examples/` and docs.
11. **Least privilege.** Beyond the CDC query functions, the reader needs only `SELECT` on
    `cdc.<ci>_CT` of each instance it reads (ADR 0009). Metadata comes from documented APIs
    (`sys.sp_cdc_get_captured_columns`, `sys.sp_cdc_help_change_data_capture`,
    `sys.sp_cdc_get_ddl_history`, `sys.fn_cdc_*`) plus `sys.columns` of the source table. Add
    no reads of other `cdc.*` tables except `cdc.lsn_time_mapping`. Never create or drop
    capture instances.
12. **Snapshot LSN first.** A snapshot's S is the `max_lsn` recorded *before* any row is
    read; a rerun returns the S already written (chunked: opened), never a newer one
    (ADR 0016). Exception: a chunked snapshot that `snapshot_on_switch` opens uses the newer
    instance's start LSN (ADR 0023 Amendment 7). Chunked rows are stamped per wave with a
    `max_lsn` recorded before that wave's read, always >= S (ADR 0028). Snapshot rows are
    `_operation = 0` with `_snapshot` = S; identify a snapshot by `coalesce(_snapshot,
    _start_lsn)` and its completion event, never by the largest `_start_lsn`. Never read
    with `NOLOCK`. Chunks are planned once, by the first `backfill()`, into
    `<target>_snapshot_plans`, never change while the snapshot is open, and tile the key
    space up to just above the MAX recorded at the open. Key values never go into the facts
    (except snapshots opened before 0.7.0, ADR 0028 Amendment 4). A run takes its snapshots
    in one mode; an open snapshot blocks the other mode and `seed()` until its completion
    row (ADR 0028).
13. **SQL injection.** Anything inlined into T-SQL (capture instance, columns, timezone,
    integers) goes through `src/mssql_cdc/client/_validators.py`.
14. **Schema changes are checked before the read.** DDL inside a batch's range is found on
    the driver while planning; a captured type the query no longer holds fails the batch
    before any row is read (`SchemaChangedError`). An instance's columns are checked the
    first time a run reads it; a source column no instance captures fails instead of reading
    NULL. Computed columns (`sys.columns.is_computed`) are left out of the inferred schema
    and read NULL if listed in `columns` (ADR 0007). A snapshot matches captured and source
    columns by `column_id`, never by name alone. `_command_id` is per capture instance:
    order across instances by `(_start_lsn, _seqval, _operation)`. Only `'bootstrap'` and
    `'resnapshot'` facts events are snapshots; `'snapshot_open'`, `'snapshot_plan'` and
    `'snapshot_chunk'` rows are not, and a chunked snapshot's completion row (min = max = S)
    comes after its last chunk (ADR 0023, 0028).

## Testing strategy

* `tests/test_source_fake.py` runs the real engine in local mode against `FakeCdcDatabase`:
  add a test there for any change to offsets, batching, guards or output schema. The fake
  must match real CDC semantics (`docs/DESIGN.md`); when a lab check shows a difference, fix
  the fake too.
* `tests/test_client_sql.py` pins the T-SQL shape.
* `tests/test_properties.py`: Hypothesis properties for pure logic (derandomized, capped). A
  new pure-logic invariant gets a property; prove it fails by breaking the code it guards once.
* `tests/integration` (`-m sqlserver`, deselected by default): anything that depends on how
  SQL Server or the driver really behaves.
* `tests/test_connect.py`: anything new a user calls from a Spark Connect client gets a step
  there; `refuse_caching` there mimics Databricks serverless. No `sparkContext`, `rdd`,
  `_jvm`, `_jsc` or `_jdf` in `src/`: try the call and fall back, as `available_cores` does.
  `MSSQL_CDC_TEST_SPARK=connect` runs the `spark`-fixture tests through that server.
* `tests/compat/test_compat.py` resumes the state each released wheel wrote: a failure
  means the state contract broke (ADR 0021).
* `lab/checks` prove SQL Server behaviour; CI (`.github/workflows/ci.yml`) runs them on
  `mcr.microsoft.com/mssql/server:2022-latest`.
* Claims in docs or write-ups need a result in `lab/results/` or a passing
  `tests/integration` test. Exception: a measurement taken outside the repo against a real
  source; state it as such ("measured against a production source", with the setup), never
  naming the source, its tables or its owner.

## Conventions

* English in code, docs, commit messages. Python >= 3.10, type hints, small modules.
* CI's `lint` job (ruff check, ruff format --check, mypy strict, pyright `--verifytypes`,
  which fails below 100%: annotate a public constant or attribute whose type is inferred)
  gates every other job: run them before pushing (ADR 0031). A broad `except` says why:
  `# noqa: BLE001 - <reason>`.
* PySpark is an optional extra (`[spark]`): installing PyPI `pyspark` on Databricks, EMR,
  Dataproc or Fabric conflicts with the platform's Spark. Add no hard dependencies to
  `[project].dependencies` beyond `pyarrow` and `mssql-python` (ADR 0003).
* Usage is documented once, on the site: keep `docs/reference/options.md` and
  `docs/reference/output-schema.md` in sync with the options read in `source.py`,
  `client.make_client` and `pipeline.to_delta`; `docs/reference/tables.md` copies the table
  comments (`tests/test_docs_tables.py` checks). A new page needs a line in `mkdocs.yml` nav.
  Docs stay platform-neutral.
* Changing the control, facts, bronze or silver schema: update its creation columns *and*
  append a migration to `src/mssql_cdc/migrations/<kind>.py` (ADR 0013); never edit a shipped one.
* After a final release is on PyPI, generate `tests/compat/<version>` from its wheel with
  `tests/compat/generate.py` and commit it (a release candidate gets none).
* Design changes are a new ADR in `docs/decisions/` with `**Date:**` (ISO-8601 with UTC
  offset) and an `**Amended:**` line per later change (`docs/decisions/README.md`).
* Diagrams are D2 only, no Mermaid (ADR 0035): edit `docs/diagrams/<name>.d2`, run
  `make diagrams`, commit the `.d2` with its `.svg`; never hand-edit an SVG. Each `.d2`
  starts with `...@_style`, uses its classes and sets no fill color (fills do not switch in
  dark mode); about 25 nodes and 1,300 px wide at most; long text goes in node labels, not
  edge labels. A change to what a diagram shows updates it in the same change. Docs pages
  link `diagrams/<name>.svg` relatively; the README (the PyPI description) by absolute URL
  on `main`.
