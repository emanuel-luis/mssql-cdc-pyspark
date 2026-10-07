# Roadmap

Small releases, one theme each, plus fixes. Under the 0.x policy (ADR 0021) a patch only
fixes, or changes nothing users can see (tests, CI, docs); a minor may break the Python API
(listed under "Breaking" in `CHANGELOG.md`) or add public API, such as exported types.

## Shipped

- [x] **v0.1, validate what exists**: lab checks t1–t7 green against SQL Server 2022, code
      and fake fixed where real CDC differed (ADRs 0008–0010); Delta tests; CI `unit`,
      `integration` and `lab` jobs; Databricks classic (DBR 18.2, dedicated): t5, t6
      `--schema`.
- [x] **PyPI release** of 0.1.0 (2026-10-01): package metadata, `py.typed`, `release.yml`
      with Trusted Publishing behind a reviewed environment and a TestPyPI dry run,
      `CHANGELOG.md` and the 0.x compatibility policy (ADR 0021); steps in `RELEASING.md`.
- [x] **v0.2, production concerns**, shipped as 0.2.0 on 2026-10-06: bootstrap snapshot
      (ADR 0016), re-snapshot after data loss (ADR 0018), seeding from an existing copy
      (ADR 0025), chunked first import next to the stream with `backfill()` and
      `reconcile()` (ADR 0028, lab check t10), schema changes and capture instance switches
      (ADR 0023, lab check t9, also on SQL Server 2017), the silver helper (ADR 0019),
      continuous-mode finalization (ADR 0026), retention headroom and operational metrics in
      the facts (ADRs 0017, 0020), `arrow-odbc` in CI (ADR 0003), one stream per table with
      `start_many` (ADR 0027). Chunked snapshots ran from Databricks against a production
      source on a 34-million and a 100-million-row table.
- [x] Even chunks for sparse single integer keys: a chunked snapshot's plan counts the rows
      of each key slice on the server (ADR 0028), which supersedes NTILE tiles for them.

## 0.2.1 (patch, shipped 2026-10-06): tests and supply chain

- [x] Faster suites and CI: unit tests off the critical path, long jobs in parallel, a
      lighter test session, pure tests where the engine is not needed (target: CI in about
      30 minutes). Locally the unit suite went from 68 to 28 minutes with 277 to 420 tests.
- [x] `tests/compat/0.2.0`: the state the 0.2.0 wheel wrote.
- [x] Supply-chain checks in CI: `pip-audit`, `zizmor` (Actions), OpenSSF Scorecard, and a
      job that installs the lowest direct dependency versions the package declares
      (`uv sync --resolution lowest-direct`) and runs the fast tests.
- [ ] Report to Spark: stopping a PySpark `foreachBatch` query mid-batch (as `to_delta` does)
      prints a `StackOverflowError` from the stream execution thread. `StreamExecution`'s
      `isInterruptionException` runs the regex `PROXY_ERROR`, whose `(.|\r\n|\r|\n)*` recurses
      once per character, over the Py4J error message (~16 KB with the embedded Java stack
      trace). The query has already terminated, so nothing is lost; Spark's built-in `rate`
      source hits it too, and `spark.driver.extraJavaOptions=-Xss16m` silences it. Found in
      v4.2.0, unchanged on master; no JIRA yet.
- [x] Fixes found by the test review, and pyarrow 19 or later (18 crashes on Windows when
      imported before `mssql_python`).

## 0.2.2 (patch, shipped 2026-10-07): time and reconcile

- [x] DST fall-back: `commit_ts` can go backwards in a named zone; resolve the overlap by
      LSN order.
- [x] Pre-2022 fixed-offset fallback (`sourceTimeZone=auto` before SQL Server 2022): driver
      and executors disagree after a DST change; refresh the offset per batch.
- [x] `reconcile` reports stream lag as `MISMATCH` where it is `IN_FLIGHT`.
- [x] `reconcile`'s join misses a change of a NULL key; join null-safe.

## 0.3.0 (minor, Breaking, shipped 2026-10-07): API shape

- [x] Keyword-only parameters after `facts_table` in `to_delta`, and `resnapshot` in
      `snapshot()`.
- [x] `Literal` types on every mode parameter (`snapshot`, `on_data_loss`, `isolation`,
      policies).
- [x] Typed public results: `TypedDict`s (still dicts) for `backfill()`, `apply_changes()`,
      `reconcile()`, `snapshot()`.
- [x] Planning warnings (columns not read) also in the facts `detail`, not only in the log.

## 0.4.0 (minor, shipped 2026-10-07): typing and protocols

- [x] `typing.Protocol` for the pluggable seams: `Backend` (mssql-python, arrow-odbc, or a
      user's own) and `CdcClient` (`SqlCdcClient`, the fake), `runtime_checkable`, exported;
      they are the 0.3 classes themselves, so subclassing keeps working with nothing to
      deprecate (ADR 0030).
- [x] `TypedDict`s for the state payloads ADR 0021 lists (`snapshot_open`,
      `snapshot_plan`, `snapshot_chunk`, completion, a wave's `userMetadata`) and the known
      source options.
- [x] `NewType` for LSN hex strings; frozen dataclasses in place of loose internal dicts.
- [x] mypy strict on `src` (no untyped defs, no implicit `Any` generics, `warn_return_any`);
      `pyright --verifytypes` in CI to keep the public API fully typed (ADR 0031).

## 0.4.1 (patch): maintenance

- [x] Split `client.py` (SQL builders, backends, planning) and `pipeline.py` (snapshot,
      backfill, recovery) into packages without changing behaviour.
- [x] One place for the facts event protocol, shared by its writers and readers
      (`mssql_cdc.events`).
- [x] Property-based tests (Hypothesis) for LSN math, plan tiling and change ordering; one
      mutation-testing pass on the core modules, results recorded.

## 0.5.0 (minor): faster first import

- [ ] Overlapping waves (read the next while committing the previous one's facts) and an
      adaptive chunk size aiming at a wave duration; target: the full bootstrap's wall time.
- [ ] silver filters chunks by wave instead of reading every chunk on each call.
- [ ] Batches capped by bytes for LOB tables; keyset bounds instead of NTILE for full
      snapshots.
- [ ] Measure `arrow-odbc`'s concurrent fetch (lab t8).
- [ ] Validation: a Databricks benchmark, and the weeks-long chunked run against the
      production source, on its largest table.

## 0.6.0 (minor): platforms

- [ ] Databricks serverless: facts from the written commit instead of `persist()`.
- [ ] Object-store `metricsPath` (`s3://`, `abfss://`) through `pyarrow.fs`.
- [ ] Spark Connect checks; t5 and t7 on serverless and on one non-Databricks platform.

## 0.7.0 (minor): credentials and privacy

- [ ] `user`/`password` and Entra ID `accessToken` options, so the secret need not live in
      the connection string.
- [ ] The snapshot plan stops writing key values in clear into the facts table.

## 0.8.0 (minor): operations at scale

- [ ] Chunked snapshots of keyless tables, cut on a chosen column.
- [ ] `snapshot_on_switch` in chunked mode.
- [ ] `start_many()`: measure the per-stream driver cost and document a ceiling.
- [ ] A verdict-freshness metric for alerts.

## 1.0.0: stability

- [ ] Frozen API and state contract; versioned docs (stable and dev).
- [ ] Databricks classic: t7 `--schema` (needs a lab SQL Server reachable from the
      cluster).
- [ ] Build provenance (attestations) in the release.

## After 1.0

- [ ] Scala/Java DSv2 `Changelog` (`TableCatalog.loadChangelog`) so
      `SELECT ... CHANGES FROM VERSION ...` works over SQL Server (Spark 4.2+). Deferred
      until the API settles (ADR 0022).
