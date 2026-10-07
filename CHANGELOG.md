# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with the 0.x
policy of [ADR 0021](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0021-compatibility-policy-for-0x/): a minor release
may break the Python API (listed under "Breaking"), a patch only fixes, and the state a
stream leaves behind (offsets, checkpoint layout, table schemas) never breaks without a
migration path. Every release says what it does to that state in its "State
compatibility" line.

## [Unreleased]

State compatibility: offsets and checkpoints unchanged. Facts migration 12 rewrites the
comment of `detail` (metadata only), the next time a stream opens the facts table; a
micro-batch row's `detail` gains the key `warnings`, an added payload
([ADR 0021](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0021-compatibility-policy-for-0x/)
amendment 6). Rows written before, and batches without warnings, keep `detail` NULL. No new
event value, no other payload key changed.

### Breaking

- Options, flags and modes are keyword-only
  ([ADR 0021](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0021-compatibility-policy-for-0x/)
  amendment 5). A positional call now raises `TypeError` before anything runs. To migrate,
  pass the same values by name:
  - `stream(...).to_delta(target, app_id, checkpoint, facts_table=None, *, ...)`: everything
    after `facts_table` (`trigger`, `query_name`, `bootstrap`, `on_data_loss`,
    `resnapshot_interval_days`, `snapshot_on_switch`, `snapshot`) by keyword, e.g.
    `to_delta("bronze.orders", "orders-v1", ckpt, "ops.facts", bootstrap=True)`.
  - `stream(...).snapshot(target, *, resnapshot=False, ...)`: `snapshot(t, True)` becomes
    `snapshot(t, resnapshot=True)`.
  - `apply_changes(spark, bronze, target, *, capture_instance=None, keys=None, ...)`:
    `apply_changes(spark, b, s, "dbo_orders", ["order_id"], ...)` becomes
    `apply_changes(spark, b, s, capture_instance="dbo_orders", keys=["order_id"], ...)`.
  - `reconcile(spark, options, silver, *, keys=None, ...)`.
  - `granularity` of `finalization.advance()`, `finalization.track()` and
    `finalization.candidate()`: `advance(spark, control, table, end, "day")` becomes
    `advance(spark, control, table, end, granularity="day")`.
  - `sink.delta_sink(target, app_id, facts_table=None, *, metrics_path=None)` and
    `spark.get_spark(app_name, master, *, delta=True)`.
- For type checkers only: `snapshot()`, `seed()`, `backfill()`, `apply_changes()` and
  `reconcile()` now return `TypedDict`s instead of `dict`. Code annotated to take their
  result as `dict` should take the new type or `Mapping[str, Any]`. At run time they are
  the same dicts.

### Added

- `mssql_cdc.types`, exported from `mssql_cdc`:
  - `Literal` aliases for the mode parameters: `SnapshotMode`, `OnDataLoss`, `Isolation`,
    `Granularity`, and `BackfillState` for `backfill()`'s `state`.
  - `TypedDict`s for the results, plain dicts at run time: `Offset` (of `snapshot()` and
    `seed()`), `BackfillStatus`, `ApplyResult`, `ReconcileResult`.

  The signatures use them, so a type checker flags a misspelt mode or result key. A wrong
  mode still raises `ValueError` naming the allowed values.
- A micro-batch row of the facts table (`event` NULL) now carries the warnings the reader
  logged on the driver in `detail`, as JSON `{"warnings": [...]}`, only when there are any:
  columns it does not read (computed columns, or columns a newer capture instance captures
  that `columns` leaves out), unknown options, and the UTC offset it converts commit times
  with on a server older than SQL Server 2022. Before, they were only in the driver's worker
  log. Needs `metricsPath`, like the other metrics
  ([ADR 0023](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0023-schema-changes-and-capture-instance-switching/)
  amendment 6).

### Changed

- `finalization.advance()` and `finalization.candidate()` take the offset as any mapping
  (`Mapping[str, Any]`), so an `Offset` and a parsed progress offset both type-check.

## [0.2.2] - 2026-10-07

State compatibility: unchanged from 0.2.1 (offsets keep their format; no migration, no new
event value).

### Fixed

- With a named source time zone that has daylight saving (`sourceTimeZone`, or `auto` on
  SQL Server 2022), commits in the hour a fall-back repeats now keep commit order. SQL
  Server's `AT TIME ZONE` reads a repeated time with the offset before the change, so
  commits made after the clock went back got commit times an hour early, behind the commits
  before them, and their rows could land in periods `finalized_until` had already declared
  final. Those commits now get the offset after the change
  ([ADR 0008](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0008-detect-source-time-zone/)
  amendment 4).
- Before SQL Server 2022 (`sourceTimeZone=auto`, fixed-offset fallback), the driver now
  reads the server's UTC offset again for each new batch instead of once per run, and sends
  it with the batch. A long run follows a daylight-saving change from its next batch, and a
  batch's end offset and rows use the same offset. A warning is logged when the offset
  changes. On Spark 4.0/4.1 an idle poll now sends `max_lsn` alone; before, it also read
  the commit time on every poll.
- `reconcile()` no longer reports stream lag as an integrity failure. A difference explained
  by a change the stream has not read yet is now `IN_FLIGHT`, not `MISMATCH`,
  `MISSING_TARGET`, `MISSING_SOURCE` or `RECORD_DIFF`. Once the source is read, it reads the
  keys in the change table from bronze's position up to `sys.fn_cdc_get_max_lsn()` read
  then, through the capture instances the stream reads. This is the change table the stream
  already reads, so no new grant is needed. Only a commit the capture job has not harvested
  yet stays unseen
  ([ADR 0028](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0028-chunked-snapshot-next-to-the-stream/)
  amendment 2).
- `reconcile()` now finds a change to a key whose value is NULL (the join is null-safe), so
  that key's difference is `IN_FLIGHT` instead of `RECORD_DIFF` or `MISSING_*`. The report's
  `key` now names it, `{"code":null}`, instead of `{}`.

## [0.2.1] - 2026-10-06

State compatibility: unchanged from 0.2.0 (no migration, no new event value).

### Fixed

- pyarrow 19 or later: with pyarrow 18 imported before `mssql_python`, the Python
  interpreter crashes on Windows (access violation). The install pins in the Databricks
  page, the docs home and the example notebook say 0.2.0, not 0.2.0rc1.

### Changed

- Tests and CI: logic the streaming engine is not needed for is tested in plain unit tests
  (specced mocks, pure functions), the test session starts Delta only for the tests that
  take it, and `-m "not spark and not sqlserver"` runs the JVM-free tests alone. CI runs
  the unit suite in three shards next to the integration and lab jobs instead of before
  them: about 13 minutes instead of 48. A parity test runs one scripted change history on
  the fake and on SQL Server 2022. `tests/compat/0.2.0` holds the state the 0.2.0 wheel
  wrote.
- CI: a release tag no longer starts its own `ci` run; the release commit's run on `main`
  is the one the `pypi` approval waits for (docs/RELEASING.md). `zizmor` treats a workflow
  a tag triggers as publishing, and `ci` restores caches.
- CI: a `security` workflow checks the supply chain on every pull request, every push to
  `main` and weekly. `pip-audit` checks every package `uv.lock` pins against the known
  vulnerabilities; `zizmor` audits the workflows and fails on a finding of medium severity
  or higher; a job installs the lowest version of each direct dependency the package
  declares (`uv sync --resolution lowest-direct`, Python 3.10) and runs the tests that
  start no JVM; the OpenSSF Scorecard runs from `main` and publishes its results.

## [0.2.0] - 2026-10-06

The first final release of the 0.2 line: the same code as 0.2.0rc2. Its changes are listed
under 0.2.0rc2 and 0.2.0rc1 below. Both release candidates were validated against a
production SQL Server 2016 from Databricks (DBR 18.2): chunked snapshots next to a running
stream, silver applied wave by wave and `reconcile` matching every bucket, on a 34-million
and a 100-million-row table with 0.2.0rc1 and again on the 34-million-row one with 0.2.0rc2.

State compatibility: offsets and checkpoints unchanged since 0.1.0. Tables written by 0.1.0
migrate the next time the library opens them, through the migrations listed in the State
compatibility lines of 0.2.0rc1 and 0.2.0rc2; `tests/compat/0.1.0` resumes such state with
this release.

## [0.2.0rc2] - 2026-10-06

State compatibility: offsets and checkpoints unchanged. Control migration 4 rewrites the
comment of `finalized_until` (metadata only), the next time `finalization.advance` or
`apply_changes` opens the control table. A migration interrupted between its change and its
version stamp now completes on the next open instead of failing every writer
(`add_columns` skips the columns a table already has), and one that loses to another job's
concurrent commit runs again. A release still writes a table a newer one migrated, with a
WARNING, so jobs that share a table can upgrade or roll back one at a time; a release that
knows fewer migrations than the table property `mssql_cdc.min_version` names refuses it,
and no migration sets that property yet
([ADR 0013](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0013-schema-migrations-per-table-kind/)
amendment,
[ADR 0021](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0021-compatibility-policy-for-0x/)).
Facts migration 10 rewrites the comments of `event`, `lost_from_ts`, `lost_to_ts`, `detail`,
`batch_id` and the table (metadata only), and `event` gains the value `'data_skipped'`;
facts migration 11 rewrites those of `event`, `lost_from_ts` and `detail` again, for the
possible skips a task records (metadata only)
([ADR 0018](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0018-automatic-resnapshot-after-data-loss/) amendment 3). The state contract
also covers the silver and reconcile schemas, the facts `event` values and the keys of a
snapshot row's `detail` and of a backfill wave's userMetadata, keys that are only ever added
([ADR 0021](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0021-compatibility-policy-for-0x/) amendment 4); `tests/compat/<version>`
holds the state each release's wheel wrote, and the tests resume it (amendment 3). A bronze
or silver table created with a column name Delta refuses without column mapping gets it,
which adds the `columnMapping` feature to its Delta protocol (older readers and writers
refuse the table); existing tables are untouched. A restarted stream's inferred schema
leaves out computed columns; an existing bronze table
keeps such a column, and the rows appended after the upgrade hold NULL in it, as its change
rows always did.

### Added

- `lockTimeoutMs` source option (off by default): `SET LOCK_TIMEOUT` before every read of
  the source table (snapshot, chunked-snapshot planning, reconcile), so a read blocked by a
  writer fails with error 1222 and Spark retries the task instead of hanging
  ([ADR 0029](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0029-driver-retries-and-lock-timeout/)).
- The stream's driver-side calls (`initialOffset`, `latestOffset`, `reportLatestOffset`,
  `partitions`, `prepareForTriggerAvailableNow`) retry a transient SQL Server error
  (SQLSTATE 08xxx, HYT00, HYT01, 40001) on a new connection, up to 3 times within about
  14 s, with a WARNING each time; `DataLossError`, `SchemaChangedError`, `ValueError` and
  `PermissionError` are never retried ([ADR 0029](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0029-driver-retries-and-lock-timeout/)).
- `apply_changes` takes the capture instance from the options' `captureInstance` when
  `capture_instance` is omitted (one given still wins).
- `is_data_loss(exc)` and `is_schema_changed(exc)` recognise `DataLossError` and
  `SchemaChangedError`, also inside the `StreamingQueryException` that
  `awaitTermination()` raises or `await_all` returns.
- Bronze and silver tables are created with `delta.columnMapping.mode = 'name'` when a
  captured column name holds a character Delta refuses otherwise (a space or `,;{}()=`, as
  in `[Unit Price]`); such tables used to fail on their first write. Other Delta features
  (deletion vectors, clustering, OPTIMIZE, type widening) are documented as opt-ins under
  Tables > Table properties
  ([ADR 0012](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0012-delta-tables-through-the-deltatable-api/) amendment).
- When a newer capture instance adds such a column to a bronze or silver table created
  without column mapping, the stream and `apply_changes` fail before writing with a
  `SchemaChangedError` that names the column, says column mapping is off on the table and
  gives the `ALTER TABLE ... SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')` that enables it (an
  upgrade of the table's Delta protocol, which its readers must support), instead of
  Delta's own error midway; the library does not enable it
  ([ADR 0012](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0012-delta-tables-through-the-deltatable-api/) amendment).
- With `failOnDataLoss=false`, a batch whose planning skips changes CDC cleanup purged
  writes a `data_skipped` facts row: the batch's `batch_id`, `rows` 0, `min_lsn` = `max_lsn`
  the `min_lsn` it resumed at, the LSNs skipped in `detail` (JSON `{from, to, certain}`,
  `certain` true) and the gap in `lost_from_ts` and `lost_to_ts`, as on a `resnapshot` row.
  It needs a facts table and `metricsPath` (`to_delta` sets it)
  ([ADR 0018](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0018-automatic-resnapshot-after-data-loss/) amendment 3).
- A task that finds, after reading its range, that CDC cleanup ran meanwhile (the
  executor-side retention guard, `failOnDataLoss=false`) writes a `data_skipped` facts row
  too, the loss possible rather than certain: `detail` `{from, to, certain: false, reason}`,
  `min_lsn` = `max_lsn` the `min_lsn` it found, and `lost_from_ts`/`lost_to_ts` from the
  batch's start offset to that `min_lsn`. It rides in the partition's metrics file, so a
  retried task or a replayed batch writes it once; it used to be only logged
  ([ADR 0018](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0018-automatic-resnapshot-after-data-loss/) amendment 3).
- A compatibility test against the state released versions wrote: `tests/compat/<version>`
  holds a checkpoint with two generations and the bronze, silver, facts and control tables
  that version's wheel wrote (0.1.0 from its PyPI wheel), and `tests/compat/test_compat.py`
  resumes each with the current code; every release adds its own (`docs/RELEASING.md`).
- `backfill()` returns `state`: `done`, `running`, `waiting_headroom`, `waiting_metrics` or
  `no_snapshot`, so a loop can tell a pause worth waiting out from a snapshot that does not
  exist (a wrong `app_id` or `target`, or a stream that bootstraps with `snapshot="full"`);
  the bootstrap guide's loop branches on it.
- `apply_changes` returns `bronze_found`, and logs a warning naming the table when bronze
  does not exist (before the stream's first batch, or a wrong name). It also warns, once
  per table, when called without `facts_table` (its verdict is then held), when bronze has
  no `_command_id`, and when the facts table has no row for bronze's name.
- `FinalizationListener.last_error` and `failures`: the error of the last attempt (`None`
  once one succeeds) and how many failed in a row. The first failed verdict of a streak is
  logged at ERROR with its traceback, then one WARNING at most every 10 minutes.
- INFO logs for the bootstrap, each snapshot and each `backfill()` wave (target, LSN, rows,
  seconds, chunks done), and a WARNING with the lost range when `to_delta` decides to
  re-snapshot.
- `finalization.finalized_until` is in the API reference, and so public.
- `SECURITY.md`: how to report a vulnerability privately, and what is in scope.
- Documentation: alerts on NULL metrics, on the verdict's freshness (control table) and on
  capture itself from the SQL Server side (`sys.dm_cdc_log_scan_sessions`,
  `sys.dm_cdc_errors`, the capture job, with a monitoring login); retention for the facts
  table's micro-batch rows and compaction of bronze; the NULLs CDC stores in change rows
  (computed columns, LOB types); quoting a password in the connection string, Entra ID
  authentication and Spark's redaction of `connectionString`; the access policy that the
  key values in a chunked snapshot's facts rows and bronze history need; the Microsoft
  license of the default driver's binaries, and how to install without them; the driver's
  Python process per stream in `start_many`; what a Spark Connect client must reach; the
  source-query cost of the default trigger; minute granularity across a daylight-saving
  fall-back; the tested platform scope, local Spark 4.2 and Databricks classic compute on a
  single node, with `metricsPath` on a path every node sees (without one, as on EMR or
  Dataproc by default, the metric columns stay NULL and schema change events do not reach
  the facts);
  Databricks serverless, unsupported until tested. The site says it documents
  `main`
  ([ADR 0003](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0003-mssql-python-default-backend/) amendment 3).

### Breaking

- `failOnDataLoss` and `includeCommandId` accept only true/false, 1/0, yes/no or y/n (any
  case), and raise `ValueError` naming the option otherwise. Values such as `on`, `off` or a
  typo like `ture` used to read as false, which turned the retention guard off.
- `maxCommitsPerBatch`, `numPartitions` and `arrowBatchSize` below 1, or not an integer,
  raise `ValueError`, and so does a negative `connectTimeout`. `maxCommitsPerBatch=0` used
  to mean unlimited without a word: omit the option instead.
- A source column named like a metadata column or one the sink adds (`_start_lsn`,
  `_chunk`, ..., in any case) raises `ValueError` when the stream loads, instead of being
  dropped, failing later on a duplicate column, or read as the chunk number: leave it out
  with `columns`.
- `to_delta` raises `ValueError` for `snapshot_on_switch=True` together with
  `snapshot="chunked"` (the switch snapshot reads the whole table inside a batch): drop
  `snapshot_on_switch`, or use `snapshot="full"`.
- `to_delta` raises `ValueError` for a `metricsPath` that is a URI (Python wrote it as a
  local directory named after the scheme, and every metric came out NULL): use a local or
  FUSE path, such as `/Volumes/...`, that every node sees. The reader refuses one too.
- `to_delta` raises `ValueError` for `on_data_loss="resnapshot"` with `failOnDataLoss=false`
  (a purge skipped while the query runs would hide the gap from the next run's check):
  remove `failOnDataLoss=false`.
- The inferred schema leaves out computed columns, which SQL Server CDC stores as NULL in
  every change row, so snapshot rows no longer carry values every later change nulls; `load()`
  logs a warning naming them (`sys.columns.is_computed`, which the reader already sees: no
  new grant). A query that selects one fails with `UNRESOLVED_COLUMN`, and a new bronze
  table lacks it: list it in `columns` to keep it, NULL in every row, snapshot and seed
  rows included
  ([ADR 0007](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0007-infer-columns-from-cdc-metadata/) amendment).

### Changed

- `reconcile()` no longer compares computed columns, also one listed in `columns`
  ([ADR 0007](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0007-infer-columns-from-cdc-metadata/) amendment).
- A micro-batch is cut into at most `numPartitions` ranges of about 50,000 change rows or
  more, so a batch of fewer than about 100,000 change rows is read in one partition, with
  one connection, instead of one per core
  ([ADR 0015](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0015-split-batches-by-change-rows/) amendment).
- Only a batch's last partition measures where the stream is (retention watermark,
  `max_lsn`, capture lag, end commit time); the other partitions skip those four queries
  ([ADR 0014](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0014-network-and-read-metrics-in-facts/) amendment 5,
  [ADR 0020](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0020-capture-and-ingestion-lag-in-facts/) amendment 2).
- The planner reads each capture instance's captured columns (and which are computed) once
  per query run, and again before checking types whenever a batch holds DDL.
- `stream()` and the reader log a WARNING naming any option they do not know, so a misspelt
  name no longer leaves its default in force silently.
- The reader logs a WARNING with the file when it cannot write a metrics file, instead of
  turning the facts' metrics NULL silently.
- Planning sends fewer queries: each range's start comes from the same query as the split
  points, and after the first resolution the client lists only the table's own capture
  instances, falling back to the whole database when that misses.
- `to_delta` names the query after the sink's `app_id` (`<app_id>.g<n>` in generation `n`)
  when `query_name` is not given, and so does `start_many`, whose queries kept the
  generation-less name after a re-snapshot.
- `backfill(isolation=None)` reads and plans with the stream's `isolationLevel` option
  instead of READ COMMITTED; `isolation` takes `"snapshot"` or `"readCommitted"` in any case
  and is checked before anything is read.
- `is_final` takes an aware `datetime` in UTC instead of raising `TypeError`.
- `advance` and `apply_changes` retry a control-table MERGE, and every table's migrations,
  that lose to a concurrent commit for up to a minute, with backoff, instead of the
  tracker's two retries.
- With `failOnDataLoss=false`, skipping purged changes logs a WARNING naming the capture
  instance and the LSN range.
- An idle stream sends one `max_lsn` query per poll: `reportLatestOffset` reuses what
  `latestOffset` read, and the commit count is skipped when nothing is new.
- The default backend caps Arrow batches near 64 MiB, as `arrow-odbc` does, so LOB-heavy
  rows read in smaller batches; the first batch of each read holds 64 rows, so it stays
  small before the row width is known.
- With `sourceTimeZone=auto` before SQL Server 2022, the driver ships the offset it read to
  the executors, so a run applies one offset to its offsets and its rows, and logs a
  WARNING each time it takes that fallback
  ([ADR 0008](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0008-detect-source-time-zone/) amendment 3).
- The sink logs a warning, once per run, when a batch that read rows found no metrics file.
- Error messages: "capture instance not found" ends with the driver's own error, and the
  change-table permission hint also works for logins whose language is not English: on an
  error naming the change table, the client asks SQL Server (`HAS_PERMS_BY_NAME`) whether
  the login may read it, whatever the language and the driver.
- The Databricks example installs the released version (`==0.2.0rc1`) instead of the
  repository's moving `main`, quotes `UID` and `PWD` in braces, and computes the verdict's
  epoch in UTC.
- Packaging and CI: pyarrow 18 or later; classifiers for Python 3.10 to 3.13, which the
  weekly CI run tests; the release workflow refuses a tag whose commit is not on `main` or
  whose version has no `CHANGELOG.md` heading; `docker compose` binds SQL Server to
  127.0.0.1, where `.env.example` and the lab scripts now point (`localhost` could resolve to
  `::1` first), and `.env.example` ships no password (set `MSSQL_SA_PASSWORD`); every lab result
  records the server's `@@VERSION`.

### Fixed

- `backfill()` completes a chunked snapshot whose `'snapshot_open'` row has no `kind`
  (written by an unreleased build that put it in `mode`), taking generation 0 as a bootstrap
  and a later one as a re-snapshot, instead of failing with `KeyError` once its last chunk
  is in.
- A checkpoint deleted or rewound while `app_id` stayed the same made Delta skip every write
  up to the old batch id while the query ran on and the verdict advanced: with a
  `facts_table`, the run's first batch now raises `ValueError` saying so.
- `on_data_loss="resnapshot"` with a checkpoint that Python and Spark resolve differently (a
  schemeless `/mnt/...` path on Databricks classic, an HDFS default file system, a Spark
  Connect client) found no offsets and re-snapshotted on every run: when the facts table
  holds batches of the stream, the pre-flight now raises `ValueError`.
- A migration re-run after a crash between its change and its version stamp, or by a job
  that lost the race to stamp it, appended duplicate columns and failed.
- `apply_changes` failed on a bronze written with `includeCommandId=false`; it now orders
  by `(_start_lsn, _seqval, _operation)` there.
- Capture instance names with non-ASCII letters (`dbo_Café`) failed every call; any
  name without `]` or a control character, up to 100 characters, is accepted. The
  validators no longer accept a trailing newline.
- `backfill(isolation="SNAPSHOT")` passed the reader's check but failed while planning.
- A stream on a database capture had not written to yet (`sys.fn_cdc_get_max_lsn()` NULL)
  failed with `TypeError` in `latestOffset`: a NULL `max_lsn` is now the zero LSN, as the
  pipeline already took it. `startingLsn=latest` on a capture instance capture has not
  reached yet (a quiet database just after the enable) started below the instance's first
  LSN and failed its first batch with a false `DataLossError`: it starts just before that
  LSN, as a snapshot is stamped, and asks to retry when neither is known.
- A snapshot's own commit is found among every commit after it, not only the last five.
- A table path holding a backtick is escaped in SQL.
- A stream whose first batch created bronze while `backfill()` created it too failed with
  `DELTA_PROTOCOL_CHANGED`; a CREATE that loses the race to another writer now takes the
  table that writer made, for every table the library creates.
- The Databricks example's epoch was off by the driver's UTC offset on a driver not set to
  UTC.

## [0.2.0rc1] - 2026-10-04

State compatibility: offsets and checkpoints unchanged. Existing tables migrate the next
time a stream, `finalization.advance` or `apply_changes` opens them: bronze migration 2 adds
`_snapshot` and `_chunk` (NULL on existing rows, whose snapshot is still found by their
`_start_lsn`) and rewrites the comment of `_start_lsn`; facts migration 7 rewrites the
comments of `rows`, `event` and `detail`, and facts migration 8 those of `event` and
`detail` and the table comment again, and facts migration 9 that of `event` (the facts
schema version is now 9; no column is added); control migration 2 adds `open_snapshot_lsn`
and `snapshot_wave`, and control migration 3 rewrites the comment of `open_snapshot_lsn`. New table kind `reconcile` (the report of `reconcile()`), with no
migrations ([ADR 0013](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0013-schema-migrations-per-table-kind/)).

### Added

- Chunked snapshots read next to the running stream, for tables the link cannot read
  within the CDC retention: `to_delta(..., snapshot="chunked")` (with `bootstrap=True` or
  `on_data_loss="resnapshot"`; needs `facts_table`) records the snapshot's LSN S and the
  key's extent and starts the stream at S without reading the table, and
  `stream(...).backfill(target, *, app_id, facts_table, chunk_rows, max_waves, max_seconds,
  min_headroom_hours, isolation)`, called repeatedly in its own task, reads them in waves of
  `numPartitions` chunks, each stamped with `max_lsn` before its read, one Delta commit per
  wave. The first call plans every chunk and records the plan in a `snapshot_plan` facts row
  that later calls reuse (another `chunk_rows` is ignored with a warning), so the chunks are
  fixed ranges and `chunks_total` is exact. One integer key is counted per slice of a fixed
  grid in one server-side `GROUP BY`, dense slices counted again on a finer grid, and the
  slices packed into chunks of at most `chunk_rows` rows whatever the skew; any other key
  gets keyset bounds `chunk_rows` rows apart. The last chunk ends just above the MAX recorded
  at the open, so the rows inserted since come from the stream alone (for a keyset plan, at
  the first key after it, looked for again before each wave while there is none). New facts
  events
  `snapshot_open`, `snapshot_plan` and `snapshot_chunk` (`last` on the final chunk); the
  `bootstrap` or `resnapshot` row comes after the last chunk. A crash between a wave's append
  and its facts rows reruns the wave without appending twice, rebuilding its facts rows from
  the commit or, once log cleanup dropped it, from the chunks bronze holds, whatever the
  rerun's `numPartitions`; a loss while one is open opens a newer one
  ([ADR 0028](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0028-chunked-snapshot-next-to-the-stream/)).
- Snapshot reader options `snapshotChunks` (read only the given key ranges, numbered in
  `_chunk`, with a `chunk-<i>.json` metrics file each), `snapshotKeys` (the columns those
  ranges apply to, by default the unique index) and `isolationLevel=snapshot` (SNAPSHOT
  isolation where the database allows it; never `NOLOCK`).
- `apply_changes` applies an open chunked snapshot, bootstrap or re-snapshot, wave by wave
  (control columns `open_snapshot_lsn`, `snapshot_wave`): a chunk row never brings back a key
  deleted after its stamp. With one integer, date or timestamp (`datetime2`, `datetime`,
  `smalldatetime`) key that the chunks are cut on, each wave also deletes the silver keys of
  its chunks' ranges that the chunks lack and whose image is older than the chunk's stamp, so
  the keys a re-snapshot's purged gap deleted leave silver wave by wave; keys in a
  `datetime2(7)` bound's microsecond, which Spark cannot place, are left to the rebuild. At
  the snapshot's completion row silver is rebuilt by absence; its verdict is held while a
  snapshot is open ([ADR 0019](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0019-silver-helper-applies-the-change-log/) amended).
- `reconcile(spark, options, silver, keys=None, *, bronze, control_table, facts_table=None,
  bucket_rows, sample, report_table, seed)`: compares silver with its SQL Server table by row
  count and key sum per bucket of an integer or date key (one count for other keys), then row
  by row through a SHA-256 of the captured columns on the mismatched buckets and a sample,
  read in ranges of `keys`, and classifies keys as MISSING_TARGET, MISSING_SOURCE,
  RECORD_DIFF or IN_FLIGHT (against silver's `applied_lsn` in `control_table`). With
  `facts_table` it checks the newest chunked snapshot's chunks against bronze (CHUNK_TILING,
  CHUNK_ROWS, CHUNK_STAMP). The report goes to an optional `report_table`; a "Validation"
  guide covers it.
- Lab check t10 (`lab/checks/t10_chunked_snapshot.py`): a chunked bootstrap next to the
  running stream under a continuous writer (inserts, updates, deletes, key updates) and a
  transaction holding a range's locks, and with `--resnapshot` a chunked re-snapshot after a
  forced cleanup purged a gap with deletes, whose keys leave silver wave by wave. Passes on
  SQL Server 2022 CU27; runs in the CI lab job.

- `stream(...).seed(target, df, as_of)`: seed the target from a copy of the table you already
  have, for tables too big to snapshot within the CDC retention. `as_of` is an LSN or the
  UTC time the copy started, mapped on the server's clock (`sys.fn_cdc_map_time_to_lsn`,
  through the new `CdcClient.time_to_lsn`). The copy is written as snapshot rows with a
  `bootstrap` facts event, and `to_delta(bootstrap=True)` starts from it; a rerun returns
  it, also after cleanup passed `as_of`. Options `allow_missing_columns` and `reseed`
  ([ADR 0025](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0025-seed-from-an-existing-copy/)).
- `finalization.track(spark, query, control_table, table_name)`: continuous-mode
  finalization. A `StreamingQueryListener` advances `finalized_until` after every batch of a
  running query (a `processingTime` trigger or the default), from a worker thread, off the
  listener bus, retrying a failed MERGE twice; `join()` waits for the last verdict after the
  query stops
  ([ADR 0026](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0026-continuous-finalization-listener/)).
- `start_many`, `await_all` and `stop_all` (`mssql_cdc.fanout`): one `to_delta` stream per
  capture instance from `{ci}` templates for target, `app_id` and checkpoint, with
  per-table option overrides, a shared facts table created before the first start, and
  failures isolated per query; a "Many tables" guide covers sizing, one job versus one job
  per table, failure isolation and Databricks
  ([ADR 0027](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0027-fan-out-one-stream-per-table/)).
- CI runs the integration tests that read through the backend (14 of them) a second time
  with `backend=arrow-odbc` (ODBC Driver 18 installed in the job): the `arrow-odbc` backend is
  now tested, except for the heartbeat, facts metrics, re-snapshot, silver, type changes and
  dropped columns
  ([ADR 0003](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0003-mssql-python-default-backend/) Amendment 2).
- Lab check t9 (`lab/checks/t9_capture_instance_switch.py`): a table switched to a new
  capture instance with a new column under a continuous writer, running
  `sql/switch_capture_instance.sql` as written with a least-privilege login. Passes on SQL
  Server 2022 CU27 and 2017 CU31; runs in the CI lab job
  ([ADR 0023](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0023-schema-changes-and-capture-instance-switching/) amended, which also
  records that the production SQL Server 2016 SP3 change tables have `__$command_id`).

### Breaking

- `apply_changes` advances silver's `finalized_until` only when given `facts_table`: a
  chunked snapshot shows in the facts alone until its first wave, and an emptied table's
  re-snapshot only there, so without them silver could claim periods it does not hold. A
  stream without a facts table can pass any name where no table exists.

### Changed

- `to_delta(bootstrap=True)` with either `snapshot` mode, and `snapshot()`, return the S of a
  chunked snapshot of the target, open or complete, instead of reading the table again;
  `seed()` refuses a target that holds one. A recovery that opened a chunked re-snapshot and
  stopped before writing its state opens a newer one, a generation on, once cleanup has
  passed the first.
- One snapshot mode per run, locked while a snapshot is open: with a facts table, a full
  snapshot taken by `to_delta` now writes a `snapshot_open` facts row (detail `mode` `full`,
  `kind` `bootstrap` or `resnapshot`; one per run, at its own LSN) before it reads the table,
  closed by its `bootstrap` or `resnapshot` row. `to_delta` (with `bootstrap=True` or
  `on_data_loss="resnapshot"`), `snapshot()` and `backfill()` raise `ValueError` while a
  snapshot of the stream in the other mode is open, and `seed()` while one in either mode
  is, saying how to finish it; the mode may change between runs. A full one stops counting
  once CDC cleanup passes its LSN, as it can then never complete: a full re-snapshot that
  outlived the retention can be retried with `snapshot="chunked"` in the same generation. `snapshot()` takes new keyword arguments `app_id` and
  `facts_table` for the check
  ([ADR 0028](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0028-chunked-snapshot-next-to-the-stream/) amended).
- `backfill(isolation="snapshot")` plans the chunks under SNAPSHOT isolation too: the
  counts and seeks of the first call and the search for the first key after MAX before each
  wave no longer wait for a writer's locks under READ COMMITTED. `plan_chunks`, `last_bound`
  and the client's `key_buckets`, `key_range` and `key_bound` take an optional `isolation`
  ([ADR 0028](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0028-chunked-snapshot-next-to-the-stream/) amended).
- The file-backed fake gives an update's 3 and 4 rows one `__$seqval` and `__$command_id`,
  as SQL Server does, so the unit tests rank them by `__$operation` alone.
- A snapshot in bronze is named by its `_snapshot` column, never by the largest
  `_start_lsn` of its rows: `snapshot()`, `to_delta(bootstrap=True)`, the re-snapshot's
  reuse check, `seed()`'s reruns and silver's rebuild point read
  `coalesce(_snapshot, _start_lsn)` of whole snapshots, and the facts' completion rows
  ([ADR 0016](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0016-bootstrap-snapshot-at-a-recorded-lsn/) amendment 3,
  [ADR 0025](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0025-seed-from-an-existing-copy/) amended).
- `apply_changes`: operation 3 deletes its own key, outranked by the 4 of the same key and
  commit, instead of being ignored, so a key update SQL Server records as 3 and 4 cannot
  leave the old key in silver.
- The bootstrap guide sizes a snapshot by MB/s (1.8 to 5.9 MB/s measured against a
  production source), not a fixed rows-per-second figure: rows per second vary with the row
  width.
- Snapshot key bounds are bound as text and `CAST` server-side for both backends (binary as
  hex): the same SQL shape and the same seeks
  ([ADR 0016](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0016-bootstrap-snapshot-at-a-recorded-lsn/) amended).
- `docker-compose.yml` takes `MSSQL_IMAGE` to run the lab on another SQL Server version
  (e.g. `mcr.microsoft.com/mssql/server:2017-latest`).

### Fixed

- The `arrow-odbc` backend now reads what `mssql-python` reads:
  - it adds the ODBC Driver 18 keyword when the connection string names no driver, and
    honours `connectTimeout`;
  - text is UTF-16 both ways;
  - `(max)`/`text`/`xml`/`image` values up to 64 KiB (longer ones fail the read);
  - `datetime2` is fetched in microseconds (no overflow past 2262);
  - `datetimeoffset` is converted to its UTC instant;
  - `close()` releases the connection.
- The integration tests retry enabling CDC while the SQL Server Agent is still starting
  (error 14258).

## [0.1.0] - 2026-10-01

State compatibility: first release: offsets v1 contract
([ADR 0002](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0002-lsn-offsets-with-commit-time/)), checkpoint generations
([ADR 0018](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0018-automatic-resnapshot-after-data-loss/)), facts migrations
1-6 (6: the `detail` column of the source's events), bronze migration 1 (the comments of
`_capture_instance` and `_command_id`), control migration 1, and the silver table kind with no
migrations yet ([ADR 0013](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0013-schema-migrations-per-table-kind/)).

### Added

- Streaming source `format("mssql_cdc")` on the Python DataSource V2 API, registered with
  `register(spark)` ([ADR 0001](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0001-python-datasource-v2/)). Offsets are
  the last processed commit LSN as hex plus its commit time
  ([ADR 0002](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0002-lsn-offsets-with-commit-time/)); every batch is a prefix
  of the commit history and ends on a commit boundary.
- `Trigger.AvailableNow` and admission control (`maxCommitsPerBatch`) on Spark 4.2, and on
  DBR 18.2 through its backport; a legacy reader for older Spark.
- Reads of the change table `cdc.<ci>_CT` itself, for `__$command_id`, with only `SELECT`
  on it beyond what the CDC functions need
  ([ADR 0009](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0009-read-change-tables-directly/)).
- Retention guard on the driver and again on the executors after each read: `DataLossError`
  unless `failOnDataLoss=false`.
- Captured columns inferred with `sys.sp_cdc_get_captured_columns`, `columns` to override
  ([ADR 0007](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0007-infer-columns-from-cdc-metadata/)).
- `sourceTimeZone=auto`: the server's time zone from `CURRENT_TIMEZONE_ID()`, or its
  current UTC offset before SQL Server 2022
  ([ADR 0008](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0008-detect-source-time-zone/)); every time column is a
  `TIMESTAMP_NTZ` in UTC.
- `numPartitions=auto`: the compute's cores
  ([ADR 0011](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0011-num-partitions-from-cores/)), with commit-aligned ranges
  balanced by the change table's rows
  ([ADR 0015](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0015-split-batches-by-change-rows/)).
- Drivers: `mssql-python` by default, installed with the package and fetching straight into
  Arrow, and `arrow-odbc` (untested, the `[arrow-odbc]` extra)
  ([ADR 0003](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0003-mssql-python-default-backend/)); a
  file-backed fake (`backend=fake`) for engine tests
  ([ADR 0006](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0006-file-backed-fake-for-engine-tests/)). `mssql-python` is
  imported only when it opens a connection, so the other backends work without its system
  libraries.
- Capture instances match ignoring case, as SQL Server's default collation does: the exact
  name first, and two names that differ only in case raise an error naming both.
  `_capture_instance` keeps the name as the options give it
  ([ADR 0016](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0016-bootstrap-snapshot-at-a-recorded-lsn/), amendment 2).
- Delta sink `sink.delta_sink()`: idempotent appends (`txnAppId`/`txnVersion`) and per-batch
  facts (row counts, LSN and commit-time ranges, timings) in the commit's `userMetadata`
  and a facts table.
- Network and read metrics per partition in the facts, folded from `metricsPath`, one
  directory per stream (`stream()` puts an explicit one's files under `<metricsPath>/<app_id>`)
  ([ADR 0014](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0014-network-and-read-metrics-in-facts/)), and the retention
  headroom ([ADR 0017](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0017-retention-headroom-in-facts/)).
- Capture lag and ingestion lag in the facts (`source_max_commit_ts`, `capture_lag_seconds`,
  `ingestion_lag_seconds`), and `max_lsn` with its commit time in the query progress on
  every trigger (`reportLatestOffset`)
  ([ADR 0020](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0020-capture-and-ingestion-lag-in-facts/)).
- The batch's end offset in the facts (`end_lsn`, `end_commit_ts`, facts migration 5): the
  retention headroom and the ingestion lag are measured from it, not from the batch's last
  change. Every micro-batch writes a facts row, those that read no rows included
  (`rows = 0`), so facts stop arriving only when the stream or CDC capture stops (ADR 0014
  amendment 3, ADRs 0017 and 0020 amended).
- Completeness signal `finalized_until` in a control table, advanced after the data and
  never backwards: `finalization.candidate()`, `advance()`, `is_final()`,
  `end_offset_from_progress()`
  ([ADR 0004](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0004-verdict-in-control-table/),
  [ADR 0005](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0005-ordering-over-atomicity/)).
- Control, facts and bronze tables created typed and commented through the `DeltaTable` API
  ([ADR 0012](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0012-delta-tables-through-the-deltatable-api/)), with
  append-only schema migrations per table kind
  ([ADR 0013](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0013-schema-migrations-per-table-kind/)).
- `stream(spark, options).to_delta(...)`: source and sink from one set of options.
- Bootstrap snapshot stamped with the `max_lsn` recorded before the read:
  `stream(...).snapshot(target)`, `to_delta(bootstrap=True)` and
  `format("mssql_cdc_snapshot")`
  ([ADR 0016](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0016-bootstrap-snapshot-at-a-recorded-lsn/)). Its partitions
  are uniform ranges of a single integer key, else `NTILE` tiles of the rows by the whole
  key, for composite and non-integer keys (ADR 0016 amendment).
- Automatic re-snapshot after CDC data loss, `to_delta(on_data_loss="resnapshot")`, into a
  new checkpoint generation, with the loss recorded in the facts
  ([ADR 0018](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0018-automatic-resnapshot-after-data-loss/)).
- Silver helper `apply_changes`: keeps a current-state table equal to the source from the
  bronze change log with MERGE, rebuilds it from a newer snapshot (bootstrap or
  re-snapshot), keeps its position in the control table (`applied_lsn`, `snapshot_lsn`,
  control migration 1) and propagates `finalized_until`
  ([ADR 0019](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0019-silver-helper-applies-the-change-log/)).
- Schema changes on the source and a second capture instance
  ([ADR 0023](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0023-schema-changes-and-capture-instance-switching/)): every
  batch checks the DDL SQL Server recorded inside it (`sys.sp_cdc_get_ddl_history`); a
  captured column whose new type the query's no longer holds fails the batch before it reads
  (`SchemaChangedError`, exported), other DDL goes on with a `schema_change` facts row, and
  `schemaChangePolicy=fail` fails on any DDL. The stream follows a newer capture instance of
  its table at that instance's start LSN, within one batch, and writes a
  `capture_instance_switched` facts row; the schema is the union of both instances' columns.
  Bronze appends use `mergeSchema`, and a type bronze cannot take fails with a message about
  `delta.enableTypeWidening`. `to_delta(snapshot_on_switch=True)` snapshots the table after
  the switch. Snapshots read NULL for a dropped column and are found under any instance of
  the table; a `DataLossError` caused by an old instance disabled too early says so, and a
  `PermissionError` names the new change table's grant. `sql/switch_capture_instance.sql`
  documents the DBA's procedure.
- `sql/heartbeat.sql`, an optional Agent job that keeps `max_lsn` moving on a quiet
  database ([ADR 0010](https://emanuel-luis.github.io/mssql-cdc-pyspark/decisions/0010-heartbeat-for-quiet-databases/)).
- The lab: a Faker OLTP workload and checks t1-t8 against SQL Server 2022, Spark and Delta
  (`LAB.md`).
- Inline type hints (`py.typed`).
- `import mssql_cdc` without PySpark raises an `ImportError` that says to run on a Spark
  platform, which ships its own, or to install the `[spark]` extra.

[Unreleased]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.1.0...v0.2.0
[0.2.0rc2]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.2.0rc1...v0.2.0rc2
[0.2.0rc1]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.1.0...v0.2.0rc1
[0.1.0]: https://github.com/emanuel-luis/mssql-cdc-pyspark/releases/tag/v0.1.0
