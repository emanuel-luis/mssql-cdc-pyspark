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

[Unreleased]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/emanuel-luis/mssql-cdc-pyspark/releases/tag/v0.1.0
