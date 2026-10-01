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
