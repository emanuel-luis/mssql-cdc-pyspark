# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with the 0.x
policy of [ADR 0021](docs/decisions/0021-compatibility-policy-for-0x.md): a minor release
may break the Python API (listed under "Breaking"), a patch only fixes, and the state a
stream leaves behind (offsets, checkpoint layout, table schemas) never breaks without a
migration path. Every release says what it does to that state in its "State
compatibility" line.

## [Unreleased]

## [0.1.0] - unreleased

State compatibility: first release: offsets v1 contract
([ADR 0002](docs/decisions/0002-lsn-offsets-with-commit-time.md)), checkpoint generations
([ADR 0018](docs/decisions/0018-automatic-resnapshot-after-data-loss.md)), facts migrations
1-3 ([ADR 0013](docs/decisions/0013-schema-migrations-per-table-kind.md)).

### Added

- Streaming source `format("mssql_cdc")` on the Python DataSource V2 API, registered with
  `register(spark)` ([ADR 0001](docs/decisions/0001-python-datasource-v2.md)). Offsets are
  the last processed commit LSN as hex plus its commit time
  ([ADR 0002](docs/decisions/0002-lsn-offsets-with-commit-time.md)); every batch is a prefix
  of the commit history and ends on a commit boundary.
- `Trigger.AvailableNow` and admission control (`maxCommitsPerBatch`) on Spark 4.2, and on
  DBR 18.2 through its backport; a legacy reader for older Spark.
- Reads of the change table `cdc.<ci>_CT` itself, for `__$command_id`, with only `SELECT`
  on it beyond what the CDC functions need
  ([ADR 0009](docs/decisions/0009-read-change-tables-directly.md)).
- Retention guard on the driver and again on the executors after each read: `DataLossError`
  unless `failOnDataLoss=false`.
- Captured columns inferred with `sys.sp_cdc_get_captured_columns`, `columns` to override
  ([ADR 0007](docs/decisions/0007-infer-columns-from-cdc-metadata.md)).
- `sourceTimeZone=auto`: the server's time zone from `CURRENT_TIMEZONE_ID()`, or its
  current UTC offset before SQL Server 2022
  ([ADR 0008](docs/decisions/0008-detect-source-time-zone.md)); every time column is a
  `TIMESTAMP_NTZ` in UTC.
- `numPartitions=auto`: the compute's cores
  ([ADR 0011](docs/decisions/0011-num-partitions-from-cores.md)), with commit-aligned ranges
  balanced by the change table's rows
  ([ADR 0015](docs/decisions/0015-split-batches-by-change-rows.md)).
- Drivers: `mssql-python` by default, fetching straight into Arrow, and `arrow-odbc`
  (untested) ([ADR 0003](docs/decisions/0003-mssql-python-default-backend.md)); a
  file-backed fake (`backend=fake`) for engine tests
  ([ADR 0006](docs/decisions/0006-file-backed-fake-for-engine-tests.md)).
- Delta sink `sink.delta_sink()`: idempotent appends (`txnAppId`/`txnVersion`) and per-batch
  facts (row counts, LSN and commit-time ranges, timings) in the commit's `userMetadata`
  and a facts table.
- Network and read metrics per partition in the facts, folded from `metricsPath`
  ([ADR 0014](docs/decisions/0014-network-and-read-metrics-in-facts.md)), and the retention
  headroom ([ADR 0017](docs/decisions/0017-retention-headroom-in-facts.md)).
- Completeness signal `finalized_until` in a control table, advanced after the data and
  never backwards: `finalization.candidate()`, `advance()`, `is_final()`,
  `end_offset_from_progress()`
  ([ADR 0004](docs/decisions/0004-verdict-in-control-table.md),
  [ADR 0005](docs/decisions/0005-ordering-over-atomicity.md)).
- Control, facts and bronze tables created typed and commented through the `DeltaTable` API
  ([ADR 0012](docs/decisions/0012-delta-tables-through-the-deltatable-api.md)), with
  append-only schema migrations per table kind
  ([ADR 0013](docs/decisions/0013-schema-migrations-per-table-kind.md)).
- `stream(spark, options).to_delta(...)`: source and sink from one set of options.
- Bootstrap snapshot stamped with the `max_lsn` recorded before the read:
  `stream(...).snapshot(target)`, `to_delta(bootstrap=True)` and
  `format("mssql_cdc_snapshot")`
  ([ADR 0016](docs/decisions/0016-bootstrap-snapshot-at-a-recorded-lsn.md)).
- Automatic re-snapshot after CDC data loss, `to_delta(on_data_loss="resnapshot")`, into a
  new checkpoint generation, with the loss recorded in the facts
  ([ADR 0018](docs/decisions/0018-automatic-resnapshot-after-data-loss.md)).
- `sql/heartbeat.sql`, an optional Agent job that keeps `max_lsn` moving on a quiet
  database ([ADR 0010](docs/decisions/0010-heartbeat-for-quiet-databases.md)).
- The lab: a Faker OLTP workload and checks t1-t8 against SQL Server 2022, Spark and Delta
  (`LAB.md`).
- Inline type hints (`py.typed`).

[Unreleased]: https://github.com/emanuel-luis/mssql-cdc-pyspark/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/emanuel-luis/mssql-cdc-pyspark/releases/tag/v0.1.0
