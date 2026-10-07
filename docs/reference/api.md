# API reference

The public Python surface ([ADR 0021](../decisions/0021-compatibility-policy-for-0x.md)),
generated from the docstrings. Everything not listed here is internal and may change in
any release. The options a stream takes are in [Options](options.md). `CdcStream` is listed
for its methods: it is what `stream()` returns, and only that call creates one.

Leading arguments are positional (`to_delta(target, app_id, checkpoint, facts_table)`,
`apply_changes(spark, bronze, target)`); options, flags and modes after them are
keyword-only (since 0.3).

## Pipeline

::: mssql_cdc.stream

::: mssql_cdc.pipeline.CdcStream
    options:
      members: [to_delta, backfill, snapshot, seed]
      inherited_members: true

::: mssql_cdc.register

## Many tables

::: mssql_cdc.start_many

::: mssql_cdc.await_all

::: mssql_cdc.stop_all

## Silver

::: mssql_cdc.apply_changes

## Validation

::: mssql_cdc.reconcile

## Finalization

::: mssql_cdc.finalization
    options:
      show_root_toc_entry: false
      members: [advance, track, FinalizationListener, finalized_until, is_final, candidate, end_offset_from_progress]

`FinalizationListener` is listed for its `join` and its `last_error`. Create it with `track`, which also
registers it and starts its worker. `finalized_until(spark, control_table, table_name)`
returns the table's verdict, a naive UTC `datetime`, or `None` before the first one.

## Sink

::: mssql_cdc.sink.delta_sink

## Types

Exported from `mssql_cdc`. The mode parameters take a `Literal`, so a type checker flags a
misspelt mode; a wrong value still raises `ValueError` naming the allowed ones. The results
are `TypedDict`s, plain dicts at run time; a minor release may add keys to them, and removing
or renaming one is a break listed under "Breaking" in the changelog.

::: mssql_cdc.types
    options:
      show_root_toc_entry: false
      members: [Offset, BackfillStatus, ApplyResult, ReconcileResult, SnapshotMode, OnDataLoss, Isolation, Granularity, BackfillState, SparkSessionLike, StreamingQueryLike]

::: mssql_cdc.source.SourceOptions
    options:
      show_if_no_docstring: true

## Protocols

The seams a client and a driver plug into
([ADR 0030](../decisions/0030-protocols-for-the-pluggable-seams.md)), exported from
`mssql_cdc`. Both are `typing.Protocol`s, `runtime_checkable`: a class with their methods is
one without inheriting, and `isinstance` checks that it has them (by name; a type checker
checks the signatures). A subclass inherits the methods that have a body. The data source
builds its client from its options, so the [backend](options.md#backend) option names a
built-in backend only. A method added to a protocol is a break for a class that implements
it without inheriting, listed under "Breaking" in the changelog.

::: mssql_cdc.Backend

::: mssql_cdc.CdcClient

::: mssql_cdc.Lsn

## Payloads

The JSON in the facts table's `detail` and in the `userMetadata` of a chunked snapshot's
wave commits ([Tables](tables.md#facts)), as `TypedDict`s for what `json.loads` returns.
The row payloads are exported from `mssql_cdc`; the nested types are in `mssql_cdc.payloads`.
Their keys are state: a release only adds one.

::: mssql_cdc.payloads
    options:
      show_root_toc_entry: false
      inherited_members: true
      show_if_no_docstring: true
      members: [SnapshotOpenDetail, SnapshotPlanDetail, SnapshotChunkDetail, SnapshotCompletionDetail, DataSkippedDetail, BatchDetail, WaveMetadata, WaveChunk, SnapshotExtent, IntExtent, KeysetExtent, SnapshotKind]

## Errors

::: mssql_cdc.DataLossError

::: mssql_cdc.SchemaChangedError

::: mssql_cdc.is_data_loss

::: mssql_cdc.is_schema_changed

## Local Spark

::: mssql_cdc.spark.get_spark
