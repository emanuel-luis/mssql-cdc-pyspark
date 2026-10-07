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
are `TypedDict`s, plain dicts at run time; a minor release may add keys to them, never remove
one.

::: mssql_cdc.types
    options:
      show_root_toc_entry: false
      members: [Offset, BackfillStatus, ApplyResult, ReconcileResult, SnapshotMode, OnDataLoss, Isolation, Granularity, BackfillState]

## Errors

::: mssql_cdc.DataLossError

::: mssql_cdc.SchemaChangedError

::: mssql_cdc.is_data_loss

::: mssql_cdc.is_schema_changed

## Local Spark

::: mssql_cdc.spark.get_spark
