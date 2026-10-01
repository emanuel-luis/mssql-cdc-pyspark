# API reference

The public Python surface ([ADR 0021](../decisions/0021-compatibility-policy-for-0x.md)),
generated from the docstrings. Everything not listed here is internal and may change in
any release. The options a stream takes are in [Options](options.md). `CdcStream` is listed
for its methods: it is what `stream()` returns, and only that call creates one.

## Pipeline

::: mssql_cdc.stream

::: mssql_cdc.pipeline.CdcStream
    options:
      members: [to_delta, snapshot, seed]

::: mssql_cdc.register

## Silver

::: mssql_cdc.apply_changes

## Finalization

::: mssql_cdc.finalization
    options:
      show_root_toc_entry: false
      members: [advance, track, FinalizationListener, is_final, candidate, end_offset_from_progress]

`FinalizationListener` is listed for its `join`. Create it with `track`, which also
registers it and starts its worker.

## Sink

::: mssql_cdc.sink.delta_sink

## Errors

::: mssql_cdc.DataLossError

::: mssql_cdc.SchemaChangedError

## Local Spark

::: mssql_cdc.spark.get_spark
