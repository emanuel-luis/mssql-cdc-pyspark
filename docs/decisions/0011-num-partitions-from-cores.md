# 0011: `numPartitions` defaults to the compute's cores

**Status:** accepted  
**Date:** 2026-09-28T20:55:34-03:00  
**Amended:** 2026-09-29T23:02:40-03:00, no local CPU fallback in `register()` on Spark Connect

## Context
`numPartitions` splits each micro-batch into commit-aligned LSN ranges read in
parallel. It defaulted to `1`, so a batch read by one task on one connection no matter
the cluster size, and every user had to pick a number. The team's convention for SQL
Server extraction is to size parallelism from the cores the compute exposes
(`defaultParallelism`, falling back to the CPU count).

The data source cannot ask Spark directly: `partitions()` runs in a Python worker on the
driver, which has no SparkSession.

## Decision
* `numPartitions` defaults to `auto`; an explicit integer still wins.
* `register(spark)` reads the cores in the caller's process (`spark.sparkContext.defaultParallelism`;
  `mssql_cdc.spark.available_cores`) and registers a subclass of
  `MssqlCdcDataSource` carrying them as `default_num_partitions`. Spark pickles the
  registered class by value, so the attribute reaches the planning worker.
* Without `register()` (the class registered directly), or on Spark Connect where
  `sparkContext` is missing, `auto` falls back to the CPU count of the node that plans.
  `register()` never uses its own process's CPU count: on Databricks Connect that would be
  the laptop's.

## Consequences
* Each partition opens its own connection to SQL Server: on a large cluster `auto`
  means that many concurrent reads against the source. Set `numPartitions` explicitly
  to cap the load on a busy production server.
* With more than one partition, each batch runs one extra planning query
  (`split_points`, an `NTILE` over `cdc.lsn_time_mapping`).
* `tests/test_source_fake.py` checks the pickling and that a stream without the option
  plans `defaultParallelism` ranges.
