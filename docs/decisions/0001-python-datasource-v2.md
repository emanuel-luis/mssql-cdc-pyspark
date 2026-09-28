# 0001: Python DataSource V2 instead of a JVM connector

**Status:** accepted  
**Date:** 2026-09-28T16:13:29-03:00 (recorded when the repository was first committed; decided before)

## Context
The project must be platform-agnostic and "100% PySpark": installable with pip on local
Spark, Databricks classic and other Spark runtimes, without building JARs against a
specific Spark/Scala version.

## Decision
Implement the source with `pyspark.sql.datasource` (`DataSource`,
`DataSourceStreamReader`). Require Spark 4.2 semantics (admission control,
`Trigger.AvailableNow`; SPARK-55304) and keep a legacy reader for older runtimes.

## Consequences
* No JAR, one package for every platform.
* Driver-side offset methods run in a Python worker; `read()` runs in executor Python
  workers, which need the SQL Server driver installed.
* No column pruning, no streaming filter pushdown, no `ReportsSourceMetrics`, and only
  built-in `ReadLimit`s. `maxCommitsPerBatch` reuses `ReadMaxRows` with commit semantics.
* Spark 4.2's DSv2 `Changelog`/`CHANGES` API is JVM-only; that is a later, separate
  deliverable (roadmap v0.3).
