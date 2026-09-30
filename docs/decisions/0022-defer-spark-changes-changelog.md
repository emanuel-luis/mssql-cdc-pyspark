# 0022: Defer a Spark `CHANGES` changelog connector

**Status:** accepted  
**Date:** 2026-09-30T11:07:08-03:00

## Context
Spark 4.2 added change data capture to DSv2 (SPARK-55668, and SPARK-55948 to
SPARK-56687): `SELECT ... CHANGES FROM VERSION ...` resolves through
`TableCatalog.loadChangelog`, which takes a `ChangelogContext` and a `ChangelogRange` and
returns a `Changelog`. `docs/DESIGN.md` shows how SQL Server CDC maps onto it; the roadmap
had a Scala/Java implementation in v0.3. What research found:

* `Changelog` is annotated `@Evolving`, and the API is already changing (SPARK-59841).
* A changelog must return `_change_type`, `_commit_version` (String or Long, whose natural
  order is the commit order) and `_commit_timestamp` (`TimestampType`). With
  post-processing on, `_commit_timestamp` must strictly increase across batches; rows that
  do not are dropped as late.
* It is JVM-only: a catalog implements it in Scala or Java, and PySpark has no way to
  implement it. That confirms ADR 0001: the Python source cannot offer `CHANGES`.
* The DBR 19 release notes list only batch post-processing of `ChangelogTable`; nothing
  shows a third-party catalog serving `CHANGES` there.
* `tran_end_time` is a `datetime`, precise to about 3.33 ms, so different commits can share
  a commit timestamp. Strictly increasing timestamps across batches would need batch cuts
  aligned to timestamps (every commit with the same `tran_end_time` in the same batch),
  not only to commits as today.

## Decision
Defer. Nothing is built for `CHANGES` now.

Considered:
* A full Scala connector (catalog, `Changelog`, the source reimplemented on the JVM):
  about 21 to 32 days, a JAR per Spark and Scala version, against an `@Evolving` API that
  is already moving.
* A JVM shim over the Python source: less code, but still a JAR per Spark version, two
  runtimes to debug, and the same moving API.
* Changelog-shaped columns in Python (`_change_type`, `_commit_version`,
  `_commit_timestamp` as an output option): cheap, but it gives the column names without
  the `CHANGES` syntax, and nobody has asked for it.

Revisit when `Changelog` is no longer `@Evolving` and a third-party catalog is shown working
with `CHANGES` on DBR 19. Add the Python changelog-shaped option only if a user asks for it.

## Consequences
* The roadmap moves the item out of v0.3 into "Later", pointing here; v0.3 is the PyPI
  release.
* ADR 0001 stands: one Python package, no JAR.
* Whoever picks this up starts from the mapping in `docs/DESIGN.md` and the timestamp-aligned
  cuts above.
