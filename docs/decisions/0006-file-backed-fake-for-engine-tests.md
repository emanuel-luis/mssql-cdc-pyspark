# 0006: A file-backed CDC fake to test the real Spark engine

**Status:** accepted  
**Date:** 2026-09-28T16:13:29-03:00 (recorded when the repository was first committed; decided before)

## Context
Most bugs in a streaming source live in the interaction with the engine: offsets,
checkpoint replays, `AvailableNow`, read limits. Mocking the engine hides them, and a SQL
Server is not always available (developer laptops, restricted networks).

## Decision
`FakeCdcClient` and `FakeCdcDatabase` simulate `cdc.lsn_time_mapping` (with idle entries),
change rows in commit order, and cleanup of `min_lsn`, stored in files so the driver and
executor processes share state. Unit tests run real Spark streaming against it.

## Consequences
* Fast engine tests with no SQL Server.
* The fake must follow real semantics; lab checks are the source of truth, and the fake
  gets fixed when they disagree.
