# 0002: Offsets are hex LSNs carrying the commit time

**Status:** accepted  
**Date:** 2026-09-28T16:13:29-03:00 (recorded when the repository was first committed; decided before)

## Context
Offsets must be JSON dicts of primitives. SQL Server LSNs are 10-byte binaries. A
batch's end LSN can be an idle "dummy" entry in `cdc.lsn_time_mapping`, with no change
rows at all.

## Decision
`{"lsn": "0x" + 20 uppercase hex, "commit_ts": "<UTC ISO-8601 ms>"}`, where `lsn` is the
last processed commit LSN and `commit_ts` its commit time. One offset per stream; the LSN
space is database-wide.

## Consequences
* Fixed-width hex: string order equals LSN order; readable in checkpoints.
* Finalization reads progress from the offset, not from rows, so it advances on idle
  tables.
* This is a checkpoint contract. Changing it requires a migration story.
