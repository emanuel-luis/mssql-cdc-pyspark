# 0004: `finalized_until` in a control table, never in table properties

**Status:** accepted (2026-09)

## Context
Pinterest stores its finalization watermark as an Iceberg table property. On Delta, and
per Databricks docs, a table-property change is a metadata commit: "Metadata changes
might cause all concurrent write operations to fail", and streaming reads "fail when they
encounter a commit that changes table metadata".

## Decision
A small Delta control table, one row per target table, updated with MERGE. Per-batch
facts go into the data commit's `userMetadata` and a facts table.

## Consequences
* No interference with writers or streaming readers of the data table.
* The verdict is not in the same commit as the data (see 0005).
* `commitInfo` does not survive checkpoints, so the facts table is the durable record.
