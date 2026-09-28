# Architecture Decision Records

| # | Decision | Status | Date | Amended |
|---|---|---|---|---|
| [0001](0001-python-datasource-v2.md) | Python DataSource V2 instead of a JVM connector | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0002](0002-lsn-offsets-with-commit-time.md) | Offsets are hex LSNs carrying the commit time | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0003](0003-mssql-python-default-backend.md) | `mssql-python` as default driver, `arrow-odbc` as fallback | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0004](0004-verdict-in-control-table.md) | `finalized_until` in a control table, never in table properties | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0005](0005-ordering-over-atomicity.md) | Data first, verdict after, monotonic | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0006](0006-file-backed-fake-for-engine-tests.md) | A file-backed CDC fake to test the real Spark engine | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0007](0007-infer-columns-from-cdc-metadata.md) | Infer captured columns from CDC metadata | accepted | 2026-09-28T17:03:21-03:00 | 2026-09-28T18:11:50-03:00 |
| [0008](0008-detect-source-time-zone.md) | Detect the server time zone by name | accepted | 2026-09-28T17:05:54-03:00 | 2026-09-28T20:19:40-03:00 |
| [0009](0009-read-change-tables-directly.md) | Read the change table directly, re-check retention after the read | accepted | 2026-09-28T17:39:20-03:00 | 2026-09-28T18:11:50-03:00 |
| [0010](0010-heartbeat-for-quiet-databases.md) | Idle lag of ~5 minutes; an optional Agent heartbeat for less | accepted | 2026-09-28T19:03:23-03:00 |  |
| [0011](0011-num-partitions-from-cores.md) | `numPartitions` defaults to the compute's cores | accepted | 2026-09-28T20:55:34-03:00 |  |

New ADRs: copy the format, next number, one decision per file. Put the time the
decision is recorded in `**Date:**` (ISO-8601 with the UTC offset, e.g.
`2026-09-28T17:03:21-03:00`; the commit time works) and add an `**Amended:**` line with
the time and a few words for every later change, here and in the table above.
