# Architecture Decision Records

| # | Decision | Status |
|---|---|---|
| [0001](0001-python-datasource-v2.md) | Python DataSource V2 instead of a JVM connector | accepted |
| [0002](0002-lsn-offsets-with-commit-time.md) | Offsets are hex LSNs carrying the commit time | accepted |
| [0003](0003-mssql-python-default-backend.md) | `mssql-python` as default driver, `arrow-odbc` as fallback | accepted |
| [0004](0004-verdict-in-control-table.md) | `finalized_until` in a control table, never in table properties | accepted |
| [0005](0005-ordering-over-atomicity.md) | Data first, verdict after, monotonic | accepted |
| [0006](0006-file-backed-fake-for-engine-tests.md) | A file-backed CDC fake to test the real Spark engine | accepted |
| [0007](0007-infer-columns-from-cdc-metadata.md) | Infer captured columns from CDC metadata | accepted |
| [0008](0008-detect-source-time-zone.md) | Detect the server time zone by name | accepted |
| [0009](0009-read-change-tables-directly.md) | Read the change table directly, re-check retention after the read | accepted |
| [0010](0010-heartbeat-for-quiet-databases.md) | Idle lag of ~5 minutes; an optional Agent heartbeat for less | accepted |

New ADRs: copy the format, next number, one decision per file.
