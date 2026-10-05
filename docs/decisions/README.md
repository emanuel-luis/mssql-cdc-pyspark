# Architecture Decision Records

| # | Decision | Status | Date | Amended |
|---|---|---|---|---|
| [0001](0001-python-datasource-v2.md) | Python DataSource V2 instead of a JVM connector | accepted | 2026-09-28T16:13:29-03:00 | 2026-09-30T11:07:08-03:00 |
| [0002](0002-lsn-offsets-with-commit-time.md) | Offsets are hex LSNs carrying the commit time | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0003](0003-mssql-python-default-backend.md) | `mssql-python` as default driver, `arrow-odbc` as fallback | accepted | 2026-09-28T16:13:29-03:00 | 2026-09-28T21:44:31-03:00, 2026-09-30T16:14:54-03:00, 2026-10-01T17:55:59-03:00 |
| [0004](0004-verdict-in-control-table.md) | `finalized_until` in a control table, never in table properties | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0005](0005-ordering-over-atomicity.md) | Data first, verdict after, monotonic | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0006](0006-file-backed-fake-for-engine-tests.md) | A file-backed CDC fake to test the real Spark engine | accepted | 2026-09-28T16:13:29-03:00 |  |
| [0007](0007-infer-columns-from-cdc-metadata.md) | Infer captured columns from CDC metadata | accepted | 2026-09-28T17:03:21-03:00 | 2026-09-28T18:11:50-03:00, 2026-10-01T17:55:59-03:00 |
| [0008](0008-detect-source-time-zone.md) | Detect the server time zone by name | accepted | 2026-09-28T17:05:54-03:00 | 2026-09-28T20:19:40-03:00, 2026-09-28T21:44:31-03:00, 2026-10-05T00:15:23-03:00 |
| [0009](0009-read-change-tables-directly.md) | Read the change table directly, re-check retention after the read | accepted | 2026-09-28T17:39:20-03:00 | 2026-09-28T18:11:50-03:00 |
| [0010](0010-heartbeat-for-quiet-databases.md) | Idle lag of ~5 minutes; an optional Agent heartbeat for less | accepted | 2026-09-28T19:03:23-03:00 |  |
| [0011](0011-num-partitions-from-cores.md) | `numPartitions` defaults to the compute's cores | accepted | 2026-09-28T20:55:34-03:00 | 2026-09-29T23:02:40-03:00 |
| [0012](0012-delta-tables-through-the-deltatable-api.md) | Delta tables through the `DeltaTable` API, created typed and commented | accepted | 2026-09-28T21:09:56-03:00 | 2026-09-28T21:15:37-03:00, 2026-09-28T21:26:25-03:00 |
| [0013](0013-schema-migrations-per-table-kind.md) | Schema migrations per table kind | accepted | 2026-09-28T21:26:25-03:00 | 2026-10-05T06:01:09-03:00 |
| [0014](0014-network-and-read-metrics-in-facts.md) | Network and read metrics in the ingestion facts | accepted | 2026-09-29T10:12:04-03:00 | 2026-09-29T10:48:30-03:00, 2026-09-29T22:15:55-03:00, 2026-09-30T15:16:41-03:00, 2026-09-30T17:47:32-03:00 |
| [0015](0015-split-batches-by-change-rows.md) | Split batches by the change table's rows | accepted | 2026-09-29T10:31:46-03:00 |  |
| [0016](0016-bootstrap-snapshot-at-a-recorded-lsn.md) | Bootstrap with a snapshot stamped with an LSN recorded before the read | accepted | 2026-09-29T12:38:28-03:00 | 2026-09-30T11:02:01-03:00, 2026-09-30T15:21:04-03:00, 2026-10-01T17:55:59-03:00, 2026-10-02T20:30:12-03:00, 2026-10-03T18:10:05-03:00 |
| [0017](0017-retention-headroom-in-facts.md) | Retention headroom in the ingestion facts | accepted | 2026-09-29T14:58:53-03:00 | 2026-09-30T15:16:41-03:00, 2026-09-30T17:47:32-03:00 |
| [0018](0018-automatic-resnapshot-after-data-loss.md) | Automatic re-snapshot after CDC data loss | accepted | 2026-09-29T17:46:41-03:00 | 2026-10-02T20:30:12-03:00, 2026-10-03T18:10:05-03:00 |
| [0019](0019-silver-helper-applies-the-change-log.md) | A silver helper applies the bronze change log to a current-state table | accepted | 2026-09-30T11:07:22-03:00 | 2026-10-02T20:30:12-03:00, 2026-10-03T14:30:38-03:00 |
| [0020](0020-capture-and-ingestion-lag-in-facts.md) | Capture and ingestion lag in the ingestion facts | accepted | 2026-09-30T10:33:11-03:00 | 2026-09-30T15:16:41-03:00 |
| [0021](0021-compatibility-policy-for-0x.md) | Compatibility policy for 0.x: the state contract is stable | accepted | 2026-09-30T11:07:08-03:00 | 2026-10-01T15:55:00-03:00, 2026-10-05T06:01:09-03:00 |
| [0022](0022-defer-spark-changes-changelog.md) | Defer a Spark `CHANGES` changelog connector | accepted | 2026-09-30T11:07:08-03:00 |  |
| [0023](0023-schema-changes-and-capture-instance-switching.md) | Schema changes on the source, and switching to a newer capture instance | accepted | 2026-09-30T18:40:00-03:00 | 2026-09-30T20:09:44-03:00, 2026-09-30T21:25:30-03:00, 2026-10-01T15:55:00-03:00, 2026-10-01T17:25:27-03:00, 2026-10-01T18:15:53-03:00 |
| [0024](0024-documentation-site.md) | A documentation site built by Zensical, hosted on GitHub Pages | accepted | 2026-10-01T14:43:57-03:00 |  |
| [0025](0025-seed-from-an-existing-copy.md) | Seed a target from an existing copy of the table | accepted | 2026-10-01T16:53:59-03:00 | 2026-10-01T19:44:50-03:00, 2026-10-02T20:30:12-03:00 |
| [0026](0026-continuous-finalization-listener.md) | Continuous-mode finalization through a streaming query listener | accepted | 2026-10-01T16:36:15-03:00 | 2026-10-01T19:44:50-03:00 |
| [0027](0027-fan-out-one-stream-per-table.md) | Many tables: one stream per capture instance, started by a fan-out helper | accepted | 2026-10-01T16:30:09-03:00 |  |
| [0028](0028-chunked-snapshot-next-to-the-stream.md) | Chunked snapshots read next to the running stream | accepted | 2026-10-02T20:30:12-03:00 | 2026-10-03T14:30:38-03:00, 2026-10-03T18:10:05-03:00, 2026-10-04T17:20:07-03:00 |
| [0029](0029-driver-retries-and-lock-timeout.md) | Driver-side retries, and an optional lock timeout | accepted | 2026-10-05T12:23:04-03:00 |  |

New ADRs: copy the format, next number, one decision per file. Put the time the
decision is recorded in `**Date:**` (ISO-8601 with the UTC offset, e.g.
`2026-09-28T17:03:21-03:00`; the commit time works) and add an `**Amended:**` line with
the time and a few words for every later change, here and in the table above.
