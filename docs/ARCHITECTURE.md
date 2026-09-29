# Architecture

## Components

```mermaid
flowchart TB
  subgraph src/mssql_cdc
    DS[source.MssqlCdcDataSource<br/>schema, streamReader]
    RD[source.MssqlCdcStreamReader<br/>offsets, partitions, read]
    CL[client.CdcClient]
    SQL[client.SqlCdcClient<br/>T-SQL]
    BE1[MssqlPythonBackend<br/>cursor.arrow_batch]
    BE2[ArrowOdbcBackend<br/>read_arrow_batches]
    FK[fake.FakeCdcClient<br/>files]
    SK[sink.delta_sink<br/>foreachBatch]
    FN[finalization<br/>advance / is_final]
  end
  DS --> RD --> CL
  CL --> SQL --> BE1 & BE2
  CL --> FK
  RD -. micro-batches .-> SK
  SK -. after commit .-> FN
```

* **Source**: Spark Python DataSource V2 (`pyspark.sql.datasource`). One stream per
  capture instance.
* **Client**: the only code that knows T-SQL. The reader depends on the `CdcClient`
  interface, so the fake can replace SQL Server in tests.
* **Backends**: turn a query into Arrow record batches. Interchangeable because every
  LSN is a hex string on the wire (invariant 6 in `CLAUDE.md`).
* **Sink**: an optional, Delta-specific `foreachBatch` writer. The source works with any
  sink.
* **Finalization**: a control table with one row per target table.

## Where code runs

| Method | Process | Notes |
|---|---|---|
| `DataSource.schema()` | driver-side Python worker | returns a DDL string; no JVM access |
| `initialOffset`, `latestOffset`, `getDefaultReadLimit`, `prepareForTriggerAvailableNow`, `reportLatestOffset`, `partitions`, `commit` | one long-lived driver-side Python worker per query | may keep state (`_target`, cached client) |
| `read(partition)` | executor Python workers, one call per partition | stateless; opens its own connection; the reader is pickled without `_client` |

## One micro-batch

```mermaid
sequenceDiagram
  participant E as Spark engine
  participant R as Reader (driver)
  participant X as read() (executor)
  participant S as SQL Server
  participant D as Delta sink
  participant F as Finalization

  E->>R: latestOffset(start, ReadMaxRows(n))
  R->>S: fn_cdc_get_max_lsn()  (or AvailableNow target)
  R->>S: n-th start_lsn after start in lsn_time_mapping
  R->>S: fn_cdc_map_lsn_to_time(end)  -> commit_ts (UTC)
  R-->>E: end = {lsn, commit_ts}
  Note over E: offset log written (checkpoint)
  E->>R: partitions(start, end)
  R->>S: fn_cdc_increment_lsn(start), fn_cdc_get_min_lsn(ci)
  R-->>E: [LsnRange(from, to)]  or DataLossError
  E->>X: read(LsnRange)
  X->>S: cdc.ci_CT WHERE start_lsn BETWEEN from AND to, JOIN lsn_time_mapping
  S-->>X: Arrow record batches
  X->>S: fn_cdc_get_min_lsn(ci)  (cleanup during the read? then DataLossError)
  X-->>E: batches cast to the Spark schema
  E->>D: foreachBatch(df, batch_id)
  D->>D: append (txnAppId, txnVersion=batch_id, userMetadata=facts)
  Note over E: commit log written
  E->>F: after the query (AvailableNow) or on progress
  F->>F: MERGE finalized_until = GREATEST(old, trunc(end.commit_ts))
```

## Offsets and the checkpoint

```json
{"lsn": "0x0000002A000001F00003", "commit_ts": "2026-09-28T14:03:12.117"}
```

* `lsn`: last processed commit LSN. The initial offset for `startingLsn=earliest` is
  `fn_cdc_decrement_lsn(min_lsn)`, so the first read starts at `min_lsn`.
* `commit_ts`: commit time of `lsn` in UTC. It is stored even when the batch has no
  rows, because `end` can be an idle "dummy" entry. This is what lets finalization
  advance on quiet tables.
* Replays: Spark re-runs an uncommitted batch with the same `(start, end)`. `read()` is
  deterministic for a range as long as CDC cleanup has not purged it; if it has, the
  guard fails the query.

## Output schema

Metadata columns `_capture_instance, _start_lsn, _seqval, _operation, _command_id,
_commit_ts`, then the captured columns. The ordering key for applying changes is
`(_start_lsn, _command_id, _seqval, _operation)`.

Captured columns come from the `columns` option (DDL) or, when it is omitted, from CDC
metadata at `load()` time on the driver: `sys.sp_cdc_get_captured_columns`, sorted by
`column_ordinal` (`SqlCdcClient.captured_columns`). It needs only the permissions of the
CDC query functions. Default type mapping:

| SQL Server | Spark |
|---|---|
| `bit` | `BOOLEAN` |
| `tinyint`, `smallint` | `SMALLINT` |
| `int` / `bigint` | `INT` / `BIGINT` |
| `real` / `float` | `FLOAT` / `DOUBLE` |
| `decimal(p,s)`, `numeric(p,s)` | `DECIMAL(p,s)` |
| `money` / `smallmoney` | `DECIMAL(19,4)` / `DECIMAL(10,4)` |
| `date` | `DATE` |
| `datetime`, `datetime2`, `smalldatetime` | `TIMESTAMP_NTZ` |
| `datetimeoffset` | `TIMESTAMP` |
| `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`, `xml`, `uniqueidentifier`, `time` | `STRING` |
| `binary`, `varbinary`, `image`, `rowversion` | `BINARY` |

Alias types map through their base type. Anything else (`sql_variant`, `geography`,
`geometry`, `hierarchyid`) fails at `load()` with a message to pass `columns`. The fake
backend has no type metadata and always needs `columns`.

## Tables written by the sink and finalization

| Table | Grain | Written by | Notes |
|---|---|---|---|
| bronze (e.g. `bronze_orders`) | one row per change | `delta_sink` | append-only; `_batch_id` added; commit `userMetadata` holds the batch facts |
| facts (optional) | one row per non-empty batch | `delta_sink` | durable copy of the facts (Delta checkpoints drop `commitInfo`), plus `started_at`/`duration_ms` (source read + target write), `written_at`, and optional network and read metrics (`source_rtt_ms`, `read_seconds`, `read_mb`, `network_wait_ms`; [ADR 0014](decisions/0014-network-and-read-metrics-in-facts.md)) |
| `table_finalization` | one row per target table | `finalization.advance` | `finalized_until`, `end_lsn`, `end_commit_ts`, `updated_at` |

All three are created on first use with `DeltaTable.createIfNotExists`: explicit types, and a
comment on the table and on every control, facts and bronze metadata column
(`DESCRIBE TABLE` shows them), and stamped with the table property
`mssql_cdc.schema_version`. Existing tables get the schema migrations of their kind that
they have not had yet ([ADR 0012](decisions/0012-delta-tables-through-the-deltatable-api.md),
[ADR 0013](decisions/0013-schema-migrations-per-table-kind.md)).

## Extension points

* **New backend**: subclass `client.Backend` (`batches`, optionally `scalar`) and add
  it to `make_client`.
* **Other sinks**: the source is sink-agnostic; any `writeStream` target works.
  Idempotency and facts are then the sink's job.
* **Continuous mode**: call `finalization.advance` from a `StreamingQueryListener`
  (roadmap), or from a separate job reading the checkpoint's committed offsets.
