# Output schema

The columns of the stream, `spark.readStream.format("mssql_cdc")`, and of the snapshot,
`spark.read.format("mssql_cdc_snapshot")`. Both have the same schema: six metadata columns,
then the captured columns. What the sink adds on top, and the other tables, are in
[Tables](tables.md).

```python
from mssql_cdc import register

register(spark)  # stream() does this for you
changes = spark.readStream.format("mssql_cdc").options(**options).load()
current = spark.read.format("mssql_cdc_snapshot").options(**options).load()
changes.printSchema()
```

## Metadata columns

| Column | Type | Change rows | Snapshot rows |
|---|---|---|---|
| `_capture_instance` | STRING | the capture instance the row was read from: from a newer instance's start LSN on, that one ([Schema changes](../guides/schema-changes.md)) | the instance the snapshot was taken for |
| `_start_lsn` | STRING | `__$start_lsn`, the commit LSN of the source transaction, as `0x` + 20 uppercase hex. All rows of a transaction share it; string order is commit order | the snapshot's LSN, recorded before the table was read |
| `_seqval` | STRING | `__$seqval`, the change's position in the log, same format | NULL |
| `_operation` | INT | 1 delete, 2 insert, 3 update (the row before), 4 update (the row after) | 0 |
| `_command_id` | INT | `__$command_id`, the order of the statement within its transaction, numbered per capture instance. Absent with `includeCommandId=false` | NULL |
| `_commit_ts` | TIMESTAMP_NTZ | the commit time from `cdc.lsn_time_mapping`, converted to UTC ([sourceTimeZone](options.md#sourcetimezone)) | the commit time of `_start_lsn` |

An update is two rows, operation 3 and operation 4.

## Ordering changes

Order rows with `(_start_lsn, _command_id, _seqval, _operation)`, for example in bronze:

```python
history = spark.read.table("bronze.orders").orderBy(
    "_start_lsn", "_command_id", "_seqval", "_operation"
)
```

After a switch to a newer capture instance, the two instances number the same change
differently in `_command_id`, but all the rows of one commit come from one instance, so the
order holds. Across instances, `(_start_lsn, _seqval, _operation)` identifies a change.
Snapshot rows share one `_start_lsn`, below every change read after them, so a MERGE that
keeps the latest image per key absorbs the overlap between a snapshot and the stream
([Silver](../guides/silver.md) does that for you).

## Captured columns

Without the [columns](options.md#columns) option, the driver infers them at `load()` from
`sys.sp_cdc_get_captured_columns`, in capture order. When the table has two capture instances
it takes the union by name, older instance first; a column both capture with different types
takes the newer type when it holds the older's values, otherwise `load()` fails with
`SchemaChangedError`. A range of an instance that lacks a column reads it as NULL.

Default types, from `mssql_cdc.client`:

| SQL Server | Spark |
|---|---|
| `bit` | BOOLEAN |
| `tinyint`, `smallint` | SMALLINT (0 to 255 does not fit Spark's signed TINYINT) |
| `int` | INT |
| `bigint` | BIGINT |
| `real` | FLOAT |
| `float` | DOUBLE |
| `decimal(p,s)`, `numeric(p,s)` | DECIMAL(p,s) |
| `money` | DECIMAL(19,4) |
| `smallmoney` | DECIMAL(10,4) |
| `date` | DATE |
| `datetime`, `datetime2`, `smalldatetime` | TIMESTAMP_NTZ |
| `datetimeoffset` | TIMESTAMP |
| `time` | STRING |
| `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`, `xml`, `uniqueidentifier` | STRING |
| `binary`, `varbinary`, `image`, `timestamp` (rowversion) | BINARY |

Alias types, such as `sysname`, map through their base type. Any other type (`sql_variant`,
`geography`, `geometry`, `hierarchyid`) fails at `load()` with
`no default Spark type for SQL Server type ...`: pass `columns`, leaving that column out.
Only `_commit_ts` is converted to UTC; captured `datetime` and `datetime2` values arrive as
stored. Every batch is cast to this schema before Spark sees it.

With `columns`, the declared list and types are the schema, for example
`order_id INT, status STRING, amount DECIMAL(18,2)`; the read casts to them.

## Snapshot rows

The snapshot reads the source table itself, under READ COMMITTED (never `NOLOCK`), in the
same schema. A captured column the source table no longer has (dropped, matched by
`column_id`) reads NULL, as its change rows do since the drop. Written by `snapshot()` or
`to_delta`, snapshot rows also get `_batch_id` NULL in bronze, and `_snapshot`, the LSN of
the snapshot they belong to ([ADR 0016](../decisions/0016-bootstrap-snapshot-at-a-recorded-lsn.md)).

With [snapshotChunks](options.md#snapshotchunks) the snapshot reads only those chunks and has
one more column, `_chunk INT`, the chunk each row was read in. `backfill()` writes them to
bronze with `_snapshot` set to the chunked snapshot's LSN S, while each wave's `_start_lsn` is
its own stamp, at or after S ([ADR 0028](../decisions/0028-chunked-snapshot-next-to-the-stream.md)).
