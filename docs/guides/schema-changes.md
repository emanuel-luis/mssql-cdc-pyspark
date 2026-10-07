# Schema changes

SQL Server CDC does not follow DDL by itself: a capture instance keeps the column list it was
enabled with, and a table can have at most two. This page covers what the stream does when
the source table changes, how to start capturing a new column, and what to do when a
column's type changes. The why behind all of it is
[ADR 0023](../decisions/0023-schema-changes-and-capture-instance-switching.md).

## The smallest setup

There is nothing to turn on. Each time the stream plans a micro-batch it asks SQL Server for
the table's capture instances and for the DDL recorded on the table inside that batch
(`sys.sp_cdc_get_ddl_history`). Give it a facts table so that what it finds is recorded:

```python
from mssql_cdc import stream

query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/Volumes/cat/sch/vol/ckpt/orders",
    facts_table="ops.ingestion_facts",
)
```

Each DDL statement and each move to a newer capture instance becomes a facts row of the
batch that read past it:

```sql
SELECT app_id, batch_id, event, detail, min_lsn, written_at
FROM ops.ingestion_facts
WHERE event IN ('schema_change', 'capture_instance_switched')
ORDER BY written_at DESC;
```

These rows travel through the metrics directory, which `to_delta` sets up by itself for a
local or Volume checkpoint. With a URI checkpoint (`abfss://`, `dbfs:/`) set the
[metricsPath](../reference/options.md#metricspath) option, or the events only show up as
warnings in the driver log.

Two things stay manual: a DBA creates and drops capture instances, and the owner of the
bronze table enables Delta type widening. The library does neither.

## What the stream does with each kind of DDL

| On the source table | What CDC does | What the stream does |
|---|---|---|
| Add a column | Nothing: the capture instance keeps its columns, and an update of only the new column writes no change row | Goes on, with a warning and a `schema_change` row. The column arrives with a [new capture instance](#capturing-a-new-column) |
| Drop a captured column | Keeps it captured, NULL from then on | Goes on, with a `schema_change` row. See [Dropping a column](#dropping-a-column) |
| Change a type to one the query's type still holds (`varchar(10)` to `varchar(50)`) | Converts the change table | Goes on, with a `schema_change` row |
| Change a type to one it does not hold (`decimal(9,2)` to `decimal(18,4)`, `int` to `bigint`) | Converts the change table | Fails the batch before reading any of it, with `SchemaChangedError`. See [Changing a column type](#changing-a-column-type) |
| Other DDL, such as a NOT NULL change | Records it | Goes on, with a `schema_change` row |
| Rename a captured column, TRUNCATE, alter the key column | Refuses them while CDC is on | Nothing to do |

The [schemaChangePolicy](../reference/options.md#schemachangepolicy) option sets the reaction.
`classify`, the default, does what the table says. `fail` fails the batch on any DDL; since
the replayed batch holds the same DDL, restart once with `classify` to go past it.

## Capturing a new column

A new column needs a new capture instance with the new column list. The stream then moves to
it on its own: there is no option to change and nothing to restart unless the running
query's schema lacks the new column.

### How the stream follows a newer capture instance

The newer instance starts at S, its `start_lsn`, the commit LSN of its enable. From S on
every commit is in both instances. Each batch reads the older instance below S and the newer
one from S, both in one batch if it crosses S, so every commit is read once and offsets and
checkpoints do not change ([Architecture](../ARCHITECTURE.md#a-second-capture-instance)).

* At `load()`, the inferred schema is the union of both instances' columns.
* A query already running when the new instance appears switches in place if its schema
  holds every column the new instance captures. Otherwise it stops at S with
  `SchemaChangedError`, before reading past it; restarted, it infers the new columns and
  resumes there.
* With the [columns](../reference/options.md#columns) option the declared list decides: the
  query switches in place and logs a warning naming captured columns it leaves out, which
  the batch's facts row keeps too ([Monitoring](monitoring.md#warnings)).
* A column the query reads that the new instance does not capture reads NULL from S. The
  warning and the event's `detail` name it.
* Bronze gains the new column (every append uses `mergeSchema`); older rows read NULL for it.
  `_capture_instance` on each row says which instance it came from. A name Delta takes only
  with column mapping needs one step of yours first
  ([below](#a-column-name-that-needs-column-mapping)).

### The procedure

The DBA runs it in the source database as `db_owner`:

1. Enable the new instance with the new column list: `sys.sp_cdc_enable_table` with a new
   `@capture_instance`.
2. Grant the reader `SELECT` on the new change table, `cdc.[<new instance>_CT]`. Each
   capture instance has its own change table; without the grant the first batch that
   reaches it fails with a `PermissionError` naming it ([Permissions](permissions.md)).
3. Check that `sys.sp_cdc_help_change_data_capture` lists both instances. The new one's
   `sys.fn_cdc_get_min_lsn` stays `0x00...` until capture has processed the enable (seconds;
   up to about 5 minutes on a quiet database).
4. Wait until every stream that reads the table has a `capture_instance_switched` facts row,
   and one batch more: Spark commits the batch after its facts row, and a replay would read
   the old instance again. A stream already at or past S when it started (for example a
   bootstrap after the enable) never reads the old instance, writes no event and needs no
   wait.
5. Disable the old instance.

For step 4, a stream is done when it has a row with a larger `batch_id` than its switch:

```sql
SELECT s.app_id, s.batch_id AS switched_in, MAX(f.batch_id) AS last_batch
FROM ops.ingestion_facts s
JOIN ops.ingestion_facts f ON f.app_id = s.app_id
WHERE s.event = 'capture_instance_switched' AND s.target = 'bronze.orders'
GROUP BY s.app_id, s.batch_id;
-- ready when last_batch > switched_in for every stream of the table
```

A stream missing from the result has not reached S yet, unless it started past it.

??? example "The full procedure, `sql/switch_capture_instance.sql`"

    ```sql
    --8<-- "sql/switch_capture_instance.sql"
    ```

### Keep the capture instance name

Leave [captureInstance](../reference/options.md#captureinstance) at the old name when it is
SQL Server's default, `<schema>_<table>`: the stream follows the table's newest instance,
also after the old one is disabled. With any other name, set it to the new one once the old
one is gone, since a disabled custom name no longer leads to its table.

Renaming has a cost. SQL Server no longer lists the old name, so rows stamped with it, the
bootstrap snapshot included, no longer count as this table's: a rerun with `bootstrap=True`
takes one more snapshot, and a silver table built from scratch fails on the old rows.

### Rows unchanged since the switch

The latest image of a row that has not changed since S has NULL in a column only the new
instance captures. To fill it, ask for a snapshot right after the switch:

```python
query = stream(spark, options).to_delta(
    "bronze.orders",
    app_id="orders-v1",
    checkpoint="/Volumes/cat/sch/vol/ckpt/orders",
    facts_table="ops.ingestion_facts",
    snapshot_on_switch=True,
)
```

After the batch that first reads the new instance, the sink appends a snapshot of the table,
so every row's latest image carries the new column. It reads the whole table: not for
[tables too big to snapshot](bootstrap.md). With a URI checkpoint it needs `metricsPath`, and
`to_delta` raises a `ValueError` without it. A crash right after the switch batch takes the
snapshot again on the replay: harmless, only slower.

### A column name that needs column mapping

SQL Server allows a space or one of `,;{}()=` in a bracketed column name, such as
`[Unit Price]`; Delta takes such a name only on a table with column mapping. A bronze or
silver table created with one has it
([Table properties](../reference/tables.md#table-properties)). When a newer capture
instance adds one to a table created without it, the stream fails before writing the
batch, and `apply_changes` before changing silver:

```text
SchemaChangedError: bronze.orders: column mapping is off on this table, and Delta takes
the new column(s) 'Unit Price' only with it. Enable it, then run again: ALTER TABLE
bronze.orders SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'). That upgrades the
table's Delta protocol: every reader and writer of the table then needs a Delta that
supports column mapping.
```

The library never enables it itself, because the upgrade locks out every reader and writer
of the table on a Delta without column mapping. Check them, run the statement on bronze,
restart the query, and run it on silver before the next `apply_changes`:

```sql
ALTER TABLE bronze.orders SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);
```

## Changing a column type

A type change goes on as a `schema_change` event when the type the query reads the column as
still holds every value of the new type. That is the case when both map to the same Spark
type, or when the new type widens to the query's along one of these steps:

* a smaller integer to a larger one: SMALLINT, INT, BIGINT;
* SMALLINT or INT to DOUBLE;
* an integer to DECIMAL(p,s) with at least 5 (SMALLINT), 10 (INT) or 20 (BIGINT) digits
  before the point;
* DECIMAL(p,s) to a DECIMAL with at least as many digits before the point and after it;
* FLOAT to DOUBLE, DATE to TIMESTAMP_NTZ.

So a `varchar` length change, or a type made narrower, goes on. A type made wider on the
source (`int` to `bigint`, `decimal(9,2)` to `decimal(18,4)`) fails the batch that holds the
DDL while it is planned, before any of it is read:

```text
SchemaChangedError: dbo_orders: the type of captured column(s) amount DECIMAL(18,4)
(read as decimal(9,2)) changed inside the batch (...). Restart the query to re-infer the
schema (and enable delta.enableTypeWidening on bronze for a widening).
```

To go on:

1. Enable type widening on bronze. The library never changes a table's properties:

    ```sql
    ALTER TABLE bronze.orders SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
    ```

2. Restart the query. It infers the new type, replays the batch, and the append widens the
   bronze column.

3. On silver, the property alone is not enough. `apply_changes` adds the columns bronze
   gains but never changes a column's type: its MERGE runs without schema evolution, so
   silver keeps the old type. Widen silver's column yourself, with Delta's own statement,
   before the next call:

    ```sql
    ALTER TABLE silver.orders SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
    ALTER TABLE silver.orders ALTER COLUMN amount TYPE DECIMAL(18,4);
    ```

Without the property, or for a change Delta cannot widen (a type that is not a widening, such
as `int` to `varchar`), the append fails with a `SchemaChangedError` that says so: write to a
new bronze table, or rewrite this one.

The same rule applies to a newer capture instance: if it captures a column with a type the
query's does not hold, the query stops at S and the restart infers it. Two instances that
type one column in incompatible ways fail `load()` itself; pass `columns` with a type that
holds both, or disable the older instance once the stream has read past the newer one's start.

### With the columns option

With [columns](../reference/options.md#columns), the check compares against the types SQL
Server reported when the query started, since the declared ones are your own conversions. A
type change made while the query runs is caught at planning, as above. One made while it was
stopped is not: the read fails casting to the declared type. Update `columns` and restart.

## Dropping a column

CDC keeps a dropped column captured and writes NULL in it from then on, so bronze keeps the
column and its new rows read NULL. A snapshot (`bootstrap=True`, a re-snapshot,
`snapshot_on_switch`) reads NULL for it too, matched by `column_id`: a column dropped and
added back under the same name is another column. To stop carrying it, capture the table
again with a new instance without it; the switch event's `detail` then lists it under "no
longer captured, read as NULL".

## Errors

| Error | Cause | What to do |
|---|---|---|
| `SchemaChangedError: ... the type of captured column(s) ... changed inside the batch` | a type change the query's type does not hold | [Changing a column type](#changing-a-column-type) |
| `SchemaChangedError: ... DDL at ... inside the batch, and schemaChangePolicy=fail` | any DDL, with `schemaChangePolicy=fail` | restart once with `classify` |
| `SchemaChangedError: The stream reaches capture instance ...` | the newer instance captures columns or types the query lacks | restart: it infers them and resumes at S |
| `SchemaChangedError: Capture instances ... capture ... as ... and ...` | two instances type one column incompatibly | pass `columns`, or disable the older instance once read past |
| `SchemaChangedError: ...: the type of a column changed on the source and the table cannot take the new one` | bronze cannot take the new type | enable `delta.enableTypeWidening` and restart, or use a new table |
| `SchemaChangedError: ...: column mapping is off on this table, and Delta takes the new column(s) ... only with it` | a new column's name needs column mapping | [enable it](#a-column-name-that-needs-column-mapping) and run again |
| `PermissionError: The login cannot read the change table ...` | no grant on the new change table | `GRANT SELECT ON cdc.[<new instance>_CT] TO <reader>` |
| `DataLossError: ... held only by capture instance ..., disabled before the stream read them` | the old instance was disabled too early | [Data loss](data-loss.md): `on_data_loss="resnapshot"` recovers with a snapshot |
| `ValueError: No capture instance of the table (...) captures ...` | `columns` lists a column no instance captures | fix `columns`, or capture it with a new instance |

## Pitfalls

* **Disabling the old instance too early.** The changes below S that only it held are gone;
  the stream fails with `DataLossError`, which names CDC cleanup and the disabled instance as
  the possible causes because it cannot tell them apart.
* Changes to a new column made before the new instance exists are lost: an update that
  touches only that column writes no change row. Enable the new instance soon after the
  ALTER, or snapshot afterwards (`snapshot_on_switch=True`).
* Until the old instance is disabled, both capture every change: twice the change-table
  writes.
* A URI checkpoint without `metricsPath` writes no events to the facts; step 4 then relies on
  the warning in the stream's log.
* Facts consumers that look for snapshots must filter `event IN ('bootstrap', 'resnapshot')`,
  not `event IS NOT NULL`: event rows of schema changes are no snapshots
  ([Tables](../reference/tables.md#facts)).
* None of this has run on Databricks yet; see [Databricks](../DATABRICKS.md) for the
  differences there.

## See also

* [Options](../reference/options.md): `schemaChangePolicy`, `columns`, `metricsPath`,
  `snapshot_on_switch`.
* [Tables](../reference/tables.md#facts): the `event` and `detail` columns.
* [Permissions](permissions.md), [Bootstrap](bootstrap.md), [Data loss](data-loss.md),
  [Silver](silver.md).
* [ADR 0023](../decisions/0023-schema-changes-and-capture-instance-switching.md): the
  measurements on SQL Server 2022 and 2017 behind these rules. Lab check t9 (`LAB.md`) runs
  the procedure above under a continuous writer on both.
