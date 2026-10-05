# Permissions

The reader only reads. A `db_owner` needs nothing else; a least-privilege login needs what
the CDC query functions need, plus one grant per change table. This page lists those
grants, what each one is for and the errors that name a missing one.

## Smallest example

For the table `dbo.orders` and its capture instance `dbo_orders`, in the source database:

```sql
-- a database user for an existing login
CREATE USER cdc_reader FOR LOGIN cdc_reader;

GRANT SELECT ON dbo.orders TO cdc_reader;           -- the captured source columns
GRANT SELECT ON cdc.[dbo_orders_CT] TO cdc_reader;  -- the change table

-- only if the capture instance was enabled with @role_name (a gating role):
ALTER ROLE cdc_gate ADD MEMBER cdc_reader;
```

Then connect as that login:

```python
options = {
    "connectionString": "Server=host,1433;Database=sales;UID=cdc_reader;PWD=...;Encrypt=yes",
    "captureInstance": "dbo_orders",
}
```

Read the password from your platform's secret store rather than writing it in code, and put
it in braces with every `}` doubled, so that a `;` in it cannot end the value. Spark does
not redact the `connectionString` option by default: where options can surface (a table
defined with `OPTIONS (...)`, a plan in the UI or a log), set
`spark.sql.redaction.options.regex` to `(?i)url|connectionstring`. Where the server takes
Entra ID logins, a managed identity needs no password at all
([connectionString](../reference/options.md#connectionstring)).

## What each grant is for

| Grant | Used for |
|---|---|
| `SELECT` on the source table | What SQL Server checks before the CDC metadata procedures and functions answer: `sys.sp_cdc_help_change_data_capture` (the table, its key and its capture instances), `sys.sp_cdc_get_captured_columns` (the inferred schema), `sys.sp_cdc_get_ddl_history` (schema changes), `sys.fn_cdc_get_min_lsn` (the retention guard). The snapshot for `bootstrap=True`, a re-snapshot and `snapshot_on_switch` reads the table itself; so do `backfill()`, which plans a chunked snapshot from the key alone (MIN, MAX, row counts per slice of an integer key in one `GROUP BY`, `TOP (n + 1)` seeks for other keys) and reads it in key ranges, and `reconcile()`, which counts the rows per key range in one scan and reads the ranges it compares. `SELECT` on the key and captured columns alone is enough. |
| Membership in the gating role | Required by the same procedures and functions when the capture instance has one. |
| `SELECT` on `cdc.[<capture instance>_CT]` | The changes. The reader queries the change table directly, because `cdc.fn_cdc_get_all_changes_<ci>` does not return `__$command_id` on SQL Server 2022 ([ADR 0009](../decisions/0009-read-change-tables-directly.md)). |

Everything else the reader touches needs no grant: `cdc.lsn_time_mapping`,
`sys.fn_cdc_get_max_lsn`, `sys.fn_cdc_increment_lsn`, `sys.fn_cdc_map_lsn_to_time`, the
server's time zone, its own session's `ASYNC_NETWORK_IO` wait in
`sys.dm_exec_session_wait_stats`, which a session may read without `VIEW SERVER STATE`,
`sys.sp_spaceused` (the row estimate that sizes the first count of a chunked snapshot's integer key), and
`sys.columns` for the collation of a string key, which shows the columns of a table the login
can `SELECT`. The integration tests run the stream, the bootstrap, chunked snapshots and
`reconcile` against SQL Server 2022 with a login that has only the two `SELECT` grants (no
gating role).

`backfill(isolation="snapshot")` needs no grant either, but the database must allow it: a
DBA runs `ALTER DATABASE <db> SET ALLOW_SNAPSHOT_ISOLATION ON`, and SQL Server refuses the
read until then.

## What the reader never needs

- Write access to the source database. The library never creates or drops capture
  instances, and never writes heartbeats: the optional
  [`sql/heartbeat.sql`](https://github.com/emanuel-luis/mssql-cdc-pyspark/blob/main/sql/heartbeat.sql)
  is a DBA's script and runs as a SQL Server Agent job.
- `db_owner`, `VIEW SERVER STATE` or access to `msdb`. The retention period lives in `msdb`
  and is never read: the [retention headroom](monitoring.md) is measured from
  `sys.fn_cdc_get_min_lsn`, which the reader already sees.
- Any other table in the `cdc` schema (`cdc.change_tables`, `cdc.captured_columns`,
  `cdc.ddl_history`...): the documented procedures above stand in for them.

## A new capture instance needs its own grant

Each capture instance has its own change table. When a DBA enables a second instance of
the table, for example to capture a new column, grant `SELECT` on its change table before
the stream reaches it:

```sql
GRANT SELECT ON cdc.[dbo_orders_v2_CT] TO cdc_reader;
```

Without it, the first batch that reaches the new instance fails with a `PermissionError`
that names the grant. The whole switch procedure is in
[Schema changes](schema-changes.md).

## Errors that point at permissions

- `PermissionError: The login cannot read the change table cdc.[dbo_orders_CT]...`: the
  change-table grant is missing. The message has the exact `GRANT` to run.
- `ValueError: Capture instance 'dbo_orders' not found, or the login lacks SELECT on its
  source columns (or membership in its gating role)...`: the name is wrong, or the
  source-table grant or the role membership is missing. Without them,
  `sys.sp_cdc_help_change_data_capture` lists nothing for the login.
- `ValueError: Capture instance 'dbo_orders' not found, the login lacks permission to read
  it, or capture has not processed its creation yet...`: `sys.fn_cdc_get_min_lsn` returned
  `0x00...`. Besides a missing grant, this happens right after `sys.sp_cdc_enable_table`,
  until the capture job has run (up to about 5 minutes on a quiet database): retry then.

## Pitfalls

- The change-table grant sidesteps the gating role. The query functions check the
  role; a direct `SELECT` on the change table does not. The data exposed is the same, but
  if you relied on the role to control access, the grant now does.
- The lakehouse side has permissions too. The Spark job creates bronze, the facts, the
  control and silver tables on first use, and later runs schema migrations on them, which
  set a table property and column comments with `ALTER TABLE`
  ([Tables](../reference/tables.md)). Its identity needs to create and alter those tables,
  and to write the checkpoint and the metrics directory on every node.

## See also

- [Schema changes](schema-changes.md): the capture instance switch, step by step.
- [ADR 0009](../decisions/0009-read-change-tables-directly.md): why the change table is read
  directly, and the trade-off with the gating role.
- [Running on Databricks](../DATABRICKS.md): network access from every worker.
