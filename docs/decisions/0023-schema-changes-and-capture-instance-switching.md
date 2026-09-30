# 0023: Schema changes on the source, and switching to a newer capture instance

**Status:** accepted  
**Date:** 2026-09-30T18:40:00-03:00

## Context
A capture instance captures a fixed column list, chosen when it is enabled. To capture a
column added to the table, SQL Server's way is a second capture instance of the same table
(at most two), then disabling the first. Until now the library read one capture instance
with the schema of `load()` and knew nothing about DDL. Measured on SQL Server 2022 CU27
and 2017 CU31 in Docker, library at `03a0f3a`, with a least-privilege login (SELECT on the
source table and on `cdc.<ci>_CT` only):

* **ADD COLUMN**: the capture instance does not change (captured columns and change table
  stay); `cdc.ddl_history` gets a row with `required_column_update = 0`; an UPDATE that
  touches only the new column writes no change row. The stream kept going and the new
  column's changes were lost until a new instance existed.
* **DROP COLUMN** of a captured column: `sp_cdc_get_captured_columns` still lists it and the
  change table keeps it; rows captured after the drop have NULL there (before-images too); a
  commit made just before the DROP but captured after it kept its value; a `ddl_history` row
  (0). The stream continued and bronze got NULLs, but `snapshot()`, `bootstrap=True` and
  `on_data_loss="resnapshot"` failed with `Invalid column name`: the snapshot selected the
  captured columns from the source table.
* **ALTER COLUMN** type (`decimal(9,2)` to `(18,4)`, `varchar(10)` to `(50)`, `int` to
  `bigint`): `sp_cdc_get_captured_columns` reports the new type, the change-table column is
  altered and its rows converted, and the `ddl_history` row has `required_column_update = 1`.
  A running query kept its load-time schema: it wrote the values that fit and failed on the
  first that did not (pyarrow `ArrowInvalid: Rescaling Decimal value would cause data loss`).
  On restart the schema was re-inferred and the Delta append failed with
  `DELTA_FAILED_TO_MERGE_FIELDS`; with `delta.enableTypeWidening = true` on bronze plus
  schema merging, Delta widened the column and wrote.
* **Other DDL**: a NOT NULL change writes a `ddl_history` row (0). Renaming a captured column
  is refused (`REPLICATED`). Renaming an uncaptured column or the table is allowed and not
  recorded; after a table rename `sp_cdc_help_change_data_capture` shows the new name under
  the old instance. TRUNCATE and altering the key column are refused. Adding back a dropped
  column's name with another type makes a new `column_id`: captured and source columns must
  be compared by `column_id`, not by name.
* **Visibility**: capture writes `ddl_history` asynchronously, but over 2,746 samples and 8
  DDLs there was no case where `max_lsn >= ddl_lsn` and the row was not yet visible through
  `sp_cdc_get_ddl_history` to the least-privilege login: a batch that ends at or below
  `max_lsn` sees its DDLs. `ddl_time` comes in whole minutes.
* **Permissions**: with SELECT on the source table the login can call
  `sys.sp_cdc_get_ddl_history`, `sp_cdc_get_captured_columns`,
  `sp_cdc_help_change_data_capture` (which lists both instances of the table),
  `fn_cdc_get_min_lsn` and `fn_cdc_get_column_ordinal`; it cannot read `cdc.ddl_history`,
  `cdc.change_tables`, `cdc.captured_columns` or `cdc.index_columns`. With only the
  change-table grant the procedures say "Object does not exist or access is denied", the help
  procedure returns nothing and `min_lsn` is `0x00`. A new instance's change table needs its
  own `GRANT SELECT`.
* **A second capture instance**: its `start_lsn` S is the commit LSN of the enable (in
  `cdc.lsn_time_mapping`). Right after the enable `max_lsn` is below S and
  `fn_cdc_get_min_lsn(v2)` is `0x00` until capture processes the enable (2.9 s under load; up
  to ~5 minutes on a quiet database, ADR 0016). Under 50 inserts/s, of 685 commits, 259 were
  only in v1 (all with LSN < S), 426 in both (all >= S, with identical `(start_lsn, seqval)`),
  none only in v2 and none in neither. So reading v1 below S and v2 from S reads every commit
  once. The enable waits while a transaction that already wrote the table is open (3.4 s;
  that commit is only in v1, below S); a transaction that began earlier but writes only after
  the enable does not block it and is complete in both. `__$command_id` differs between the
  instances for the same change (v1 1, 3, 3, 5; v2 2, 4, 4, 6), each keeping the order:
  `(start_lsn, seqval, operation)` identifies a change across instances, `command_id` does
  not. A third instance is refused. `sp_cdc_disable_table` on v1 takes effect at once: its
  change table is gone, `min_lsn` is `0x00`, its `ddl_history` rows are deleted and readers
  get `Invalid object name`. DDL made before v2 existed is not in v2's history. Documented
  only: cleanup moves `start_lsn` of every instance up to its new low watermark, so two
  instances can share one; Debezium breaks that tie by `create_date`.
* **The library before this change**: Spark accepted a changed source schema on restart with
  the same checkpoint (`foreachBatch`). Switching `captureInstance` to v2 before the stream
  had read v1 up to S raised `DataLossError` (right), but blamed CDC cleanup; after catching
  up on v1 the switch worked, since offsets are database-wide LSNs. v2's new column failed
  the bronze append with `DELTA_METADATA_MISMATCH` (no `mergeSchema`), and a column v2 lacked
  was filled with NULL by Delta. A rerun with `bootstrap=True` after the switch wrote a second
  full snapshot: the lookup filtered `_capture_instance = ci`, as did the ADR 0018 recovery.
  Once v1 was dropped, a stream still configured for it failed at `load()` with "not found",
  not naming v2.
* **Others**: Debezium's SQL Server connector leaves the new instance to the DBA, cuts the
  old one at the new one's `start_lsn` (the same rule), emits a notification, and drops
  nothing; it reads `cdc.change_tables`, which invariant 11 rules out here. Estuary switches
  to the newest instance with `start_lsn <=` its position, and can manage instances itself
  with `db_owner`. Lakeflow Connect uses a DDL trigger (`db_owner`). Fivetran reads one
  instance. Fabric mirroring fails on DDL and reseeds. AWS DMS reads the log instead.

## Decision
The offset contract does not change (invariant 1): every LSN is database-wide, so which
capture instance holds a range follows from the instances' `start_lsn`, computed again each
time a batch is planned, replays included.

### D1. Detect DDL on the driver, every batch, and react by kind
* When planning a batch, the reader asks `sys.sp_cdc_get_ddl_history(<ci>)` of each instance
  the batch reads for entries with `ddl_lsn` in `(start, end]` (`CdcClient.ddl_history`), and
  compares the captured types (`sp_cdc_get_captured_columns`) with the query's schema, by
  `column_id`, not by name alone.
* A type change of a captured column (`required_column_update = 1`, or a captured type the
  query's cannot hold) fails the planning of the batch that contains it, before any row is
  read, with `SchemaChangedError` (a `RuntimeError`, exported): "restart the query to
  re-infer the schema (and enable `delta.enableTypeWidening` on bronze for a widening)".
* ADD and DROP of columns and any other DDL: the stream continues, logs a warning, and
  leaves a `schema_change` event for the facts (below).
* Option `schemaChangePolicy`: `classify` (the default, as above) or `fail` (any DDL in the
  batch fails it).
* Considered:
  - Diffing `sp_cdc_get_captured_columns` each batch alone: it sees type changes only; an ADD
    leaves the captured list as it was and a DROP stays listed.
  - Reading `cdc.ddl_history` or a DDL trigger on the source: the first needs a grant beyond
    invariant 11, the second `db_owner` and writes to the source (Lakeflow Connect).
  - Failing on every DDL (Fabric mirroring): a NOT NULL change, or a column nobody captures,
    would stop ingestion for nothing. Ignoring DDL, as before: the type change failed
    mid-batch in an executor with an Arrow error, after some rows were written.
  - `sp_cdc_get_ddl_history` is documented, readable by the least-privilege login, and was
    never behind `max_lsn` in the measurements.

### D2. Follow a newer capture instance of the same table
* The reader lists the table's instances with `sys.sp_cdc_help_change_data_capture
  @source_schema, @source_name` (`CdcClient.capture_instances`, oldest first); newer means a
  later `create_date` (a tie on `start_lsn` after cleanup is broken by it).
* `partitions()` cuts `[from, to]` at S, the newer instance's `start_lsn`: the older instance
  for `[from, S - 1]` (clamped to the range), the newer for `[S, to]`, never an empty range
  (invariant 3). Each `LsnRange` names its capture instance, so executors stay stateless
  (invariant 5), and the retention guard runs per instance: a range of the older instance
  checks the older's `min_lsn` (invariant 4).
* Schema at `load()`: the union by column name of the instances' captured columns, in capture
  order, older first. Two types for one name resolve to the newer's when it holds the older's
  values; otherwise `load()` fails with `SchemaChangedError`. A range selects typed NULL for a
  column its instance lacks.
* Ordering across instances is `(_start_lsn, _seqval, _operation)`: `_command_id` numbers
  each instance's rows its own way, so it breaks ties only within one `_start_lsn`, which the
  cut at S keeps within one instance. The existing key
  `(_start_lsn, _command_id, _seqval, _operation)` stays right for the same reason; the bronze
  column comment says so.
* A newer instance that appears while a query runs: the query switches in place when its
  schema holds every column the newer instance captures, and otherwise fails with
  `SchemaChangedError` at the boundary, before reading past S, so that the next `load()`
  infers the new columns.
* A configured instance that was dropped: `load()` and planning follow the newest instance of
  its table (found through its default name `<schema>_<table>`, or through the newest name
  the run has seen); otherwise the not-found error names the table's instances.
* `DataLossError` says when the gap comes from an instance that starts later (the older one,
  which held the range, was dropped before the stream read it) instead of blaming cleanup; a
  `PermissionError` on the newer change table names its own grant.
* The reader leaves a `capture_instance_switched` event (detail `old -> new`) when it plans
  the first batch that reads the newer instance: from then on the older one can be dropped.
* Considered:
  - The user changes `captureInstance` by hand (the state before): it needs the stream to have
    read exactly up to S first, or it fails, and every consumer does it at its own moment.
  - A new checkpoint or stream per instance: a second checkpoint (invariant 8 needs a new
    `app_id`) and a gap or an overlap to reconcile at S.
  - Estuary's rule, the newest instance with `start_lsn <=` the position, switches only at a
    batch boundary; cutting inside the batch lets one batch span the switch.

### D3. The DBA creates and drops instances; the library never does
`sql/switch_capture_instance.sql` documents the procedure: enable the new instance with the
new column list, `GRANT SELECT ON cdc.<new>_CT TO <reader>`, wait for every stream's
`capture_instance_switched` event, then disable the old one. Considered: letting the library
manage instances (Estuary): it needs `db_owner` and writes to the source, against invariant 11.

### D4. Bronze takes new columns; type widening is the owner's choice
Every bronze append, micro-batches and snapshots alike, uses `mergeSchema`: a column the
newer instance captures is added (older rows read NULL), and a column the batch lacks is
NULL. A type change fails the append unless the bronze table has
`delta.enableTypeWidening = true` and the change is a widening Delta supports; the library
documents the property and never sets it. Considered: `overwriteSchema` (a rewrite of the
table) and casting new types back to the old ones (silent truncation).

### D5. Rows unchanged since a switch keep NULL for a newly captured column
The latest image of a row that did not change after S has NULL for a column only the newer
instance captures. `to_delta(..., snapshot_on_switch=False)`: with `True`, after the batch
that first reads the newer instance (the `capture_instance_switched` event), the sink appends
a snapshot (ADR 0016's machinery, stamped with `max_lsn` recorded before its read), so the
latest image carries the new column's values. Considered: always snapshotting at a switch (a
full read, too slow for large tables, see the README on tables too big to snapshot) and
never (a column that stays NULL until each row changes).

### Needed whichever option
* The snapshot reads typed NULL for a captured column the source table no longer has,
  matched by `column_id` (`CdcClient.present_columns`), instead of failing.
* Existing snapshots are looked up by the set of capture instances of the same source table,
  case-insensitive (`pipeline._instances`): `snapshot()`, `bootstrap=True` and the ADR 0018
  recovery. The recovery's retention test uses the `min_lsn` of the table's oldest instance.
* Silver (ADR 0019) reads the rows of that set: with `options`, the table's instances come
  from SQL Server. A row in the range to apply of any other instance fails the call, naming
  it, instead of being skipped. A column bronze gained is added to silver.
* Facts events. The reader, which cannot write Delta, leaves one JSON file per event in the
  metrics directory (`metricsPath`), `event-<kind>-<lsn>.json` with `event`,
  `capture_instance`, `lsn`, `commit_ts` and `detail`. The sink writes each as a facts row of
  the batch, in the same Delta commit as the batch's own row (`txnAppId` `<app_id>#facts`,
  `txnVersion` the batch id), so a replay writes both or neither, and removes the files only
  after that commit; a file with the same kind and LSN is one event. The row has `event` set
  to the kind, the batch's `batch_id`, `rows = 0`, `min_lsn = max_lsn = end_lsn` the event's
  LSN, `end_commit_ts` its commit time, `lost_*` NULL, and the new column `detail` (facts
  migration 6), which also rewrites the comments of `batch_id`, `rows`, `event`, `end_lsn`
  and `end_commit_ts`. Without `metricsPath`, events are warning logs only.
  - Considered: a separate `txnAppId` per event kind (a second event of one kind in a later
    batch needs a new version anyway) and writing from the driver (the reader has no Spark
    session, and the rows would land before the batch's data).
* Bronze migration 1 gives existing tables the new comments of `_capture_instance` (the
  instance each row actually came from) and `_command_id` (numbered per instance).

## Consequences
* A facts consumer that took `event IS NOT NULL` for "a snapshot" (as ADR 0018 and the README
  suggested for the rebuild point) must now filter `event IN ('bootstrap', 'resnapshot')`:
  `schema_change` and `capture_instance_switched` rows carry `rows = 0` and a `batch_id`.
  Statistics over micro-batches still filter `event IS NULL`. Silver and the tests' helpers
  do.
* A type change needs a restart; a widening also needs `delta.enableTypeWidening` on bronze,
  and on silver (`apply_changes` adds new columns but does not change types). A narrowing or
  an incompatible change needs a new bronze table or a rewrite.
* The newer instance's change table needs its own grant before the stream reaches S; the
  first read without it raises a `PermissionError` naming it.
* The older instance can be dropped once every stream reading the table has its
  `capture_instance_switched` event. Dropped earlier, the changes below S that only it held
  are lost: `DataLossError` says so, and `on_data_loss="resnapshot"` recovers with a snapshot.
* Planning asks one `sp_cdc_help_change_data_capture` and, per instance read, one
  `sp_cdc_get_ddl_history` and one `sp_cdc_get_captured_columns` more per batch.
* After the old instance is dropped and `captureInstance` renamed to the new one, SQL Server
  no longer lists the old name: rows stamped with it, a bootstrap snapshot included, match
  nothing. A `bootstrap=True` rerun then takes one more snapshot (harmless, only slower), and a
  silver table built from scratch fails loudly on the old rows (keep the old name configured,
  as the stream follows it). ponytail: record the table's instance names in the facts events
  and match on those if this shows up.
* SQL Server 2016 SP3, from the documentation only: the `sp_cdc_*` procedures used here exist
  with no documented per-version differences. `__$command_id` came with KB3030352 (2016 RTM
  CU5, SP1 CU2), so SP3 should have it, but `sp_vupgrade_replication` can fail: check the
  column exists on the production change tables (`includeCommandId=false` otherwise).
  `CURRENT_TIMEZONE_ID` is absent (the ADR 0008 fallback applies). 2016 left extended support
  on 2026-07-15.
* Unverified: all of it on SQL Server 2016; the `arrow-odbc` backend (ADR 0003); Databricks;
  a tie on `start_lsn` after cleanup (documented, not reproduced); `ddl_history` visibility
  is measured (8 DDLs), not documented; `snapshot_on_switch` against SQL Server. An optional
  lab check t9, also against the 2017 image, would cover the switch under a continuous writer.
* Tests: `tests/test_delta_sink.py` (event files folded once across a replay, `mergeSchema`,
  bronze migration 1, a switch adding a column with its events and no second bootstrap
  snapshot, following the new instance after the old is dropped, `snapshot_on_switch`, a
  snapshot after a DROP COLUMN, a re-snapshot after the old instance was dropped too early)
  and `tests/test_silver.py` (silver across a switch, with and without `options`).
