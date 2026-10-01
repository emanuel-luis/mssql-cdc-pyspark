-- Switch a CDC-tracked table to a new capture instance, for example to capture a column added
-- to the table: a capture instance keeps the column list it was enabled with (ADR 0023).
-- A procedure to follow step by step in the source database as db_owner, not a script to run
-- unattended. mssql-cdc-pyspark reads both instances and moves to the new one on its own at
-- the new one's start LSN; it never creates or drops capture instances.
--
-- Replace the names first: the table dbo.orders, its current instance dbo_orders, the new
-- one dbo_orders_v2, and cdc_reader, the database user the streams connect as.

-- 1. Enable the new instance with the new column list. A table has at most two instances.
--    The enable waits for open transactions that already wrote the table; they end up in the
--    old instance only, below the new one's start LSN, which the stream reads from the old.
EXEC sys.sp_cdc_enable_table
     @source_schema        = N'dbo',
     @source_name          = N'orders',
     @capture_instance     = N'dbo_orders_v2',
     @role_name            = NULL,   -- or the old instance's gating role (the reader is a member)
     @captured_column_list = NULL,   -- NULL: every column; or N'order_id, status, note'
     @supports_net_changes = 0;
GO

-- 2. Let the reader read the new change table. Each capture instance has its own; without
--    this grant the first batch that reaches the new instance fails with a PermissionError
--    naming it. Nothing else changes for the reader (ADR 0009).
GRANT SELECT ON cdc.[dbo_orders_v2_CT] TO [cdc_reader];
GO

-- 3. Check: both instances of the table, and the new one's start LSN. Its min_lsn stays
--    0x00... until the capture job has processed the enable (seconds; up to ~5 minutes on a
--    quiet database).
EXEC sys.sp_cdc_help_change_data_capture @source_schema = N'dbo', @source_name = N'orders';
SELECT sys.fn_cdc_get_min_lsn(N'dbo_orders_v2') AS new_min_lsn;
GO

-- 4. Wait for every stream that reads the table. Each one writes a facts row with
--    event = 'capture_instance_switched' (detail 'dbo_orders -> dbo_orders_v2') once it has
--    read past the new instance's start. Spark commits that batch after its facts row: wait
--    for the stream's next batch too (a row of the same app_id with a larger batch_id). The
--    event reaches the facts only through metricsPath, which to_delta sets on its own only for
--    a local or Volume checkpoint; without it, look for the warning in the stream's log. A
--    stream already at or past the new instance's start (started after the enable, e.g. with
--    bootstrap=True: its facts end_lsn >= new_min_lsn) never reads the old instance, writes
--    no event and needs no wait. A stream whose schema lacks a column the new instance
--    captures stops there with SchemaChangedError instead: restart it, and it infers the new
--    columns and goes on.
--    In the lakehouse, for example:
--      SELECT app_id, batch_id, detail, written_at FROM ops.ingestion_facts
--      WHERE event = 'capture_instance_switched' AND target = 'bronze.orders';
--    Until step 5 both instances capture every change: twice the change-table writes.

-- 5. Disable the old instance. Its change table goes at once. A stream that had not read up
--    to the new instance's start loses the changes only the old one held: it fails with
--    DataLossError, and to_delta(on_data_loss="resnapshot") recovers with a snapshot.
EXEC sys.sp_cdc_disable_table
     @source_schema    = N'dbo',
     @source_name      = N'orders',
     @capture_instance = N'dbo_orders';
GO
