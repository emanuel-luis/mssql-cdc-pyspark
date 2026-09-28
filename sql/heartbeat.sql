-- Optional CDC heartbeat for a quiet source database. Run it in that database as db_owner
-- (creating the SQL Server Agent job needs sysadmin or SQLAgentOperatorRole). Idempotent.
--
-- sys.fn_cdc_get_max_lsn() only moves when the capture job writes to cdc.lsn_time_mapping.
-- With no changes to CDC-tracked tables, SQL Server 2022 writes an idle entry about every
-- 5 minutes (lab t1), so finalized_until can lag that long; writes to untracked tables and
-- CHECKPOINT do not help. A tiny update to a CDC-tracked table every 10 seconds (the Agent
-- minimum) bounds the lag at about that interval. See docs/decisions/0010.
--
-- Remove: EXEC msdb.dbo.sp_delete_job @job_name = N'cdc_heartbeat_<database>';
--         EXEC sys.sp_cdc_disable_table N'dbo', N'cdc_heartbeat', N'all';
--         DROP TABLE dbo.cdc_heartbeat;
IF OBJECT_ID(N'dbo.cdc_heartbeat') IS NULL
    CREATE TABLE dbo.cdc_heartbeat (
        id      TINYINT      NOT NULL PRIMARY KEY,
        beat_at DATETIME2(3) NOT NULL
    );
GO
IF NOT EXISTS (SELECT 1 FROM dbo.cdc_heartbeat)
    INSERT INTO dbo.cdc_heartbeat (id, beat_at) VALUES (1, SYSUTCDATETIME());
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_cdc_heartbeat')
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'cdc_heartbeat',
         @role_name = NULL, @supports_net_changes = 0;
GO
DECLARE @db sysname = DB_NAME();
DECLARE @job sysname = N'cdc_heartbeat_' + @db;
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @job)
BEGIN
    EXEC msdb.dbo.sp_add_job @job_name = @job,
         @description = N'Keeps sys.fn_cdc_get_max_lsn() moving on a quiet database (mssql-cdc-pyspark).';
    EXEC msdb.dbo.sp_add_jobstep @job_name = @job, @step_name = N'beat', @subsystem = N'TSQL',
         @database_name = @db,
         @command = N'UPDATE dbo.cdc_heartbeat SET beat_at = SYSUTCDATETIME() WHERE id = 1;';
    EXEC msdb.dbo.sp_add_jobschedule @job_name = @job, @name = N'every 10 seconds',
         @freq_type = 4, @freq_interval = 1,          -- daily, every day
         @freq_subday_type = 2, @freq_subday_interval = 10;  -- every 10 seconds
    EXEC msdb.dbo.sp_add_jobserver @job_name = @job;
END
GO
