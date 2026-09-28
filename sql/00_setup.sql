-- Lab database with two CDC-tracked tables. Idempotent. Run as sysadmin (sa).
IF DB_ID(N'$(DATABASE)') IS NULL
    EXEC(N'CREATE DATABASE [$(DATABASE)]');
GO
USE [$(DATABASE)];
GO
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO
IF OBJECT_ID(N'dbo.customers') IS NULL
    CREATE TABLE dbo.customers (
        customer_id INT            NOT NULL PRIMARY KEY,
        name        NVARCHAR(120)  NOT NULL,
        email       NVARCHAR(200)  NOT NULL,
        city        NVARCHAR(80)   NULL,
        created_at  DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
    );
GO
IF OBJECT_ID(N'dbo.orders') IS NULL
    CREATE TABLE dbo.orders (
        order_id    INT            NOT NULL PRIMARY KEY,
        customer_id INT            NOT NULL,
        status      VARCHAR(20)    NOT NULL,
        amount      DECIMAL(18,2)  NOT NULL,
        created_at  DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at  DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
    );
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_customers')
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'customers',
         @role_name = NULL, @supports_net_changes = 0;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_orders')
    EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders',
         @role_name = NULL, @supports_net_changes = 0;
GO
SELECT capture_instance, CONVERT(varchar(22), start_lsn, 1) AS start_lsn FROM cdc.change_tables;
GO
