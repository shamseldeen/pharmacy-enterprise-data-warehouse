-- =========================================================
-- generate_procedures_from_backup.sql
-- Purpose : Generate one stored procedure per gold table from
--           dbo.gold_script_backup:  gold.usp_build_<table> = DROP + CTAS
-- Requires: extract_build_scripts_from_queryinsights.sql (all 3 steps)
-- Run in  : wh_gold (Fabric Warehouse)
-- How     : WHILE loop + dynamic SQL (sp_executesql).
--           CREATE OR ALTER -> idempotent: safe to re-run any time.
-- Check   : SELECT COUNT(*) FROM sys.procedures
--           WHERE SCHEMA_NAME(schema_id) = 'gold' AND name LIKE 'usp[_]build[_]%';
--           -> expect 21 (+ usp_build_all, created separately)
-- Notes   : - Newline before END protects against a trailing -- comment.
--           - Prefix usp_ (not sp_): sp_ is reserved for system procedures.
--           - After generating: test with EXCEPT before/after on a small table,
--             then run gold_rebuild_drill_reconciliation.sql.
-- Author  : Shamseldeen | 2026-10-24
-- =========================================================

DECLARE @i    int = 1;
DECLARE @n    int;
DECLARE @t    varchar(200);
DECLARE @stmt varchar(max);
DECLARE @sql  nvarchar(max);

SELECT @n = COUNT(*) FROM dbo.gold_script_backup;

WHILE @i <= @n
BEGIN
    SELECT @t = target_table, @stmt = create_statement
    FROM (
        SELECT target_table, create_statement,
               ROW_NUMBER() OVER (ORDER BY target_table) AS rn
        FROM dbo.gold_script_backup
    ) x
    WHERE rn = @i;

    SET @sql = N'CREATE OR ALTER PROCEDURE gold.usp_build_' + @t + N'
AS
-- =========================================================
-- Rebuilds gold.' + @t + N' (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.' + @t + N';

' + CAST(@stmt AS nvarchar(max)) + N'
END';

    EXEC sp_executesql @sql;
    SET @i = @i + 1;
END;
