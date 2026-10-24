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
