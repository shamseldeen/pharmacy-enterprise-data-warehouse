DROP TABLE IF EXISTS dbo.gold_script_backup;

CREATE TABLE dbo.gold_script_backup AS
WITH runs AS (
    SELECT start_time, command,
           TRANSLATE(command, CHAR(13) + CHAR(10) + CHAR(9), '   ') AS cmd_flat
    FROM queryinsights.exec_requests_history
    WHERE command LIKE '%CREATE TABLE gold.%'
      AND status = 'Succeeded'
),
targets AS (
    SELECT *,
        LEFT(SUBSTRING(cmd_flat, CHARINDEX('CREATE TABLE gold.', cmd_flat) + 18, 60),
             CHARINDEX(' ', SUBSTRING(cmd_flat, CHARINDEX('CREATE TABLE gold.', cmd_flat) + 18, 60) + ' ') - 1) AS target_table
    FROM runs
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY target_table ORDER BY start_time DESC) AS rn
    FROM targets
)
SELECT
    target_table,
    start_time                AS captured_from_run_at_utc,
    CAST(command AS varchar(max)) AS create_statement
FROM latest
WHERE rn = 1
  AND target_table NOT LIKE 'tmp[_]%';
