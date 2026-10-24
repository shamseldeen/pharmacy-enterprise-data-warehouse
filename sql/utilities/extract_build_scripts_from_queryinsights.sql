-- =========================================================
-- extract_build_scripts_from_queryinsights.sql
-- Purpose : Recover the latest successful CREATE TABLE statement for every
--           gold table from the warehouse query history, and store it in a
--           permanent table (dbo.gold_script_backup).
-- Why     : Gold scripts were written in "My queries" (not in Git).
--           queryinsights keeps executed SQL for 30 days only.
-- Run in  : wh_gold (Fabric Warehouse)
-- Steps   : 1) extract latest CTAS per table      (this block)
--           2) append post-create INSERTs          (ref_calendar_events)
--           3) fix doubled single quotes           ('' -> ')
-- Gotchas : - Each statement is logged separately: DROP and CREATE are
--             different rows; header comments before CREATE are NOT kept.
--           - queryinsights stores single quotes doubled ('Riyadh' -> ''Riyadh'').
--             Without step 3, 13 of 21 generated procedures fail.
--           - Tables built with CREATE TABLE (...) + INSERT need step 2.
-- Author  : Shamseldeen | 2026-10-24
-- =========================================================

-- ---------- STEP 1: latest successful CTAS per gold table ----------

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






-- ---------- STEP 2: append post-create INSERTs ----------
-- ref_calendar_events was built with CREATE TABLE (...) + INSERT VALUES,
-- so the INSERT must be added to its stored statement.
-- Check first which tables were modified after creation:
--   INSERT INTO / UPDATE / ALTER TABLE / DELETE FROM on gold.*
UPDATE b
SET create_statement = b.create_statement + CHAR(10) + CHAR(10) + CAST(q.command AS varchar(max))
FROM dbo.gold_script_backup b
CROSS JOIN (
    SELECT TOP 1 command
    FROM queryinsights.exec_requests_history
    WHERE status = 'Succeeded'
      AND command LIKE '%INSERT INTO gold.ref_calendar_events%'
      AND command NOT LIKE '%queryinsights%'
      AND command NOT LIKE '%gold_script_backup%'
    ORDER BY start_time DESC
) q
WHERE b.target_table = 'ref_calendar_events';

-- ---------- STEP 3: fix doubled single quotes ----------
-- Verify first: single_quotes must equal doubled_pairs * 2 for every table,
-- otherwise the script contains real empty strings ('') -> stop and review.
SELECT target_table,
       LEN(create_statement) - LEN(REPLACE(create_statement, '''', ''))          AS single_quotes,
       (LEN(create_statement) - LEN(REPLACE(create_statement, '''''', ''))) / 2  AS doubled_pairs
FROM dbo.gold_script_backup
ORDER BY target_table;

UPDATE dbo.gold_script_backup
SET create_statement = REPLACE(create_statement, '''''', '''');
