CREATE   PROCEDURE gold.usp_build_dim_date
AS
-- =========================================================
-- Rebuilds gold.dim_date (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_date;

CREATE TABLE gold.dim_date AS
WITH digits AS (
    SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS v(n)
),
tally AS (                                   -- numbers 0 .. 9999, no loops
    SELECT a.n + b.n*10 + c.n*100 + d.n*1000 AS n
    FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d
),
base AS (
    SELECT DATEADD(day, n, CAST('2020-01-01' AS date)) AS d,
           (DATEDIFF(day, CAST('2000-01-02' AS date),
                     DATEADD(day, n, CAST('2020-01-01' AS date))) % 7) + 1 AS dow  -- 2000-01-02 was Sunday -> Sunday = 1
    FROM tally
    WHERE n <= DATEDIFF(day, CAST('2020-01-01' AS date), CAST('2026-12-31' AS date))
)
SELECT
    YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)    AS date_key,          -- e.g. 20250315
    d                                            AS full_date,
    YEAR(d)                                      AS year_number,
    DATEPART(quarter, d)                         AS quarter_number,
    MONTH(d)                                     AS month_number,
    CASE MONTH(d) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr'
                  WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug'
                  WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' ELSE 'Dec' END AS month_name,
    DATEFROMPARTS(YEAR(d), MONTH(d), 1)          AS month_start,
    DAY(d)                                       AS day_of_month,
    dow                                          AS day_of_week,        -- 1 = Sunday ... 7 = Saturday
    CASE dow WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed'
             WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' ELSE 'Sat' END AS day_name,
    CAST(CASE WHEN dow IN (6, 7) THEN 1 ELSE 0 END AS bit) AS is_weekend,  -- Saudi weekend: Fri + Sat
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'RAMADAN' AND d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_ramadan,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'EID_AL_FITR' AND d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_eid_al_fitr,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'EID_AL_ADHA' AND d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_eid_al_adha,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'SCHOOL_HOLIDAY' AND d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_school_holiday,
    CAST(CASE WHEN MONTH(d) = 9 AND DAY(d) = 23 THEN 1                        -- National Day
              WHEN MONTH(d) = 2 AND DAY(d) = 22 AND YEAR(d) >= 2022 THEN 1    -- Founding Day (from 2022)
              ELSE 0 END AS bit) AS is_national_holiday
FROM base
END

GO