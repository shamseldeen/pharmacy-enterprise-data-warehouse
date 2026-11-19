CREATE PROCEDURE gold.usp_build_dim_date
AS
-- =========================================================
-- Rebuilds gold.dim_date (DROP + CTAS)
-- 2026-11-19: outer CTE aliased as b and every subquery uses b.d.
-- An unqualified d failed the SQL project build during deployment (SQL71501).
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_date;

CREATE TABLE gold.dim_date AS
WITH digits AS (
    SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS v(n)
),
tally AS (
    SELECT a.n + b.n*10 + c.n*100 + d.n*1000 AS n
    FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d
),
base AS (
    SELECT DATEADD(day, n, CAST('2020-01-01' AS date)) AS d,
           (DATEDIFF(day, CAST('2000-01-02' AS date),
                     DATEADD(day, n, CAST('2020-01-01' AS date))) % 7) + 1 AS dow
    FROM tally
    WHERE n <= DATEDIFF(day, CAST('2020-01-01' AS date), CAST('2026-12-31' AS date))
)
SELECT
    YEAR(b.d) * 10000 + MONTH(b.d) * 100 + DAY(b.d) AS date_key,
    b.d                                             AS full_date,
    YEAR(b.d)                                       AS year_number,
    DATEPART(quarter, b.d)                          AS quarter_number,
    MONTH(b.d)                                      AS month_number,
    CASE MONTH(b.d) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr'
                    WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug'
                    WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' ELSE 'Dec' END AS month_name,
    DATEFROMPARTS(YEAR(b.d), MONTH(b.d), 1)         AS month_start,
    DAY(b.d)                                        AS day_of_month,
    b.dow                                           AS day_of_week,
    CASE b.dow WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed'
               WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' ELSE 'Sat' END AS day_name,
    CAST(CASE WHEN b.dow IN (6, 7) THEN 1 ELSE 0 END AS bit) AS is_weekend,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'RAMADAN' AND b.d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_ramadan,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'EID_AL_FITR' AND b.d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_eid_al_fitr,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'EID_AL_ADHA' AND b.d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_eid_al_adha,
    CAST(CASE WHEN EXISTS (SELECT 1 FROM gold.ref_calendar_events e
                           WHERE e.event_type = 'SCHOOL_HOLIDAY' AND b.d BETWEEN e.start_date AND e.end_date)
              THEN 1 ELSE 0 END AS bit) AS is_school_holiday,
    CAST(CASE WHEN MONTH(b.d) = 9 AND DAY(b.d) = 23 THEN 1
              WHEN MONTH(b.d) = 2 AND DAY(b.d) = 22 AND YEAR(b.d) >= 2022 THEN 1
              ELSE 0 END AS bit) AS is_national_holiday
FROM base AS b
END

GO