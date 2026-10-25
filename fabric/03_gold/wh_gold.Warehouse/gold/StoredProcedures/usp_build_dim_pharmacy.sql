CREATE   PROCEDURE gold.usp_build_dim_pharmacy
AS
-- =========================================================
-- Rebuilds gold.dim_pharmacy (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_pharmacy;

CREATE TABLE gold.dim_pharmacy AS
WITH hours AS (
    SELECT pharmacy_id,
           MAX(CAST(is_24h AS int)) AS is_24h_any,
           SUM(operating_minutes)   AS weekly_operating_minutes
    FROM lh_silver.silver.erp_pharmacy_operating_hours
    GROUP BY pharmacy_id
),
first_sale AS (                     -- Silver, not Gold facts: no dim <-> fact circle
    SELECT pharmacy_id,
           MIN(CAST(transaction_ts AS date)) AS first_activity_date
    FROM lh_silver.silver.pos_sales_transactions
    GROUP BY pharmacy_id
),
data_start AS (
    SELECT MIN(CAST(transaction_ts AS date)) AS data_start_date
    FROM lh_silver.silver.pos_sales_transactions
),
src AS (
    SELECT p.*,
           h.is_24h_any, h.weekly_operating_minutes,
           fs.first_activity_date,
           CASE WHEN fs.first_activity_date
                     < DATEFROMPARTS(YEAR(p.opening_date), MONTH(p.opening_date), 1)
                THEN 1 ELSE 0 END                                     AS conflict_int,
           ds.data_start_date
    FROM lh_silver.silver.erp_pharmacies p
    LEFT JOIN hours h       ON h.pharmacy_id  = p.pharmacy_id
    LEFT JOIN first_sale fs ON fs.pharmacy_id = p.pharmacy_id
    CROSS JOIN data_start ds
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY s.pharmacy_id) AS int)  AS pharmacy_key,
    s.pharmacy_id,
    s.pharmacy_name,
    s.branch_type,
    s.region_id,
    s.region_name,
    s.city_name,
    s.district_name,
    s.city_name + ', Saudi Arabia'                           AS map_location,
    s.opening_date,
    YEAR(s.opening_date)                                     AS opening_year,
    CAST(s.weekly_operating_minutes / 60.0 AS decimal(6,1))  AS weekly_operating_hours,
    CAST(CASE WHEN s.is_24h_any = 1 THEN 1 ELSE 0 END AS bit) AS is_24h,
    s.first_activity_date,
    CASE WHEN s.conflict_int = 1 THEN s.first_activity_date
         ELSE s.opening_date END                             AS effective_opening_date,
    CAST(s.conflict_int AS bit)                              AS is_opening_date_conflict,
    CAST(CASE WHEN s.conflict_int = 1
               AND s.first_activity_date < DATEADD(month, 1, s.data_start_date)
              THEN 1 ELSE 0 END AS bit)                      AS is_age_lower_bound
FROM src s

UNION ALL

SELECT -1, 'UNKNOWN', 'Unknown pharmacy', 'Unknown', NULL, 'Unknown', 'Unknown', 'Unknown',
       NULL, NULL, NULL, NULL, CAST(0 AS bit),
       NULL, NULL, CAST(0 AS bit), CAST(0 AS bit)
END

GO