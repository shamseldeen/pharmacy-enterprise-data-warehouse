CREATE   PROCEDURE gold.usp_build_dim_doctor
AS
-- =========================================================
-- Rebuilds gold.dim_doctor (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_doctor;

CREATE TABLE gold.dim_doctor AS
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY d.doctor_id) AS int) AS doctor_key,
    d.doctor_id,
    d.full_name,
    d.gender,
    d.specialty_name,
    d.region_name,
    d.city_name,
    d.active_from,
    d.active_to,                                            -- NULL = still practising (open-ended)
    CAST(CASE WHEN d.active_to IS NULL
                OR d.active_to > CAST('2025-12-31' AS date)
              THEN 1 ELSE 0 END AS bit)                    AS is_active_at_asof
FROM lh_silver.silver.rx_doctors d
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown doctor', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
       NULL, NULL, CAST(0 AS bit)
END

GO