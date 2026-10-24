CREATE   PROCEDURE gold.usp_build_dim_insurance_plan
AS
-- =========================================================
-- Rebuilds gold.dim_insurance_plan (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_insurance_plan;

CREATE TABLE gold.dim_insurance_plan AS
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY p.insurance_plan_id) AS int) AS plan_key,
    p.insurance_plan_id,
    p.payer_name,
    p.plan_name,
    p.plan_tier,
    p.network_type,
    p.default_coverage_pct,
    p.default_copay_sar,
    p.annual_benefit_limit_sar,
    p.covers_prescriptions
FROM lh_silver.silver.insurance_plans p
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown payer', 'Unknown plan', 'Unknown', 'Unknown',
       NULL, NULL, NULL, CAST(0 AS bit)
END

GO