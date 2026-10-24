CREATE   PROCEDURE gold.usp_build_dim_supplier
AS
-- =========================================================
-- Rebuilds gold.dim_supplier (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_supplier;

CREATE TABLE gold.dim_supplier AS
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY s.supplier_id) AS int) AS supplier_key,
    s.supplier_id,
    s.supplier_name,
    s.supplier_type_name,
    s.region_name,
    s.city_name,
    s.standard_lead_time_days,
    s.is_cold_chain_capable,
    s.minimum_order_value_sar,
    s.supplier_status
FROM lh_silver.silver.supply_chain_suppliers s
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown supplier', 'Unknown', 'Unknown', 'Unknown',
       NULL, CAST(0 AS bit), NULL, 'Unknown'
END

GO