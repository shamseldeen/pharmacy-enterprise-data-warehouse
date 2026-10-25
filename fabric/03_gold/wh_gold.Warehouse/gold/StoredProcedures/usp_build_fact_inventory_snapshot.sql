CREATE   PROCEDURE gold.usp_build_fact_inventory_snapshot
AS
-- =========================================================
-- Rebuilds gold.fact_inventory_snapshot (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_inventory_snapshot;

CREATE TABLE gold.fact_inventory_snapshot AS
WITH avg_price AS (            -- average actual selling price per product (all years)
    SELECT product_key,
           SUM(net_before_vat_sar) / NULLIF(SUM(quantity), 0) AS avg_unit_price_sar
    FROM gold.fact_sales_line
    GROUP BY product_key
)
SELECT
    YEAR(s.month_start) * 10000 + MONTH(s.month_start) * 100 + 1      AS date_key,      -- first day of month
    s.month_start,
    COALESCE(ph.pharmacy_key, -1)                                     AS pharmacy_key,
    COALESCE(p.product_key,  -1)                                      AS product_key,
    s.opening_stock,
    s.net_movement_qty,
    s.closing_stock,
    s.unfulfilled_qty,
    CAST(CASE WHEN s.closing_stock = 0   THEN 1 ELSE 0 END AS bit)    AS is_stockout,
    CAST(CASE WHEN s.unfulfilled_qty > 0 THEN 1 ELSE 0 END AS bit)    AS has_lost_demand,
    CAST(s.closing_stock * p.est_unit_cost_sar AS decimal(18,2))      AS stock_value_at_cost_sar,
    CAST(s.unfulfilled_qty * COALESCE(ap.avg_unit_price_sar, p.list_price_sar)
         AS decimal(18,2))                                            AS lost_sales_value_est_sar
FROM lh_silver.silver.inventory_monthly_snapshot s
LEFT JOIN gold.dim_pharmacy ph ON ph.pharmacy_id = s.pharmacy_id
LEFT JOIN gold.dim_product  p  ON p.product_id   = s.product_id
LEFT JOIN avg_price         ap ON ap.product_key = p.product_key
END

GO