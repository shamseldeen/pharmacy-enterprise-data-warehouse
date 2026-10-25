CREATE   PROCEDURE gold.usp_build_dim_product
AS
-- =========================================================
-- Rebuilds gold.dim_product (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_product;

CREATE TABLE gold.dim_product AS
WITH batch_stats AS (
    SELECT DISTINCT
        product_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unit_cost_sar)
            OVER (PARTITION BY product_id)                                       AS median_unit_cost,
        COUNT(*) OVER (PARTITION BY product_id)                                  AS batch_count,
        MAX(CAST(is_recalled AS int)) OVER (PARTITION BY product_id)             AS has_recall,
        MAX(CASE WHEN temperature_class = 'Cold Chain' THEN 1 ELSE 0 END)
            OVER (PARTITION BY product_id)                                       AS needs_cold_chain
    FROM lh_silver.silver.product_batches
),
prod AS (
    SELECT p.product_id, p.trade_name, p.generic_name, p.strength, p.strength_unit,
           p.dosage_form, p.category, p.list_price_sar,
           b.median_unit_cost, b.batch_count, b.has_recall, b.needs_cold_chain,
           CASE WHEN p.list_price_sar > 0 AND b.median_unit_cost IS NOT NULL
                THEN b.median_unit_cost / p.list_price_sar END AS raw_ratio
    FROM lh_silver.silver.products p
    LEFT JOIN batch_stats b ON b.product_id = p.product_id
),
cat AS (
    SELECT DISTINCT category,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY raw_ratio)
               OVER (PARTITION BY category) AS cat_ratio
    FROM prod
    WHERE raw_ratio IS NOT NULL
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY pr.product_id) AS int) AS product_key,
    pr.product_id,
    pr.trade_name,
    pr.generic_name,
    pr.strength,
    pr.strength_unit,
    pr.dosage_form,
    pr.category,
    CAST(pr.list_price_sar AS decimal(18,2))                        AS list_price_sar,
    CAST(pr.median_unit_cost AS decimal(18,2))                      AS est_unit_cost_sar,
    CAST(COALESCE(pr.raw_ratio, c.cat_ratio) AS decimal(9,4))       AS est_cost_ratio,
    CAST(1 - COALESCE(pr.raw_ratio, c.cat_ratio) AS decimal(9,4))   AS est_margin_pct,
    COALESCE(pr.batch_count, 0)                                     AS batch_count,
    CAST(COALESCE(pr.has_recall, 0) AS bit)                         AS has_recalled_batch,
    CAST(COALESCE(pr.needs_cold_chain, 0) AS bit)                   AS needs_cold_chain,
    CAST(CASE WHEN pr.raw_ratio IS NULL THEN 1 ELSE 0 END AS bit)   AS is_cost_imputed,
    CAST(CASE WHEN pr.raw_ratio < 0.30
                OR pr.raw_ratio > 1.00 THEN 1 ELSE 0 END AS bit)    AS is_cost_ratio_suspicious   -- <<< CHANGED
FROM prod pr
LEFT JOIN cat c ON c.category = pr.category
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown product', NULL, NULL, NULL, 'Unknown', 'Unknown',
       NULL, NULL, NULL, NULL, 0, CAST(0 AS bit), CAST(0 AS bit), CAST(0 AS bit), CAST(0 AS bit)
END

GO