CREATE   PROCEDURE gold.usp_build_agg_product_loss_summary
AS
-- =========================================================
-- Rebuilds gold.agg_product_loss_summary (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.agg_product_loss_summary;

CREATE TABLE gold.agg_product_loss_summary AS
WITH sales AS (
    SELECT
        product_key,
        COUNT_BIG(*)                                                         AS total_lines,
        SUM(CAST(is_sold_below_cost AS int))                                 AS loss_lines,
        SUM(CAST(quantity AS bigint))                                        AS total_qty,
        SUM(CASE WHEN is_sold_below_cost = 1 THEN CAST(quantity AS bigint) ELSE 0 END) AS loss_qty,
        SUM(net_before_vat_sar)                                              AS revenue_sar,
        SUM(est_margin_sar)                                                  AS margin_sar,
        SUM(CASE WHEN is_sold_below_cost = 1 THEN -est_margin_sar ELSE 0 END) AS loss_sar,
        MIN(CASE WHEN is_sold_below_cost = 1 THEN CAST(transaction_ts AS date) END) AS first_loss_date,
        MAX(CASE WHEN is_sold_below_cost = 1 THEN CAST(transaction_ts AS date) END) AS last_loss_date,
        COUNT(DISTINCT CASE WHEN is_sold_below_cost = 1 THEN date_key / 100 END)    AS loss_months,
        COUNT(DISTINCT CASE WHEN is_sold_below_cost = 1 THEN pharmacy_key END)      AS loss_pharmacies
    FROM gold.fact_sales_line
    GROUP BY product_key
),
stock AS (
    SELECT
        product_key,
        SUM(closing_stock)            AS closing_stock_asof,
        SUM(stock_value_at_cost_sar)  AS stock_value_asof_sar
    FROM gold.fact_inventory_snapshot
    WHERE month_start = (SELECT MAX(month_start) FROM gold.fact_inventory_snapshot)
    GROUP BY product_key
)
SELECT
    s.product_key,
    p.product_id,
    p.trade_name,
    p.category,
    p.is_cost_imputed,
    p.is_cost_ratio_suspicious,
    s.total_lines,
    s.loss_lines,
    CAST(s.loss_lines * 100.0 / s.total_lines AS decimal(5,2))  AS loss_lines_pct,
    s.total_qty,
    s.loss_qty,
    CAST(s.revenue_sar AS decimal(18,2))                         AS revenue_sar,
    CAST(s.margin_sar  AS decimal(18,2))                         AS margin_sar,
    CAST(s.loss_sar    AS decimal(18,2))                         AS loss_sar,
    s.first_loss_date,
    s.last_loss_date,
    DATEDIFF(day, s.first_loss_date, s.last_loss_date) + 1       AS loss_span_days,
    s.loss_months,
    s.loss_pharmacies,
    COALESCE(st.closing_stock_asof, 0)                           AS closing_stock_asof,
    CAST(COALESCE(st.stock_value_asof_sar, 0) AS decimal(18,2))  AS stock_value_asof_sar
FROM sales s
JOIN gold.dim_product p        ON p.product_key  = s.product_key
LEFT JOIN stock st             ON st.product_key = s.product_key
WHERE s.loss_lines > 0
END

GO