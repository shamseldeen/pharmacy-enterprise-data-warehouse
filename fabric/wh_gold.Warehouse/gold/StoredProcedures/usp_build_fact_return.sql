CREATE   PROCEDURE gold.usp_build_fact_return
AS
-- =========================================================
-- Rebuilds gold.fact_return (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_return;

CREATE TABLE gold.fact_return AS
SELECT
    r.return_id,
    r.original_transaction_id,
    r.original_sales_line_id,
    YEAR(r.return_ts) * 10000 + MONTH(r.return_ts) * 100 + DAY(r.return_ts) AS return_date_key,
    r.return_ts,
    s.date_key                                                   AS sale_date_key,
    DATEDIFF(day, s.transaction_ts, r.return_ts)                 AS days_to_return,
    COALESCE(s.pharmacy_key, -1)                                 AS pharmacy_key,
    COALESCE(s.customer_key, -1)                                 AS customer_key,
    COALESCE(s.employee_key, -1)                                 AS employee_key,   -- who sold it
    COALESCE(p.product_key,  -1)                                 AS product_key,
    r.return_reason,
    r.return_quantity,
    r.refund_amount_sar,
    s.quantity                                                   AS sold_quantity,
    s.net_amount_sar                                             AS sold_net_amount_sar,
    CAST(CASE WHEN r.refund_amount_sar > s.net_amount_sar THEN 1 ELSE 0 END AS bit) AS is_refund_above_sale,
    CAST(CASE WHEN r.return_quantity   > s.quantity       THEN 1 ELSE 0 END AS bit) AS is_qty_above_sale
FROM lh_silver.silver.pos_returns r
LEFT JOIN gold.fact_sales_line s ON s.sales_line_id = r.original_sales_line_id
LEFT JOIN gold.dim_product     p ON p.product_id    = r.product_id
END

GO