CREATE   PROCEDURE gold.usp_build_fact_sales_line
AS
-- =========================================================
-- Rebuilds gold.fact_sales_line (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_sales_line;

CREATE TABLE gold.fact_sales_line AS
WITH base AS (
    SELECT
        l.sales_line_id,
        l.transaction_id,
        l.line_number,
        YEAR(t.transaction_ts) * 10000 + MONTH(t.transaction_ts) * 100 + DAY(t.transaction_ts) AS date_key,
        t.transaction_ts,
        COALESCE(ph.pharmacy_key, -1)  AS pharmacy_key,
        COALESCE(p.product_key,  -1)   AS product_key,
        COALESCE(c.customer_key, -1)   AS customer_key,
        COALESCE(e.employee_key, -1)   AS employee_key,
        t.channel_id,
        l.quantity,
        l.unit_price_sar,
        l.gross_amount_sar,
        l.discount_amount_sar,
        l.net_before_vat_sar,
        l.vat_rate,
        l.vat_amount_sar,
        l.net_amount_sar,
        -- Cost = price BEFORE discount x product cost ratio (a discount lowers margin, not cost)
        CAST(l.gross_amount_sar * p.est_cost_ratio AS decimal(18,2)) AS est_cost_sar
    FROM lh_silver.silver.pos_sales_lines        l
    JOIN lh_silver.silver.pos_sales_transactions t  ON t.transaction_id = l.transaction_id
    LEFT JOIN gold.dim_pharmacy ph ON ph.pharmacy_id = t.pharmacy_id
    LEFT JOIN gold.dim_product  p  ON p.product_id   = l.product_id
    LEFT JOIN gold.dim_customer c  ON c.customer_id  = t.customer_id
    LEFT JOIN gold.dim_employee e  ON e.employee_id  = t.employee_id
    WHERE t.transaction_status = 'COMPLETED'
)
SELECT
    b.*,
    CAST(b.net_before_vat_sar - b.est_cost_sar AS decimal(18,2))                      AS est_margin_sar,
    CAST(CASE WHEN b.net_before_vat_sar - b.est_cost_sar < 0 THEN 1 ELSE 0 END AS bit) AS is_sold_below_cost
FROM base b
END

GO