CREATE   PROCEDURE gold.usp_build_fact_sales_transaction
AS
-- =========================================================
-- Rebuilds gold.fact_sales_transaction (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_sales_transaction;

CREATE TABLE gold.fact_sales_transaction AS
WITH line_totals AS (
    SELECT transaction_id,
           COUNT(*)                  AS line_count,
           SUM(quantity)             AS items_qty,
           SUM(gross_amount_sar)     AS gross_amount_sar,
           SUM(discount_amount_sar)  AS discount_amount_sar,
           SUM(net_before_vat_sar)   AS net_before_vat_sar,
           SUM(vat_amount_sar)       AS vat_amount_sar,
           SUM(net_amount_sar)       AS net_amount_sar,
           SUM(est_margin_sar)       AS est_margin_sar
    FROM gold.fact_sales_line
    GROUP BY transaction_id
),
pay AS (                    -- one payment per completed transaction
    SELECT transaction_id,
           MAX(payment_method_id)    AS payment_method_id,
           SUM(payment_amount_sar)   AS paid_amount_sar
    FROM lh_silver.silver.pos_payments
    GROUP BY transaction_id
),
ret AS (                    -- refunds against this transaction
    SELECT original_transaction_id AS transaction_id,
           COUNT(*)                AS return_lines,
           SUM(refund_amount_sar)  AS refund_amount_sar
    FROM lh_silver.silver.pos_returns
    GROUP BY original_transaction_id
)
SELECT
    t.transaction_id,
    t.receipt_number,
    YEAR(t.transaction_ts) * 10000 + MONTH(t.transaction_ts) * 100 + DAY(t.transaction_ts) AS date_key,
    t.transaction_ts,
    DATEPART(hour, t.transaction_ts)                                   AS hour_of_day,     -- for peak hours
    COALESCE(ph.pharmacy_key, -1)                                      AS pharmacy_key,
    COALESCE(c.customer_key,  -1)                                      AS customer_key,
    COALESCE(e.employee_key,  -1)                                      AS employee_key,
    t.channel_id,
    t.transaction_status,
    CAST(CASE WHEN t.transaction_status = 'COMPLETED' THEN 1 ELSE 0 END AS bit) AS is_completed,
    CAST(CASE WHEN t.transaction_status = 'CANCELLED' THEN 1 ELSE 0 END AS bit) AS is_cancelled,
    CAST(CASE WHEN t.transaction_status = 'VOIDED'    THEN 1 ELSE 0 END AS bit) AS is_voided,
    COALESCE(lt.line_count, 0)                                         AS line_count,
    COALESCE(lt.items_qty, 0)                                          AS items_qty,
    COALESCE(lt.gross_amount_sar,    0)                                AS gross_amount_sar,
    COALESCE(lt.discount_amount_sar, 0)                                AS discount_amount_sar,
    COALESCE(lt.net_before_vat_sar,  0)                                AS net_before_vat_sar,
    COALESCE(lt.vat_amount_sar,      0)                                AS vat_amount_sar,
    COALESCE(lt.net_amount_sar,      0)                                AS net_amount_sar,
    COALESCE(lt.est_margin_sar,      0)                                AS est_margin_sar,
    p.payment_method_id,
    COALESCE(p.paid_amount_sar, 0)                                     AS paid_amount_sar,
    COALESCE(r.return_lines, 0)                                        AS return_lines,
    COALESCE(r.refund_amount_sar, 0)                                   AS refund_amount_sar,
    CAST(CASE WHEN r.transaction_id IS NOT NULL THEN 1 ELSE 0 END AS bit) AS has_return
FROM lh_silver.silver.pos_sales_transactions t
LEFT JOIN line_totals lt ON lt.transaction_id = t.transaction_id
LEFT JOIN pay         p  ON p.transaction_id  = t.transaction_id
LEFT JOIN ret         r  ON r.transaction_id  = t.transaction_id
LEFT JOIN gold.dim_pharmacy ph ON ph.pharmacy_id = t.pharmacy_id
LEFT JOIN gold.dim_customer c  ON c.customer_id  = t.customer_id
LEFT JOIN gold.dim_employee e  ON e.employee_id  = t.employee_id
END

GO