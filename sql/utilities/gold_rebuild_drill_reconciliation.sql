-- =========================================================
-- gold_rebuild_drill_reconciliation.sql
-- Purpose : Disaster-recovery drill. Proves the whole Gold layer can be
--           rebuilt from stored procedures and reconciles to the riyal.
-- Run in  : wh_gold (Fabric Warehouse), each STEP in its own window
-- Result  : 2026-10-24 -> 21 / 21 OK, full rebuild in 1 min 55 s
--           revenue 3,126,598,408.75 | margin 918,000,519.14
--           expense 6,799,713,291.35 | hours 100,616,484
-- Check   : row count + one money/flag total per table
--           (count catches missing rows, total catches changed logic)
-- Author  : Shamseldeen | 2026-10-24
-- =========================================================

-- ---------- STEP 1: snapshot BEFORE ----------
DROP TABLE IF EXISTS dbo.tmp_drill_before;

CREATE TABLE dbo.tmp_drill_before AS
SELECT 'ref_calendar_events' AS t, COUNT_BIG(*) AS n, CAST(0 AS decimal(18,2)) AS amount FROM gold.ref_calendar_events
UNION ALL SELECT 'dim_date',                COUNT_BIG(*), 0 FROM gold.dim_date
UNION ALL SELECT 'dim_pharmacy',            COUNT_BIG(*), 0 FROM gold.dim_pharmacy
UNION ALL SELECT 'dim_product',             COUNT_BIG(*), SUM(CAST(is_cost_ratio_suspicious AS int)) FROM gold.dim_product
UNION ALL SELECT 'dim_customer',            COUNT_BIG(*), 0 FROM gold.dim_customer
UNION ALL SELECT 'dim_employee',            COUNT_BIG(*), 0 FROM gold.dim_employee
UNION ALL SELECT 'dim_doctor',              COUNT_BIG(*), 0 FROM gold.dim_doctor
UNION ALL SELECT 'dim_insurance_plan',      COUNT_BIG(*), 0 FROM gold.dim_insurance_plan
UNION ALL SELECT 'dim_supplier',            COUNT_BIG(*), 0 FROM gold.dim_supplier
UNION ALL SELECT 'fact_sales_line',         COUNT_BIG(*), SUM(est_margin_sar)       FROM gold.fact_sales_line
UNION ALL SELECT 'fact_sales_transaction',  COUNT_BIG(*), SUM(net_before_vat_sar)   FROM gold.fact_sales_transaction
UNION ALL SELECT 'fact_return',             COUNT_BIG(*), SUM(refund_amount_sar)    FROM gold.fact_return
UNION ALL SELECT 'fact_rx_dispense',        COUNT_BIG(*), 0 FROM gold.fact_rx_dispense
UNION ALL SELECT 'fact_claim',              COUNT_BIG(*), 0 FROM gold.fact_claim
UNION ALL SELECT 'fact_inventory_snapshot', COUNT_BIG(*), SUM(stock_value_at_cost_sar) FROM gold.fact_inventory_snapshot
UNION ALL SELECT 'fact_purchase_line',      COUNT_BIG(*), 0 FROM gold.fact_purchase_line
UNION ALL SELECT 'fact_branch_expense',     COUNT_BIG(*), SUM(expense_amount_sar)   FROM gold.fact_branch_expense
UNION ALL SELECT 'fact_timekeeping',        COUNT_BIG(*), SUM(CAST(worked_hours AS bigint)) FROM gold.fact_timekeeping
UNION ALL SELECT 'agg_product_loss_summary',     COUNT_BIG(*), SUM(loss_sar)        FROM gold.agg_product_loss_summary
UNION ALL SELECT 'agg_stock_aging_asof',         COUNT_BIG(*), SUM(stock_value_sar) FROM gold.agg_stock_aging_asof
UNION ALL SELECT 'agg_branch_scorecard_monthly', COUNT_BIG(*), SUM(revenue_sar)     FROM gold.agg_branch_scorecard_monthly;

SELECT * FROM dbo.tmp_drill_before ORDER BY t;

-- ---------- STEP 2: the "disaster" (full rebuild, ~2 min) ----------
-- Do not stop it halfway and do not close the tab.
EXEC gold.usp_build_all;

-- ---------- STEP 3: compare AFTER vs BEFORE (expect 21 x OK) ----------
WITH after_ AS (
    SELECT 'ref_calendar_events' AS t, COUNT_BIG(*) AS n, CAST(0 AS decimal(18,2)) AS amount FROM gold.ref_calendar_events
    UNION ALL SELECT 'dim_date',                COUNT_BIG(*), 0 FROM gold.dim_date
    UNION ALL SELECT 'dim_pharmacy',            COUNT_BIG(*), 0 FROM gold.dim_pharmacy
    UNION ALL SELECT 'dim_product',             COUNT_BIG(*), SUM(CAST(is_cost_ratio_suspicious AS int)) FROM gold.dim_product
    UNION ALL SELECT 'dim_customer',            COUNT_BIG(*), 0 FROM gold.dim_customer
    UNION ALL SELECT 'dim_employee',            COUNT_BIG(*), 0 FROM gold.dim_employee
    UNION ALL SELECT 'dim_doctor',              COUNT_BIG(*), 0 FROM gold.dim_doctor
    UNION ALL SELECT 'dim_insurance_plan',      COUNT_BIG(*), 0 FROM gold.dim_insurance_plan
    UNION ALL SELECT 'dim_supplier',            COUNT_BIG(*), 0 FROM gold.dim_supplier
    UNION ALL SELECT 'fact_sales_line',         COUNT_BIG(*), SUM(est_margin_sar)       FROM gold.fact_sales_line
    UNION ALL SELECT 'fact_sales_transaction',  COUNT_BIG(*), SUM(net_before_vat_sar)   FROM gold.fact_sales_transaction
    UNION ALL SELECT 'fact_return',             COUNT_BIG(*), SUM(refund_amount_sar)    FROM gold.fact_return
    UNION ALL SELECT 'fact_rx_dispense',        COUNT_BIG(*), 0 FROM gold.fact_rx_dispense
    UNION ALL SELECT 'fact_claim',              COUNT_BIG(*), 0 FROM gold.fact_claim
    UNION ALL SELECT 'fact_inventory_snapshot', COUNT_BIG(*), SUM(stock_value_at_cost_sar) FROM gold.fact_inventory_snapshot
    UNION ALL SELECT 'fact_purchase_line',      COUNT_BIG(*), 0 FROM gold.fact_purchase_line
    UNION ALL SELECT 'fact_branch_expense',     COUNT_BIG(*), SUM(expense_amount_sar)   FROM gold.fact_branch_expense
    UNION ALL SELECT 'fact_timekeeping',        COUNT_BIG(*), SUM(CAST(worked_hours AS bigint)) FROM gold.fact_timekeeping
    UNION ALL SELECT 'agg_product_loss_summary',     COUNT_BIG(*), SUM(loss_sar)        FROM gold.agg_product_loss_summary
    UNION ALL SELECT 'agg_stock_aging_asof',         COUNT_BIG(*), SUM(stock_value_sar) FROM gold.agg_stock_aging_asof
    UNION ALL SELECT 'agg_branch_scorecard_monthly', COUNT_BIG(*), SUM(revenue_sar)     FROM gold.agg_branch_scorecard_monthly
)
SELECT b.t,
       b.n      AS n_before,      a.n      AS n_after,
       b.amount AS amount_before, a.amount AS amount_after,
       CASE WHEN b.n = a.n AND b.amount = a.amount THEN 'OK' ELSE 'DIFF' END AS result
FROM dbo.tmp_drill_before b
JOIN after_ a ON a.t = b.t
ORDER BY CASE WHEN b.n = a.n AND b.amount = a.amount THEN 1 ELSE 0 END, b.t;

-- ---------- STEP 4: clean up ----------
DROP TABLE dbo.tmp_drill_before;
