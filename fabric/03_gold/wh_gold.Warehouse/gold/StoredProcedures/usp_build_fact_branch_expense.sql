CREATE   PROCEDURE gold.usp_build_fact_branch_expense
AS
-- =========================================================
-- Rebuilds gold.fact_branch_expense (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_branch_expense;

CREATE TABLE gold.fact_branch_expense AS
SELECT
    x.branch_expense_id,
    YEAR(x.accounting_month) * 10000 + MONTH(x.accounting_month) * 100 + 1 AS date_key,
    x.accounting_month,
    COALESCE(ph.pharmacy_key, -1)        AS pharmacy_key,
    x.expense_category_id,
    c.expense_category_code,
    c.expense_group,
    x.cost_basis,
    x.expense_amount_sar
FROM lh_silver.silver.finance_branch_monthly_expenses x
LEFT JOIN lh_silver.silver.finance_branch_expense_categories c ON c.expense_category_id = x.expense_category_id
LEFT JOIN gold.dim_pharmacy ph ON ph.pharmacy_id = x.pharmacy_id
END

GO