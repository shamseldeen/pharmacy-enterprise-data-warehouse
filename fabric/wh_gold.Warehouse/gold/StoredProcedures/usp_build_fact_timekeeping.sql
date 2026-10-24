CREATE   PROCEDURE gold.usp_build_fact_timekeeping
AS
-- =========================================================
-- Rebuilds gold.fact_timekeeping (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_timekeeping;

CREATE TABLE gold.fact_timekeeping AS
SELECT
    t.timekeeping_id,
    YEAR(t.accounting_month) * 10000 + MONTH(t.accounting_month) * 100 + 1 AS date_key,
    t.accounting_month,
    COALESCE(e.employee_key, -1)         AS employee_key,
    COALESCE(ph.pharmacy_key, -1)        AS pharmacy_key,
    t.assignment_id,
    t.scheduled_hours,
    t.worked_hours,
    t.overtime_hours,
    t.absence_hours,
    t.sick_leave_hours,
    t.late_minutes
FROM lh_silver.silver.erp_employee_monthly_timekeeping t
LEFT JOIN gold.dim_employee e  ON e.employee_id  = t.employee_id
LEFT JOIN gold.dim_pharmacy ph ON ph.pharmacy_id = t.pharmacy_id
END

GO