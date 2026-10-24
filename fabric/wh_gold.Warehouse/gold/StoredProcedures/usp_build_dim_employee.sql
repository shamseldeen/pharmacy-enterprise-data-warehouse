CREATE   PROCEDURE gold.usp_build_dim_employee
AS
-- =========================================================
-- Rebuilds gold.dim_employee (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_employee;

CREATE TABLE gold.dim_employee AS
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY e.employee_id) AS int) AS employee_key,
    e.employee_id,
    e.full_name,
    e.gender,
    e.nationality_group,
    e.primary_job_role_name                                    AS job_role,
    CAST(CASE WHEN e.primary_job_role_name IN ('Pharmacist','Senior Pharmacist','Pharmacy Manager')
              THEN 1 ELSE 0 END AS bit)                        AS is_pharmacist,
    CAST(CASE WHEN e.primary_job_role_name IN ('Pharmacist','Senior Pharmacist','Pharmacy Manager',
                                               'Pharmacy Technician')
              THEN 1 ELSE 0 END AS bit)                        AS can_dispense,     -- pharmacists + pharmacy assistants
    e.hire_date,
    e.termination_date,
    CAST(CASE WHEN e.termination_date IS NULL
                OR e.termination_date > CAST('2025-12-31' AS date)
              THEN 1 ELSE 0 END AS bit)                        AS is_active_at_asof,
    CAST(FLOOR(DATEDIFF(day, e.hire_date,
              COALESCE(e.termination_date, CAST('2025-12-31' AS date))) / 365.25) AS int) AS tenure_years
FROM lh_silver.silver.erp_employees e
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown employee', 'Unknown', 'Unknown', 'Unknown',
       CAST(0 AS bit), CAST(0 AS bit), NULL, NULL, CAST(0 AS bit), NULL
END

GO