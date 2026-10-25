CREATE   PROCEDURE gold.usp_build_agg_branch_scorecard_monthly
AS
-- =========================================================
-- Rebuilds gold.agg_branch_scorecard_monthly (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.agg_branch_scorecard_monthly;

CREATE TABLE gold.agg_branch_scorecard_monthly AS
WITH months AS (
    SELECT DISTINCT DATEFROMPARTS(date_key / 10000, (date_key / 100) % 100, 1) AS month_start
    FROM gold.fact_sales_transaction
),
ph AS (
    SELECT pharmacy_key, branch_type, region_name, city_name,
           effective_opening_date, is_age_lower_bound,
           CASE WHEN region_name IN ('Riyadh', 'Makkah', 'Eastern Province')
                THEN 'Major' ELSE 'Other' END AS region_tier
    FROM gold.dim_pharmacy
    WHERE pharmacy_key <> -1
),
grid AS (
    SELECT p.*, m.month_start,
           DATEDIFF(month, p.effective_opening_date, m.month_start) AS age_months
    FROM ph p
    CROSS JOIN months m
    WHERE m.month_start >= DATEFROMPARTS(YEAR(p.effective_opening_date), MONTH(p.effective_opening_date), 1)
),
sales AS (
    SELECT pharmacy_key,
           DATEFROMPARTS(date_key / 10000, (date_key / 100) % 100, 1) AS month_start,
           SUM(net_before_vat_sar) AS revenue_sar,
           SUM(est_margin_sar)     AS margin_sar,
           COUNT(*)                AS transactions
    FROM gold.fact_sales_transaction
    WHERE is_completed = 1
    GROUP BY pharmacy_key, DATEFROMPARTS(date_key / 10000, (date_key / 100) % 100, 1)
),
rets AS (
    SELECT pharmacy_key,
           DATEFROMPARTS(return_date_key / 10000, (return_date_key / 100) % 100, 1) AS month_start,
           SUM(refund_amount_sar) AS refund_sar
    FROM gold.fact_return
    GROUP BY pharmacy_key, DATEFROMPARTS(return_date_key / 10000, (return_date_key / 100) % 100, 1)
),
hrs AS (
    SELECT pharmacy_key, accounting_month AS month_start,
           SUM(CAST(worked_hours AS bigint)) AS worked_hours
    FROM gold.fact_timekeeping
    GROUP BY pharmacy_key, accounting_month
),
exps AS (
    SELECT pharmacy_key, accounting_month AS month_start,
           SUM(expense_amount_sar) AS expense_sar
    FROM gold.fact_branch_expense
    GROUP BY pharmacy_key, accounting_month
),
resp_rank AS (              -- responsible pharmacist: Manager > Senior > Pharmacist, then most hours
    SELECT t.pharmacy_key, t.accounting_month AS month_start, t.employee_key,
           MIN(CASE e.job_role WHEN 'Pharmacy Manager'  THEN 1
                               WHEN 'Senior Pharmacist' THEN 2
                               ELSE 3 END)                                AS role_rank,
           ROW_NUMBER() OVER (
               PARTITION BY t.pharmacy_key, t.accounting_month
               ORDER BY MIN(CASE e.job_role WHEN 'Pharmacy Manager'  THEN 1
                                            WHEN 'Senior Pharmacist' THEN 2
                                            ELSE 3 END),
                        SUM(t.worked_hours) DESC,
                        t.employee_key)                                   AS rn
    FROM gold.fact_timekeeping t
    JOIN gold.dim_employee e ON e.employee_key = t.employee_key
    WHERE e.job_role IN ('Pharmacy Manager', 'Senior Pharmacist', 'Pharmacist')
    GROUP BY t.pharmacy_key, t.accounting_month, t.employee_key
),
base AS (
    SELECT
        g.pharmacy_key, g.month_start,
        YEAR(g.month_start) * 10000 + MONTH(g.month_start) * 100 + 1 AS date_key,
        g.branch_type, g.region_name, g.city_name, g.region_tier,
        g.effective_opening_date, g.is_age_lower_bound, g.age_months,
        COALESCE(s.revenue_sar, 0)        AS revenue_sar,
        COALESCE(s.margin_sar, 0)         AS margin_sar,
        COALESCE(s.transactions, 0)       AS transactions,
        COALESCE(r.refund_sar, 0)         AS refund_sar,
        COALESCE(h.worked_hours, 0)       AS worked_hours,
        COALESCE(x.expense_sar, 0)        AS expense_sar,
        COALESCE(rp.employee_key, -1)     AS responsible_employee_key,
        CASE WHEN rp.employee_key IS NULL THEN NULL
             WHEN rp.role_rank = 1 THEN 0 ELSE 1 END AS responsible_inferred_int
    FROM grid g
    LEFT JOIN sales s     ON s.pharmacy_key  = g.pharmacy_key AND s.month_start  = g.month_start
    LEFT JOIN rets  r     ON r.pharmacy_key  = g.pharmacy_key AND r.month_start  = g.month_start
    LEFT JOIN hrs   h     ON h.pharmacy_key  = g.pharmacy_key AND h.month_start  = g.month_start
    LEFT JOIN exps  x     ON x.pharmacy_key  = g.pharmacy_key AND x.month_start  = g.month_start
    LEFT JOIN resp_rank rp ON rp.pharmacy_key = g.pharmacy_key AND rp.month_start = g.month_start AND rp.rn = 1
),
yearly AS (
    SELECT pharmacy_key, YEAR(month_start) AS yr,
           SUM(revenue_sar) * 12.0 / COUNT(*) AS annualized_revenue
    FROM base
    GROUP BY pharmacy_key, YEAR(month_start)
),
bands AS (
    SELECT pharmacy_key, yr,
           CASE NTILE(3) OVER (PARTITION BY yr ORDER BY annualized_revenue DESC)
                WHEN 1 THEN 'A' WHEN 2 THEN 'B' ELSE 'C' END AS size_band
    FROM yearly
),
calc AS (
    SELECT b.*,
        COALESCE(bp.size_band, bc.size_band)                                  AS size_band,
        CASE WHEN bp.size_band IS NULL THEN 1 ELSE 0 END                      AS band_same_year_int,
        CAST(b.margin_sar  * 100.0 / NULLIF(b.revenue_sar, 0)  AS decimal(9,2))  AS margin_pct,
        CAST(b.revenue_sar / NULLIF(b.worked_hours, 0)         AS decimal(18,2)) AS sales_per_hour,
        CAST(CASE WHEN b.expense_sar > 0
                  THEN b.expense_sar * 100.0 / NULLIF(b.revenue_sar, 0) END AS decimal(9,2)) AS expense_pct,
        CAST(b.revenue_sar / NULLIF(b.transactions, 0)         AS decimal(18,2)) AS avg_basket_sar,
        CAST(b.refund_sar  * 100.0 / NULLIF(b.revenue_sar, 0)  AS decimal(9,2))  AS return_pct,
        LAG(b.revenue_sar, 12) OVER (PARTITION BY b.pharmacy_key ORDER BY b.month_start) AS revenue_ly_sar,
        CASE WHEN b.responsible_employee_key
                  <> LAG(b.responsible_employee_key) OVER (PARTITION BY b.pharmacy_key ORDER BY b.month_start)
             THEN 1 ELSE 0 END                                                AS resp_changed_int
    FROM base b
    LEFT JOIN bands bp ON bp.pharmacy_key = b.pharmacy_key AND bp.yr = YEAR(b.month_start) - 1
    LEFT JOIN bands bc ON bc.pharmacy_key = b.pharmacy_key AND bc.yr = YEAR(b.month_start)
),
calc2 AS (
    SELECT c.*,
        c.size_band + '-' + c.region_tier                                     AS peer_group,
        CASE WHEN c.age_months >= 12 AND c.revenue_sar > 0 THEN 1 ELSE 0 END   AS ranked_int,
        CASE WHEN c.age_months >= 24 AND c.revenue_ly_sar > 0 THEN 1 ELSE 0 END AS growth_ranked_int,
        CAST((c.revenue_sar - c.revenue_ly_sar) * 100.0 / NULLIF(c.revenue_ly_sar, 0) AS decimal(9,2)) AS yoy_growth_pct,
        SUM(c.resp_changed_int) OVER (PARTITION BY c.pharmacy_key ORDER BY c.month_start
                                      ROWS UNBOUNDED PRECEDING)               AS resp_run_id
    FROM calc c
),
ranked AS (
    SELECT c.*,
        CASE WHEN ranked_int = 1 AND sales_per_hour IS NOT NULL THEN PERCENT_RANK() OVER (
             PARTITION BY month_start, peer_group, ranked_int, CASE WHEN sales_per_hour IS NULL THEN 0 ELSE 1 END
             ORDER BY sales_per_hour) END                                     AS pr_sales_per_hour,
        CASE WHEN ranked_int = 1 AND margin_pct IS NOT NULL THEN PERCENT_RANK() OVER (
             PARTITION BY month_start, peer_group, ranked_int, CASE WHEN margin_pct IS NULL THEN 0 ELSE 1 END
             ORDER BY margin_pct) END                                         AS pr_margin_pct,
        CASE WHEN ranked_int = 1 AND expense_pct IS NOT NULL THEN PERCENT_RANK() OVER (
             PARTITION BY month_start, peer_group, ranked_int, CASE WHEN expense_pct IS NULL THEN 0 ELSE 1 END
             ORDER BY expense_pct DESC) END                                   AS pr_expense_pct,
        CASE WHEN ranked_int = 1 AND avg_basket_sar IS NOT NULL THEN PERCENT_RANK() OVER (
             PARTITION BY month_start, peer_group, ranked_int, CASE WHEN avg_basket_sar IS NULL THEN 0 ELSE 1 END
             ORDER BY avg_basket_sar) END                                     AS pr_avg_basket,
        CASE WHEN ranked_int = 1 AND return_pct IS NOT NULL THEN PERCENT_RANK() OVER (
             PARTITION BY month_start, peer_group, ranked_int, CASE WHEN return_pct IS NULL THEN 0 ELSE 1 END
             ORDER BY return_pct DESC) END                                    AS pr_return_pct,
        CASE WHEN growth_ranked_int = 1 AND yoy_growth_pct IS NOT NULL THEN PERCENT_RANK() OVER (
             PARTITION BY month_start, peer_group, growth_ranked_int, CASE WHEN yoy_growth_pct IS NULL THEN 0 ELSE 1 END
             ORDER BY yoy_growth_pct) END                                     AS pr_yoy_growth,
        ROW_NUMBER() OVER (PARTITION BY pharmacy_key, resp_run_id ORDER BY month_start) AS months_with_current_responsible
    FROM calc2 c
)
SELECT
    date_key, month_start, pharmacy_key,
    branch_type, region_name, city_name, region_tier,
    effective_opening_date, is_age_lower_bound, age_months,
    size_band, CAST(band_same_year_int AS bit) AS is_band_from_same_year, peer_group,
    CAST(ranked_int AS bit)        AS is_ranked,
    CAST(growth_ranked_int AS bit) AS is_growth_ranked,
    CAST(revenue_sar AS decimal(18,2))    AS revenue_sar,
    CAST(margin_sar  AS decimal(18,2))    AS margin_sar,
    transactions,
    CAST(refund_sar  AS decimal(18,2))    AS refund_sar,
    worked_hours,
    CAST(expense_sar AS decimal(18,2))    AS expense_sar,
    CAST(revenue_ly_sar AS decimal(18,2)) AS revenue_ly_sar,
    sales_per_hour, margin_pct, expense_pct, avg_basket_sar, return_pct, yoy_growth_pct,
    CAST(pr_sales_per_hour AS decimal(5,4)) AS pr_sales_per_hour,
    CAST(pr_margin_pct     AS decimal(5,4)) AS pr_margin_pct,
    CAST(pr_expense_pct    AS decimal(5,4)) AS pr_expense_pct,
    CAST(pr_avg_basket     AS decimal(5,4)) AS pr_avg_basket,
    CAST(pr_return_pct     AS decimal(5,4)) AS pr_return_pct,
    CAST(pr_yoy_growth     AS decimal(5,4)) AS pr_yoy_growth,
    CAST( (COALESCE(pr_sales_per_hour, 0) + COALESCE(pr_margin_pct, 0) + COALESCE(pr_expense_pct, 0)
         + COALESCE(pr_avg_basket, 0)     + COALESCE(pr_return_pct, 0))
        / NULLIF( CASE WHEN pr_sales_per_hour IS NULL THEN 0 ELSE 1 END
                + CASE WHEN pr_margin_pct     IS NULL THEN 0 ELSE 1 END
                + CASE WHEN pr_expense_pct    IS NULL THEN 0 ELSE 1 END
                + CASE WHEN pr_avg_basket     IS NULL THEN 0 ELSE 1 END
                + CASE WHEN pr_return_pct     IS NULL THEN 0 ELSE 1 END, 0)
        AS decimal(5,4))                    AS overall_score,
    responsible_employee_key,
    CAST(responsible_inferred_int AS bit)   AS is_responsible_inferred,
    CAST(resp_changed_int AS bit)           AS is_responsible_changed,
    months_with_current_responsible
FROM ranked
END

GO