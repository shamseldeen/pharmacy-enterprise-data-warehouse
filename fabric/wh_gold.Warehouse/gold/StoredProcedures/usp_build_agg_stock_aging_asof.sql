CREATE   PROCEDURE gold.usp_build_agg_stock_aging_asof
AS
-- =========================================================
-- Rebuilds gold.agg_stock_aging_asof (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.agg_stock_aging_asof;

CREATE TABLE gold.agg_stock_aging_asof AS
WITH last_snap AS (                 -- latest count per pharmacy
    SELECT pharmacy_key,
           MAX(month_start)            AS snap_month,
           EOMONTH(MAX(month_start))   AS snap_date
    FROM gold.fact_inventory_snapshot
    GROUP BY pharmacy_key
),
global_asof AS (
    SELECT MAX(month_start)            AS asof_month,
           EOMONTH(MAX(month_start))   AS asof_date
    FROM gold.fact_inventory_snapshot
),
stock AS (
    SELECT i.date_key, i.month_start, i.pharmacy_key, i.product_key,
           i.closing_stock, i.stock_value_at_cost_sar, l.snap_date
    FROM gold.fact_inventory_snapshot i
    JOIN last_snap l
      ON l.pharmacy_key = i.pharmacy_key
     AND l.snap_month   = i.month_start
    WHERE i.closing_stock > 0
),
last_sale AS (                      -- last sale up to that pharmacy's count date
    SELECT f.pharmacy_key, f.product_key,
           MAX(CAST(f.transaction_ts AS date)) AS last_sale_date
    FROM gold.fact_sales_line f
    JOIN last_snap l ON l.pharmacy_key = f.pharmacy_key
    WHERE CAST(f.transaction_ts AS date) <= l.snap_date
    GROUP BY f.pharmacy_key, f.product_key
),
chain_sales_180d AS (               -- is there demand anywhere in the chain?
    SELECT f.product_key,
           SUM(CAST(f.quantity AS bigint)) AS chain_qty_180d
    FROM gold.fact_sales_line f
    CROSS JOIN global_asof g
    WHERE CAST(f.transaction_ts AS date) > DATEADD(day, -180, g.asof_date)
    GROUP BY f.product_key
),
demand_cycle AS (                   -- last 4 months = every pharmacy counted once
    SELECT i.pharmacy_key, i.product_key,
           SUM(CAST(i.unfulfilled_qty AS bigint)) AS lost_qty
    FROM gold.fact_inventory_snapshot i
    CROSS JOIN global_asof g
    WHERE i.month_start >= DATEADD(month, -3, g.asof_month)
      AND i.has_lost_demand = 1
    GROUP BY i.pharmacy_key, i.product_key
),
product_demand AS (
    SELECT product_key,
           COUNT(*)      AS pharmacies_lost_demand,
           SUM(lost_qty) AS lost_qty
    FROM demand_cycle
    GROUP BY product_key
),
batch_supplier AS (                 -- main supplier = most batches, tie -> lowest id
    SELECT product_id, supplier_id,
           ROW_NUMBER() OVER (PARTITION BY product_id
                              ORDER BY COUNT(*) DESC, supplier_id) AS rn
    FROM lh_silver.silver.product_batches
    GROUP BY product_id, supplier_id
),
base AS (
    SELECT
        s.date_key,
        s.month_start                                             AS snap_month,
        s.snap_date,
        s.pharmacy_key,
        s.product_key,
        COALESCE(ds.supplier_key, -1)                             AS supplier_key,
        s.closing_stock,
        CAST(s.stock_value_at_cost_sar AS decimal(18,2))          AS stock_value_sar,
        ls.last_sale_date,
        DATEDIFF(day, ls.last_sale_date, s.snap_date)             AS days_since_last_sale,
        COALESCE(cs.chain_qty_180d, 0)                            AS chain_qty_180d,
        COALESCE(pd.pharmacies_lost_demand, 0)
            - CASE WHEN d.pharmacy_key IS NULL THEN 0 ELSE 1 END  AS other_pharmacies_lost_demand,
        COALESCE(pd.lost_qty, 0) - COALESCE(d.lost_qty, 0)        AS other_lost_qty
    FROM stock s
    JOIN gold.dim_product p         ON p.product_key  = s.product_key
    LEFT JOIN batch_supplier bs     ON bs.product_id  = p.product_id AND bs.rn = 1
    LEFT JOIN gold.dim_supplier ds  ON ds.supplier_id = bs.supplier_id
    LEFT JOIN last_sale ls          ON ls.pharmacy_key = s.pharmacy_key AND ls.product_key = s.product_key
    LEFT JOIN chain_sales_180d cs   ON cs.product_key = s.product_key
    LEFT JOIN demand_cycle d        ON d.pharmacy_key = s.pharmacy_key AND d.product_key = s.product_key
    LEFT JOIN product_demand pd     ON pd.product_key = s.product_key
),
flags AS (
    SELECT b.*,
        CASE WHEN b.last_sale_date IS NULL OR b.days_since_last_sale > 180
             THEN 1 ELSE 0 END                                    AS dead_int
    FROM base b
)
SELECT
    f.date_key, f.snap_month, f.snap_date, f.pharmacy_key, f.product_key, f.supplier_key,
    f.closing_stock, f.stock_value_sar, f.last_sale_date, f.days_since_last_sale,
    f.chain_qty_180d, f.other_pharmacies_lost_demand, f.other_lost_qty,
    CASE
        WHEN f.last_sale_date IS NULL      THEN '5_never_sold_here'
        WHEN f.days_since_last_sale <= 90  THEN '1_sold_0_90d'
        WHEN f.days_since_last_sale <= 180 THEN '2_sold_91_180d'
        WHEN f.days_since_last_sale <= 365 THEN '3_sold_181_365d'
        ELSE                                    '4_sold_over_365d'
    END                                                           AS age_bucket,
    CAST(f.dead_int AS bit)                                       AS is_dead_stock,
    CAST(CASE WHEN f.dead_int = 1 AND f.other_pharmacies_lost_demand > 0
              THEN 1 ELSE 0 END AS bit)                           AS is_rotation_candidate,
    CAST(CASE WHEN f.dead_int = 1 AND f.other_pharmacies_lost_demand = 0
                   AND f.chain_qty_180d = 0
              THEN 1 ELSE 0 END AS bit)                           AS is_no_demand_anywhere,
    CASE
        WHEN f.dead_int = 1 AND f.other_pharmacies_lost_demand > 0 THEN '1_ROTATE'
        WHEN f.dead_int = 1                                        THEN '2_SUPPLIER_DEAL'
        WHEN f.days_since_last_sale > 90                           THEN '3_WATCH'
        ELSE                                                            '4_OK'
    END                                                           AS recommended_action
FROM flags f
END

GO