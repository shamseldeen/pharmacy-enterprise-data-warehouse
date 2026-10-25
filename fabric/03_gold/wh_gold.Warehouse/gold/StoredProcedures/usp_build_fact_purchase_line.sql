CREATE   PROCEDURE gold.usp_build_fact_purchase_line
AS
-- =========================================================
-- Rebuilds gold.fact_purchase_line (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_purchase_line;

CREATE TABLE gold.fact_purchase_line AS
SELECT
    l.po_line_id,
    l.purchase_order_id,
    YEAR(po.order_date) * 10000 + MONTH(po.order_date) * 100 + DAY(po.order_date) AS order_date_key,
    po.order_date,
    po.expected_delivery_date,
    r.receipt_date,
    po.order_status,
    COALESCE(s.supplier_key, -1)                                  AS supplier_key,
    COALESCE(p.product_key,  -1)                                  AS product_key,
    COALESCE(e.employee_key, -1)                                  AS buyer_employee_key,
    po.warehouse_id,
    w.warehouse_name,
    w.region_name                                                 AS warehouse_region,
    -- ordered
    l.ordered_qty,
    l.unit_cost_sar,
    l.gross_cost_sar,
    l.discount_sar,
    l.tax_sar,
    l.net_cost_sar,
    -- received
    COALESCE(r.received_qty, 0)                                   AS received_qty,
    COALESCE(r.rejected_qty, 0)                                   AS rejected_qty,
    COALESCE(r.received_qty, 0) - COALESCE(r.rejected_qty, 0)     AS accepted_qty,
    CAST(CASE WHEN r.receipt_id IS NOT NULL THEN 1 ELSE 0 END AS bit) AS is_received,
    COALESCE(r.is_on_time, CAST(0 AS bit))                        AS is_on_time,
    COALESCE(r.is_in_full, CAST(0 AS bit))                        AS is_in_full,
    CAST(CASE WHEN r.is_on_time = 1 AND r.is_in_full = 1 THEN 1 ELSE 0 END AS bit) AS is_otif,
    DATEDIFF(day, po.order_date, r.receipt_date)                  AS lead_time_days,
    DATEDIFF(day, po.expected_delivery_date, r.receipt_date)      AS delay_days      -- >0 = late
FROM lh_silver.silver.supply_chain_purchase_order_lines l
JOIN lh_silver.silver.supply_chain_purchase_orders      po ON po.purchase_order_id = l.purchase_order_id
LEFT JOIN lh_silver.silver.supply_chain_purchase_receipts r ON r.po_line_id       = l.po_line_id
LEFT JOIN lh_silver.silver.supply_chain_warehouses        w ON w.warehouse_id      = po.warehouse_id
LEFT JOIN gold.dim_supplier s ON s.supplier_id = po.supplier_id
LEFT JOIN gold.dim_product  p ON p.product_id  = l.product_id
LEFT JOIN gold.dim_employee e ON e.employee_id = po.buyer_employee_id
END

GO