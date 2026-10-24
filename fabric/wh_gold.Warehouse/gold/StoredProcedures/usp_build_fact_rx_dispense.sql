CREATE   PROCEDURE gold.usp_build_fact_rx_dispense
AS
-- =========================================================
-- Rebuilds gold.fact_rx_dispense (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_rx_dispense;

CREATE TABLE gold.fact_rx_dispense AS
SELECT
    d.dispense_id,
    d.prescription_id,
    d.prescription_item_id,
    YEAR(d.dispense_ts) * 10000 + MONTH(d.dispense_ts) * 100 + DAY(d.dispense_ts) AS dispense_date_key,
    d.prescribed_ts,
    d.dispense_ts,
    DATEDIFF(minute, d.prescribed_ts, d.dispense_ts)               AS minutes_to_dispense,
    COALESCE(ph.pharmacy_key, -1)                                  AS pharmacy_key,
    COALESCE(c.customer_key,  -1)                                  AS customer_key,
    COALESCE(dr.doctor_key,   -1)                                  AS doctor_key,
    COALESCE(p.product_key,   -1)                                  AS product_key,
    COALESCE(e.employee_key,  -1)                                  AS dispensing_employee_key,
    rx.prescription_source,
    d.prescribed_quantity,
    d.dispensed_quantity,
    CAST(CASE WHEN d.dispensed_quantity < d.prescribed_quantity THEN 1 ELSE 0 END AS bit) AS is_partial_fill,
    d.dosage_frequency,
    d.duration_days,
    d.authorized_refills,
    d.route,
    COALESCE(e.is_pharmacist, CAST(0 AS bit))                      AS is_dispensed_by_pharmacist,
    COALESCE(e.can_dispense,  CAST(0 AS bit))                      AS is_authorized_dispenser,
    e.job_role                                                     AS dispenser_role,
    d.dispense_linked_transaction_id
FROM lh_silver.silver.rx_dispenses d
LEFT JOIN lh_silver.silver.rx_prescriptions rx ON rx.prescription_id = d.prescription_id
LEFT JOIN gold.dim_pharmacy ph ON ph.pharmacy_id = d.pharmacy_id
LEFT JOIN gold.dim_customer c  ON c.customer_id  = d.customer_id
LEFT JOIN gold.dim_doctor   dr ON dr.doctor_id   = d.doctor_id
LEFT JOIN gold.dim_product  p  ON p.product_id   = d.product_id
LEFT JOIN gold.dim_employee e  ON e.employee_id  = d.dispensing_pharmacist_employee_id
END

GO