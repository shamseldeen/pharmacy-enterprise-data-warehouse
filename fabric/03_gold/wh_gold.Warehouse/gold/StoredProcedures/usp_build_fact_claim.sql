CREATE   PROCEDURE gold.usp_build_fact_claim
AS
-- =========================================================
-- Rebuilds gold.fact_claim (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.fact_claim;

CREATE TABLE gold.fact_claim AS
SELECT
    cl.claim_id,
    YEAR(cl.submitted_ts) * 10000 + MONTH(cl.submitted_ts) * 100 + DAY(cl.submitted_ts) AS submitted_date_key,
    cl.submitted_ts,
    cl.adjudicated_ts,
    DATEDIFF(hour, cl.submitted_ts, cl.adjudicated_ts)               AS hours_to_adjudicate,
    COALESCE(ph.pharmacy_key, -1)                                    AS pharmacy_key,
    COALESCE(c.customer_key,  -1)                                    AS customer_key,
    COALESCE(pl.plan_key,     -1)                                    AS plan_key,
    cl.prescription_id,
    cl.linked_transaction_id,
    st.claim_status_name,
    rr.rejection_reason_name,
    cl.claimed_amount_sar,
    cl.approved_amount_sar,
    cl.rejected_amount_sar,
    cl.customer_copay_sar,
    CAST(CASE WHEN cl.rejected_amount_sar > 0 AND cl.approved_amount_sar = 0 THEN 1 ELSE 0 END AS bit) AS is_fully_rejected,
    CAST(CASE WHEN cl.rejected_amount_sar > 0 AND cl.approved_amount_sar > 0 THEN 1 ELSE 0 END AS bit) AS is_partially_rejected
FROM lh_silver.silver.insurance_claims cl
LEFT JOIN lh_silver.silver.insurance_claim_statuses          st ON st.claim_status_id     = cl.claim_status_id
LEFT JOIN lh_silver.silver.insurance_claim_rejection_reasons rr ON rr.rejection_reason_id = cl.rejection_reason_id
LEFT JOIN gold.dim_pharmacy       ph ON ph.pharmacy_id       = cl.pharmacy_id
LEFT JOIN gold.dim_customer       c  ON c.customer_id        = cl.customer_id
LEFT JOIN gold.dim_insurance_plan pl ON pl.insurance_plan_id = cl.insurance_plan_id
END

GO