CREATE   PROCEDURE gold.usp_build_dim_customer
AS
-- =========================================================
-- Rebuilds gold.dim_customer (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.dim_customer;

CREATE TABLE gold.dim_customer AS
WITH consent AS (       -- pivot current consent; missing row = NO consent (opt-in rule)
    SELECT customer_id,
           MAX(CASE WHEN consent_type = 'MARKETING_SMS'    AND is_consented = 1 THEN 1 ELSE 0 END) AS sms_ok,
           MAX(CASE WHEN consent_type = 'MARKETING_EMAIL'  AND is_consented = 1 THEN 1 ELSE 0 END) AS email_ok,
           MAX(CASE WHEN consent_type = 'APP_NOTIFICATION' AND is_consented = 1 THEN 1 ELSE 0 END) AS app_ok
    FROM lh_silver.silver.crm_customer_consent_current
    GROUP BY customer_id
),
loyalty AS (
    SELECT customer_id, tier_at_enrollment, account_status
    FROM lh_silver.silver.crm_loyalty_accounts
),
insured_ever AS (       -- ever had an insurance membership
    SELECT DISTINCT customer_id
    FROM lh_silver.silver.insurance_customer_membership
),
insured_now AS (        -- coverage period includes the as-of date
    SELECT DISTINCT customer_id
    FROM lh_silver.silver.insurance_eligibility_history
    WHERE CAST('2025-12-31' AS date) BETWEEN coverage_start_date
                                         AND COALESCE(coverage_end_date, CAST('9999-12-31' AS date))
),
base AS (
    SELECT c.*,
           DATEDIFF(day, c.birth_date,        CAST('2025-12-31' AS date)) / 365.25 AS age_years,
           DATEDIFF(day, c.registration_date, CAST('2025-12-31' AS date)) / 365.25 AS tenure_years
    FROM lh_silver.silver.crm_customers c
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY b.customer_id) AS int) AS customer_key,
    b.customer_id,
    b.full_name,
    b.gender,
    b.nationality_group,
    b.region_name,
    b.city_name,
    b.birth_date,
    CAST(FLOOR(b.age_years) AS int)                          AS age_at_asof,
    CASE WHEN b.age_years < 18 THEN '0-17'
         WHEN b.age_years < 30 THEN '18-29'
         WHEN b.age_years < 45 THEN '30-44'
         WHEN b.age_years < 60 THEN '45-59'
         ELSE '60+' END                                      AS age_band,
    b.registration_date,
    CAST(FLOOR(b.tenure_years) AS int)                       AS tenure_years,
    b.preferred_contact_channel,
    CAST(b.is_loyalty_enrolled AS bit)                       AS is_loyalty_enrolled,
    l.tier_at_enrollment                                     AS loyalty_tier,
    l.account_status                                         AS loyalty_status,
    CAST(CASE WHEN ie.customer_id IS NOT NULL THEN 1 ELSE 0 END AS bit) AS has_insurance_ever,
    CAST(CASE WHEN inow.customer_id IS NOT NULL THEN 1 ELSE 0 END AS bit) AS is_insured_at_asof,
    CAST(COALESCE(cs.sms_ok,   0) AS bit)                    AS can_contact_sms,
    CAST(COALESCE(cs.email_ok, 0) AS bit)                    AS can_contact_email,
    CAST(COALESCE(cs.app_ok,   0) AS bit)                    AS can_contact_app,
    -- Marketing list rule: adult (18+) AND consented on at least one channel
    CAST(CASE WHEN b.age_years >= 18
               AND (COALESCE(cs.sms_ok,0) = 1 OR COALESCE(cs.email_ok,0) = 1 OR COALESCE(cs.app_ok,0) = 1)
              THEN 1 ELSE 0 END AS bit)                      AS is_marketable
FROM base b
LEFT JOIN loyalty      l    ON l.customer_id    = b.customer_id
LEFT JOIN insured_ever ie   ON ie.customer_id   = b.customer_id
LEFT JOIN insured_now  inow ON inow.customer_id = b.customer_id
LEFT JOIN consent      cs   ON cs.customer_id   = b.customer_id

UNION ALL

SELECT -1, 'UNKNOWN', 'Unknown customer', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
       NULL, NULL, 'Unknown', NULL, NULL, NULL, CAST(0 AS bit), NULL, NULL,
       CAST(0 AS bit), CAST(0 AS bit), CAST(0 AS bit), CAST(0 AS bit), CAST(0 AS bit), CAST(0 AS bit)
END

GO