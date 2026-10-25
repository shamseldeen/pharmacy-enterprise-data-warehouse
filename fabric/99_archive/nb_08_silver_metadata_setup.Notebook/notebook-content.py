# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8e17c00e-45b4-42aa-be54-35d1e2ab222d",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "f12cee0f-bc6d-49ed-88bd-125dbae6fa11",
# META       "known_lakehouses": [
# META         {
# META           "id": "8e17c00e-45b4-42aa-be54-35d1e2ab222d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# CELL 01 — CREATE SILVER ENTITY CONFIGURATION TABLE
# ============================================================
# One configuration row per Silver entity.
# Required setup: lh_silver is attached as the default Lakehouse.

entity_config_table = (
    "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`."
    "`metadata`.`silver_entity_config`"
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {entity_config_table} (
    entity_id              STRING NOT NULL,
    domain                 STRING NOT NULL,
    target_lakehouse       STRING NOT NULL,
    target_schema          STRING NOT NULL,
    target_table           STRING NOT NULL,
    grain_description      STRING,
    transformation_pattern STRING,
    handler_name           STRING,
    load_policy            STRING NOT NULL,
    dependency_group       INT NOT NULL,
    is_enabled             BOOLEAN NOT NULL,
    config_status          STRING NOT NULL,
    baseline_status        STRING NOT NULL,
    contract_version       INT NOT NULL,
    created_at             TIMESTAMP NOT NULL,
    updated_at             TIMESTAMP NOT NULL
)
USING DELTA
""")

print("READY: lh_silver.metadata.silver_entity_config")
spark.table(entity_config_table).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 02 — CREATE SILVER SOURCE CONFIGURATION TABLE
# ============================================================
# One row per source used by a Silver entity.
# One entity can have multiple sources: yearly union or enrichment.

source_config_table = (
    "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`."
    "`metadata`.`silver_source_config`"
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {source_config_table} (
    source_id          STRING NOT NULL,
    entity_id          STRING NOT NULL,
    source_workspace   STRING NOT NULL,
    source_lakehouse   STRING NOT NULL,
    source_schema      STRING NOT NULL,
    source_table       STRING NOT NULL,
    source_alias       STRING NOT NULL,
    source_order       INT NOT NULL,
    source_role        STRING NOT NULL,
    is_enabled         BOOLEAN NOT NULL,
    contract_version   INT NOT NULL,
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL
)
USING DELTA
""")

print("READY: lh_silver.metadata.silver_source_config")
spark.table(source_config_table).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 03 — CREATE SILVER COLUMN CONFIGURATION TABLE
# ============================================================
# One row per source-to-target column mapping.
# Complex derived columns are produced by named entity handlers.

column_config_table = (
    "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`."
    "`metadata`.`silver_column_config`"
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {column_config_table} (
    column_config_id   STRING NOT NULL,
    entity_id          STRING NOT NULL,
    source_id          STRING NOT NULL,
    source_column      STRING NOT NULL,
    target_column      STRING NOT NULL,
    target_data_type   STRING NOT NULL,
    column_order       INT NOT NULL,
    trim_whitespace    BOOLEAN NOT NULL,
    empty_to_null      BOOLEAN NOT NULL,
    is_enabled         BOOLEAN NOT NULL,
    contract_version   INT NOT NULL,
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL
)
USING DELTA
""")

print("READY: lh_silver.metadata.silver_column_config")
spark.table(column_config_table).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 04 — CREATE SILVER RULE CONFIGURATION TABLE
# ============================================================
# One row per declared validation rule for a Silver entity.
# Self-contained: safe to run after a Spark session timeout.

rule_config_table = (
    "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`."
    "`metadata`.`silver_rule_config`"
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {rule_config_table} (
    rule_id              STRING NOT NULL,
    entity_id            STRING NOT NULL,
    rule_name            STRING NOT NULL,
    rule_type            STRING NOT NULL,
    validation_stage     STRING NOT NULL,
    severity             STRING NOT NULL,
    failure_action       STRING NOT NULL,
    rule_parameters_json STRING NOT NULL,
    decision_reference   STRING,
    is_enabled           BOOLEAN NOT NULL,
    contract_version     INT NOT NULL,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
)
USING DELTA
""")

print("READY: lh_silver.metadata.silver_rule_config")
spark.table(rule_config_table).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 05 — CREATE SILVER RUN LOG TABLE
# ============================================================
# One row per execution attempt for one Silver entity.
# Self-contained: does not depend on earlier cell variables.

run_log_table = (
    "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`."
    "`metadata`.`silver_run_log`"
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {run_log_table} (
    log_id                  STRING NOT NULL,
    run_id                  STRING NOT NULL,
    entity_id               STRING NOT NULL,
    attempt_number          INT NOT NULL,
    contract_version        INT NOT NULL,
    target_table            STRING NOT NULL,
    run_status              STRING NOT NULL,
    started_at              TIMESTAMP NOT NULL,
    finished_at             TIMESTAMP,
    duration_seconds        DOUBLE,
    source_row_count        BIGINT,
    stage_row_count         BIGINT,
    target_row_count        BIGINT,
    rows_written            BIGINT,
    rules_evaluated         INT,
    rules_failed            INT,
    validation_summary_json STRING,
    source_versions_json    STRING,
    target_delta_version    BIGINT,
    error_message           STRING,
    updated_at              TIMESTAMP NOT NULL
)
USING DELTA
""")

print("READY: lh_silver.metadata.silver_run_log")
spark.table(run_log_table).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 06 — REGISTER THE TEN COMPLETED SILVER ENTITIES
# ============================================================
# Insert missing metadata rows only; preserve existing settings.
# This cell does not read or rebuild business tables.

entity_config_table = (
    "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`."
    "`metadata`.`silver_entity_config`"
)

spark.sql(f"""
MERGE INTO {entity_config_table} AS target
USING (
    SELECT
        seed.target_table AS entity_id,
        seed.domain,
        'lh_silver' AS target_lakehouse,
        'silver' AS target_schema,
        seed.target_table,
        seed.grain_description,
        seed.transformation_pattern,
        CAST(NULL AS STRING) AS handler_name,
        'REUSE_EXISTING' AS load_policy,
        0 AS dependency_group,
        FALSE AS is_enabled,
        'DRAFT' AS config_status,
        'EXISTING_VERIFIED' AS baseline_status,
        1 AS contract_version,
        current_timestamp() AS created_at,
        current_timestamp() AS updated_at
    FROM VALUES
        ('erp_branch_lifecycle_events', 'erp', 'One row per branch lifecycle event', 'DIRECT'),
        ('erp_employees', 'erp', 'One row per employee', 'DIRECT'),
        ('erp_job_roles', 'erp', 'One row per job role', 'DIRECT'),
        ('erp_pharmacies', 'erp', 'One row per pharmacy', 'DIRECT'),
        ('product_batches', 'supply_chain', 'One row per product batch', 'DIRECT'),
        ('products', 'reference', 'One row per product', 'DIRECT'),
        ('rx_dispenses', 'rx', 'One row per dispense event', 'JOIN_ENRICHMENT'),
        ('rx_doctors', 'rx', 'One row per doctor', 'DIRECT'),
        ('rx_prescription_items', 'rx', 'One row per prescription item', 'DIRECT'),
        ('rx_prescriptions', 'rx', 'One row per prescription header', 'DIRECT')
    AS seed(target_table, domain, grain_description, transformation_pattern)
) AS source
ON target.entity_id = source.entity_id
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
SELECT
    entity_id,
    baseline_status,
    load_policy,
    is_enabled,
    config_status
FROM {entity_config_table}
WHERE baseline_status = 'EXISTING_VERIFIED'
ORDER BY entity_id
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 07 — REGISTER THE REMAINING SILVER CANDIDATES
# ============================================================
# Insert draft candidates from the audited source ledger.
# Business grains and dependency groups await contract approval.

entity_config_table = (
    "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`."
    "`metadata`.`silver_entity_config`"
)

spark.sql(f"""
MERGE INTO {entity_config_table} AS target
USING (
    SELECT
        seed.entity_id,
        seed.domain,
        'lh_silver' AS target_lakehouse,
        'silver' AS target_schema,
        seed.entity_id AS target_table,
        CAST(NULL AS STRING) AS grain_description,
        CASE
            WHEN seed.entity_id IN (
                'pos_canonical_sales_lines',
                'pos_payments',
                'pos_sales_transactions'
            ) THEN 'YEARLY_UNION'
            ELSE CAST(NULL AS STRING)
        END AS transformation_pattern,
        CAST(NULL AS STRING) AS handler_name,
        'CREATE_ONLY' AS load_policy,
        -1 AS dependency_group,
        FALSE AS is_enabled,
        'DRAFT' AS config_status,
        'PENDING' AS baseline_status,
        1 AS contract_version,
        current_timestamp() AS created_at,
        current_timestamp() AS updated_at
    FROM VALUES
        ('crm_customer_consent_history', 'crm'),
        ('crm_customer_service_interactions', 'crm'),
        ('crm_customer_service_reference', 'crm'),
        ('crm_customers', 'crm'),
        ('crm_loyalty_accounts', 'crm'),
        ('delivery_deliveries', 'delivery'),
        ('delivery_delivery_methods', 'delivery'),
        ('erp_employee_assignments', 'erp'),
        ('erp_employee_monthly_timekeeping', 'erp'),
        ('erp_employee_shift_rosters', 'erp'),
        ('erp_operating_hour_profiles', 'erp'),
        ('erp_pharmacy_operating_hours', 'erp'),
        ('erp_shift_types', 'erp'),
        ('finance_branch_expense_categories', 'finance'),
        ('finance_branch_monthly_expenses', 'finance'),
        ('finance_vat_rate_timeline', 'finance'),
        ('insurance_claim_rejection_reasons', 'insurance'),
        ('insurance_claim_statuses', 'insurance'),
        ('insurance_customer_membership', 'insurance'),
        ('insurance_eligibility_history', 'insurance'),
        ('insurance_claim_lines', 'insurance'),
        ('insurance_claims', 'insurance'),
        ('insurance_plans', 'insurance'),
        ('inventory_cycle_counts', 'inventory'),
        ('inventory_monthly_snapshot', 'inventory'),
        ('inventory_movements', 'inventory'),
        ('inventory_stock_transfers', 'inventory'),
        ('marketing_campaigns', 'marketing'),
        ('marketing_channels', 'marketing'),
        ('marketing_promotion_types', 'marketing'),
        ('marketing_promotions', 'marketing'),
        ('pos_canonical_sales_lines', 'pos'),
        ('pos_payment_methods', 'pos'),
        ('pos_payments', 'pos'),
        ('pos_return_reasons', 'pos'),
        ('pos_returns', 'pos'),
        ('pos_sales_transactions', 'pos'),
        ('reference_cities', 'reference'),
        ('reference_regions', 'reference'),
        ('rx_doctor_specialties', 'rx'),
        ('supply_chain_purchase_order_lines', 'supply_chain'),
        ('supply_chain_purchase_orders', 'supply_chain'),
        ('supply_chain_purchase_receipts', 'supply_chain'),
        ('supply_chain_supplier_contracts', 'supply_chain'),
        ('supply_chain_supplier_types', 'supply_chain'),
        ('supply_chain_suppliers', 'supply_chain'),
        ('supply_chain_warehouses', 'supply_chain')
    AS seed(entity_id, domain)
) AS source
ON target.entity_id = source.entity_id
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
SELECT
    baseline_status,
    config_status,
    is_enabled,
    COUNT(*) AS entities
FROM {entity_config_table}
GROUP BY baseline_status, config_status, is_enabled
ORDER BY baseline_status, config_status, is_enabled
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 08 — REGISTER BRONZE SOURCES FOR ALL SILVER CANDIDATES
# ============================================================
# Reuse the audited manifest; read/write metadata only.

metadata_root = "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`.`metadata`"
entity_config_table = f"{metadata_root}.`silver_entity_config`"
source_config_table = f"{metadata_root}.`silver_source_config`"

same_name_entities = """
crm_customer_consent_history crm_customer_service_interactions
crm_customer_service_reference crm_customers
crm_loyalty_accounts delivery_deliveries
delivery_delivery_methods erp_branch_lifecycle_events
erp_employee_assignments erp_employee_monthly_timekeeping
erp_employee_shift_rosters erp_employees
erp_job_roles erp_operating_hour_profiles
erp_pharmacies erp_pharmacy_operating_hours
erp_shift_types finance_branch_expense_categories
finance_branch_monthly_expenses finance_vat_rate_timeline
insurance_claim_rejection_reasons insurance_claim_statuses
insurance_customer_membership insurance_eligibility_history
insurance_plans marketing_campaigns
marketing_channels marketing_promotion_types
marketing_promotions pos_payment_methods
pos_return_reasons reference_cities
reference_regions rx_doctor_specialties
rx_doctors supply_chain_supplier_contracts
supply_chain_supplier_types supply_chain_suppliers
supply_chain_warehouses
""".split()

source_overrides = {
    "insurance_claim_lines": "insurance_insurance_claim_lines_final",
    "insurance_claims": "insurance_insurance_claims_fixed",
    "inventory_cycle_counts": "inventory_inventory_cycle_counts_2025_final",
    "inventory_monthly_snapshot": "inventory_inventory_monthly_snapshot_2025_final",
    "inventory_movements": "inventory_inventory_movements_final_v2",
    "inventory_stock_transfers": "inventory_stock_transfers_final",
    "pos_returns": "pos_returns_final",
    "product_batches": "supply_chain_product_batches_final",
    "products": "reference_products_final_11274",
    "rx_dispenses": "rx_prescription_dispenses_fixed",
    "rx_prescription_items": "rx_prescription_items_final",
    "rx_prescriptions": "rx_prescriptions_fixed",
    "supply_chain_purchase_order_lines": "supply_chain_purchase_order_lines_final",
    "supply_chain_purchase_orders": "supply_chain_purchase_orders_fixed",
    "supply_chain_purchase_receipts": "supply_chain_purchase_receipts_fixed",
}

source_groups = {
    entity: [entity]
    for entity in same_name_entities
}

source_groups.update({
    entity: [table]
    for entity, table in source_overrides.items()
})

# The three audited yearly families contain 2020 through 2025.
for entity in (
    "pos_canonical_sales_lines",
    "pos_payments",
    "pos_sales_transactions"
):
    source_groups[entity] = [
        f"{entity}_{year}"
        + (
            "_final"
            if entity == "pos_canonical_sales_lines" and year == 2025
            else ""
        )
        for year in range(2020, 2026)
    ]

# Check registry references before writing source configuration.
registered_entities = {
    row.entity_id
    for row in spark.table(entity_config_table)
        .select("entity_id")
        .collect()
}

if registered_entities != set(source_groups):
    raise ValueError(
        "Registry differs from the audited entity manifest. "
        f"Missing: {sorted(set(source_groups) - registered_entities)}; "
        f"Unexpected: {sorted(registered_entities - set(source_groups))}"
    )

source_rows = []

for entity, tables in source_groups.items():
    for position, table in enumerate(tables, start=1):
        alias = "src" if len(tables) == 1 else f"y{2019 + position}"
        role = "PRIMARY" if len(tables) == 1 else "UNION_MEMBER"

        source_rows.append((
            f"{entity}:{alias}",
            entity,
            table,
            alias,
            position,
            role
        ))

# Existing RX dispense enrichment also uses items and headers.
source_rows.extend([
    (
        "rx_dispenses:items",
        "rx_dispenses",
        "rx_prescription_items_final",
        "items",
        2,
        "ENRICHMENT"
    ),
    (
        "rx_dispenses:rx",
        "rx_dispenses",
        "rx_prescriptions_fixed",
        "rx",
        3,
        "ENRICHMENT"
    ),
])

spark.createDataFrame(
    source_rows,
    "source_id STRING, entity_id STRING, source_table STRING, "
    "source_alias STRING, source_order INT, source_role STRING"
).createOrReplaceTempView("_silver_source_seed")

spark.sql(f"""
MERGE INTO {source_config_table} AS target
USING (
    SELECT
        source_id,
        entity_id,
        'Pharmacy-Enterprise-DW-DEV' AS source_workspace,
        'lh_bronze' AS source_lakehouse,
        'bronze' AS source_schema,
        source_table,
        source_alias,
        source_order,
        source_role,
        FALSE AS is_enabled,
        1 AS contract_version,
        current_timestamp() AS created_at,
        current_timestamp() AS updated_at
    FROM _silver_source_seed
) AS source
ON target.source_id = source.source_id
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
SELECT
    COUNT(*) AS source_attachments,
    COUNT(DISTINCT source_table) AS bronze_tables,
    COUNT(DISTINCT entity_id) AS silver_entities
FROM {source_config_table}
WHERE source_workspace = 'Pharmacy-Enterprise-DW-DEV'
  AND source_lakehouse = 'lh_bronze'
  AND source_schema = 'bronze'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 09 — CONFIGURE THE FIRST SILVER PILOT CONTRACT
# ============================================================
# This declares expectations; runtime validation is still pending.

import json
from pyspark.sql import functions as F

metadata_root = "`Pharmacy-Enterprise-DW-DEV`.`lh_silver`.`metadata`"
entity_id = "reference_regions"
source_id = "reference_regions:src"

# Confirm the declared primary source using metadata only.
pilot_sources = (
    spark.table(f"{metadata_root}.`silver_source_config`")
        .filter(F.col("entity_id") == entity_id)
        .collect()
)

if (
    len(pilot_sources) != 1
    or pilot_sources[0].source_id != source_id
    or pilot_sources[0].source_table != "reference_regions"
    or pilot_sources[0].source_role != "PRIMARY"
):
    raise ValueError("Pilot source configuration needs review.")


# Reusable insert-only helper for column and rule contracts.
def register_contract_rows(
    contract_entity_id, table_name, key_column, rows, schema
):
    seed = (
        spark.createDataFrame(rows, schema)
            .withColumn("entity_id", F.lit(contract_entity_id))
            .withColumn("is_enabled", F.lit(True))
            .withColumn("contract_version", F.lit(1))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
    )

    seed.createOrReplaceTempView("_silver_contract_seed")

    spark.sql(f"""
        MERGE INTO {metadata_root}.`{table_name}` AS target
        USING _silver_contract_seed AS source
        ON target.{key_column} = source.{key_column}
        WHEN NOT MATCHED THEN INSERT *
    """)


columns = ["region_id", "region_name", "country_name"]

column_rows = [
    (
        f"{source_id}:{name}",
        source_id,
        name,
        name,
        "string",
        position,
        True,
        True
    )
    for position, name in enumerate(columns, start=1)
]

register_contract_rows(
    entity_id,
    "silver_column_config",
    "column_config_id",
    column_rows,
    "column_config_id STRING, source_id STRING, source_column STRING, "
    "target_column STRING, target_data_type STRING, column_order INT, "
    "trim_whitespace BOOLEAN, empty_to_null BOOLEAN"
)

rule_rows = [
    (
        f"{entity_id}:pk",
        "Region primary key",
        "PRIMARY_KEY",
        "PRE_PUBLISH",
        "ERROR",
        "BLOCK",
        json.dumps({"columns": ["region_id"]}),
        "SIL-CONTRACT-REF-REGIONS-001"
    ),
    (
        f"{entity_id}:required",
        "Required region attributes",
        "NOT_NULL",
        "PRE_PUBLISH",
        "ERROR",
        "BLOCK",
        json.dumps({"columns": ["region_name", "country_name"]}),
        "SIL-CONTRACT-REF-REGIONS-001"
    ),
]

register_contract_rows(
    entity_id,
    "silver_rule_config",
    "rule_id",
    rule_rows,
    "rule_id STRING, rule_name STRING, rule_type STRING, "
    "validation_stage STRING, severity STRING, failure_action STRING, "
    "rule_parameters_json STRING, decision_reference STRING"
)

# Activate the source, then approve the entity contract last.
spark.sql(f"""
UPDATE {metadata_root}.`silver_source_config`
SET is_enabled = TRUE,
    updated_at = current_timestamp()
WHERE source_id = '{source_id}'
  AND contract_version = 1
""")

spark.sql(f"""
UPDATE {metadata_root}.`silver_entity_config`
SET grain_description = 'One row per region',
    transformation_pattern = 'DIRECT',
    dependency_group = 1,
    config_status = 'APPROVED',
    is_enabled = TRUE,
    updated_at = current_timestamp()
WHERE entity_id = '{entity_id}'
  AND baseline_status = 'PENDING'
  AND load_policy = 'CREATE_ONLY'
  AND config_status = 'DRAFT'
  AND contract_version = 1
""")

spark.sql(f"""
SELECT
    e.entity_id,
    e.config_status,
    e.is_enabled,
    (SELECT COUNT(*)
     FROM {metadata_root}.`silver_column_config`
     WHERE entity_id = '{entity_id}'
       AND is_enabled = TRUE) AS columns_configured,
    (SELECT COUNT(*)
     FROM {metadata_root}.`silver_rule_config`
     WHERE entity_id = '{entity_id}'
       AND is_enabled = TRUE) AS rules_configured
FROM {metadata_root}.`silver_entity_config` AS e
WHERE e.entity_id = '{entity_id}'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
