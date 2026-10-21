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
# META         },
# META         {
# META           "id": "c353d594-1dea-4854-bbee-ea4e6fe22064"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

source_df = spark.sql("""
SELECT
    batch_id,
    product_id,
    supplier_id,
    manufacture_date,
    expiry_date,
    unit_cost_sar,
    received_date,
    temperature_class,
    recall_flag
FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`supply_chain_product_batches_final`
""")

silver_product_batches_stage_df = source_df.select(
    "batch_id",
    "product_id",
    "supplier_id",
    "manufacture_date",
    "expiry_date",
    F.col("unit_cost_sar")
        .cast("decimal(18,2)")
        .alias("unit_cost_sar"),
    "received_date",
    "temperature_class",
    F.col("recall_flag")
        .cast("boolean")
        .alias("is_recalled")
)

print("Prepared Silver columns:", len(silver_product_batches_stage_df.columns))
silver_product_batches_stage_df.printSchema()
display(silver_product_batches_stage_df.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

target_table = "silver.product_batches"

(
    silver_product_batches_stage_df
        .write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(target_table)
)

print("Created Silver table:", target_table)

silver_product_batches_df = spark.table(target_table)

metrics = silver_product_batches_df.agg(
    F.count("*").alias("silver_rows"),
    F.countDistinct("batch_id").alias("distinct_batch_ids"),
    F.sum(
        F.when(
            F.col("batch_id").isNull()
            | (F.trim(F.col("batch_id")) == ""),
            1
        ).otherwise(0)
    ).alias("invalid_batch_ids"),
    F.sum(
        F.when(F.col("unit_cost_sar").isNull(), 1).otherwise(0)
    ).alias("null_unit_costs"),
    F.sum(
        F.when(F.col("is_recalled").isNull(), 1).otherwise(0)
    ).alias("null_recall_flags")
).first()

schema_types = {
    field.name: field.dataType.simpleString()
    for field in silver_product_batches_df.schema.fields
}

passed = (
    metrics["silver_rows"] == 40000
    and metrics["distinct_batch_ids"] == 40000
    and (metrics["invalid_batch_ids"] or 0) == 0
    and (metrics["null_unit_costs"] or 0) == 0
    and (metrics["null_recall_flags"] or 0) == 0
    and schema_types.get("unit_cost_sar") == "decimal(18,2)"
    and schema_types.get("is_recalled") == "boolean"
)

print("Silver rows:", metrics["silver_rows"])
print("Distinct batch IDs:", metrics["distinct_batch_ids"])
print("Invalid batch IDs:", metrics["invalid_batch_ids"])
print("NULL unit costs:", metrics["null_unit_costs"])
print("NULL recall flags:", metrics["null_recall_flags"])
print("unit_cost_sar type:", schema_types.get("unit_cost_sar"))
print("is_recalled type:", schema_types.get("is_recalled"))
print("STATUS:", "PASS" if passed else "REVIEW")

if not passed:
    raise AssertionError("Silver product batches require review.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_df = spark.table("silver.product_batches")

metrics = silver_df.agg(
    F.count("*").alias("rows"),
    F.countDistinct("batch_id").alias("distinct_batch_ids"),
    F.sum(
        F.when(
            F.col("batch_id").isNull() |
            (F.trim(F.col("batch_id")) == ""),
            1
        ).otherwise(0)
    ).alias("invalid_batch_ids"),
    F.sum(
        F.when(F.col("unit_cost_sar").isNull(), 1).otherwise(0)
    ).alias("null_unit_costs"),
    F.sum(
        F.when(F.col("is_recalled").isNull(), 1).otherwise(0)
    ).alias("null_recall_flags")
).first()

print("Rows:", metrics["rows"])
print("Distinct batch IDs:", metrics["distinct_batch_ids"])
print("Invalid batch IDs:", metrics["invalid_batch_ids"])
print("NULL unit costs:", metrics["null_unit_costs"])
print("NULL recall flags:", metrics["null_recall_flags"])
print("Columns:", silver_df.columns)

silver_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Silver implementation status — VERIFIED
# 
# - Target: `lh_silver.silver.product_batches`
# - Source: `bronze.supply_chain_product_batches_final`
# - Status: Existing Silver table verified; no rewrite performed.
# - Validation in `nb_07`, Cell 03: 40,000 rows; 40,000 distinct batch IDs; invalid batch IDs: 0; NULL unit costs: 0; NULL recall flags: 0.
# - Schema: 9 columns; `unit_cost_sar` is `Decimal(18,2)`; `is_recalled` is `Boolean`.
# - The Cell 02 write attempt stopped because the target table already existed and `errorifexists` prevented replacement. Do not drop or overwrite it.
