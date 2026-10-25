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

# CELL 01 — PRODUCT SOURCE READ SMOKE TEST

source_df = spark.sql("""
SELECT
    product_id,
    trade_name,
    generic_name,
    strength,
    strength_unit,
    dosage_form,
    category,
    list_price_sar,
    source_type
FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`reference_products_final_11274`
""")

print("Columns discovered:", len(source_df.columns))
source_df.printSchema()
display(source_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 02 — LIST PRICE PRECISION DISCOVERY

from pyspark.sql import functions as F

price = F.col("list_price_sar")
price_in_halalas = price * F.lit(100.0)

price_rule_profile = source_df.agg(
    F.sum(
        F.when(price < 0, 1).otherwise(0)
    ).alias("negative_price_rows"),

    F.sum(
        F.when(price == 0, 1).otherwise(0)
    ).alias("zero_price_rows"),

    F.sum(
        F.when(
            price.isNotNull()
            & (
                F.abs(
                    price_in_halalas - F.round(price_in_halalas, 0)
                ) > F.lit(1e-6)
            ),
            1
        ).otherwise(0)
    ).alias("prices_with_more_than_2_decimals")
)

display(price_rule_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 03 — BUILD SILVER PRODUCT STAGE (NO WRITE)

from pyspark.sql import functions as F

def normalize_text(column_name):
    trimmed_value = F.trim(F.col(column_name))

    return (
        F.when(
            trimmed_value == "",
            F.lit(None).cast("string")
        )
        .otherwise(trimmed_value)
        .alias(column_name)
    )


price_cast_failures = source_df.filter(
    F.col("list_price_sar").isNotNull()
    & F.col("list_price_sar").cast("decimal(18,2)").isNull()
).count()

print("Price cast failures:", price_cast_failures)

if price_cast_failures > 0:
    raise ValueError(
        "Price conversion failed. Stop before writing to Silver."
    )


silver_products_stage_df = source_df.select(
    normalize_text("product_id"),
    normalize_text("trade_name"),
    normalize_text("generic_name"),
    normalize_text("strength"),
    normalize_text("strength_unit"),
    normalize_text("dosage_form"),
    normalize_text("category"),
    F.col("list_price_sar")
        .cast("decimal(18,2)")
        .alias("list_price_sar"),
    normalize_text("source_type")
)

print("Stage columns:", len(silver_products_stage_df.columns))
silver_products_stage_df.printSchema()
display(silver_products_stage_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELL 04 — WRITE AND VERIFY SILVER PRODUCTS

from pyspark.sql import functions as F

target_table = "silver.products"

existing_tables = {
    row["tableName"].lower()
    for row in spark.sql("SHOW TABLES IN silver").collect()
}

if "products" in existing_tables:
    raise ValueError(
        "silver.products already exists. Stop; do not overwrite it."
    )

(
    silver_products_stage_df
        .write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(target_table)
)

print("Created Silver table:", target_table)

silver_products_df = spark.table(target_table)

metrics = silver_products_df.agg(
    F.count("*").alias("silver_rows"),
    F.countDistinct("product_id").alias("distinct_product_ids"),
    F.sum(
        F.when(
            F.col("product_id").isNull()
            | (F.trim(F.col("product_id")) == ""),
            1
        ).otherwise(0)
    ).alias("invalid_product_ids")
).first()

silver_rows = metrics["silver_rows"]
distinct_ids = metrics["distinct_product_ids"]
invalid_ids = metrics["invalid_product_ids"] or 0
duplicate_extra_rows = silver_rows - distinct_ids

price_type = next(
    field.dataType.simpleString()
    for field in silver_products_df.schema.fields
    if field.name == "list_price_sar"
)

passed = (
    silver_rows == 11274
    and distinct_ids == 11274
    and invalid_ids == 0
    and duplicate_extra_rows == 0
    and price_type == "decimal(18,2)"
)

print("Silver rows:", silver_rows)
print("Distinct product IDs:", distinct_ids)
print("Invalid product IDs:", invalid_ids)
print("Duplicate extra rows:", duplicate_extra_rows)
print("Price data type:", price_type)
print("STATUS:", "PASS" if passed else "REVIEW")

if not passed:
    raise AssertionError("Silver product table validation requires review.")

display(silver_products_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## CELL 04 — Silver Products: Write and Verification
# 
# **Status:** COMPLETED — PASS
# 
# - **Source:** `lh_bronze.bronze.reference_products_final_11274`
# - **Target:** `lh_silver.silver.products`
# - **Rows written:** 11,274
# - **Product IDs:** 11,274 distinct; 0 invalid; 0 duplicate extra rows
# - **Price type:** `decimal(18,2)`
# - **Price conversion failures:** 0
# 
# **Transformation:** Text fields are trimmed; whitespace-only values become NULL. Existing NULLs are preserved. `list_price_sar` is converted to `decimal(18,2)`.
# 
# **Verification:** The target table was read back from Silver and its row count, product IDs, and price data type passed validation.

# CELL ********************

tables = spark.sql("SHOW TABLES IN silver").collect()

products_entries = [
    row for row in tables
    if row["tableName"].lower() == "products"
]

print("silver.products registered in catalog:", len(products_entries))

for row in products_entries:
    print(row)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
