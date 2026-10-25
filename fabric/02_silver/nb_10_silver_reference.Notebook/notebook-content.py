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

# =========================================================
# CELL 1 — Settings
# =========================================================
from pyspark.sql import functions as F

# Where we read from and where we write to
BRONZE = "lh_bronze.bronze"
SILVER = "lh_silver.silver"

# The 16 small reference tables
TABLES = [
    "reference_cities",
    "reference_regions",
    "rx_doctor_specialties",
    "pos_payment_methods",
    "pos_return_reasons",
    "insurance_claim_statuses",
    "insurance_claim_rejection_reasons",
    "marketing_channels",
    "marketing_promotion_types",
    "delivery_delivery_methods",
    "finance_branch_expense_categories",
    "finance_vat_rate_timeline",
    "erp_shift_types",
    "erp_operating_hour_profiles",
    "crm_customer_service_reference",
    "supply_chain_supplier_types",
]

print(f"{len(TABLES)} tables configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Read-only check: can we read every Bronze table?
# =========================================================
total = 0
for t in TABLES:
    n = spark.read.table(f"{BRONZE}.{t}").count()
    total += n
    print(f"{t:40s} {n:>6}")

print("-" * 48)
print(f"{'TOTAL':40s} {total:>6}")   # expected: 263

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 3 — Clean each table and write it to Silver
# =========================================================
def clean(df, source_table):
    # 1) Text columns: remove leading/trailing spaces, and turn "" into null
    for col_name, col_type in df.dtypes:
        if col_type == "string":
            trimmed = F.trim(F.col(col_name))
            df = df.withColumn(
                col_name,
                F.when(trimmed == "", None).otherwise(trimmed)
            )
    # 2) Audit columns: when this row was loaded, and from where
    df = (df
          .withColumn("_silver_loaded_at", F.current_timestamp())
          .withColumn("_source_table", F.lit(f"{BRONZE}.{source_table}")))
    return df


def load_table(t):
    df = spark.read.table(f"{BRONZE}.{t}")
    df = clean(df, t)
    (df.write
       .mode("overwrite")                    # replace the table each run
       .option("overwriteSchema", "true")    # allow column changes
       .saveAsTable(f"{SILVER}.{t}"))
    print(f"OK  {t}")


# Try ONE table first. If it works, comment this line out
# and uncomment the loop below.
# load_table("reference_regions")

for t in TABLES:
     load_table(t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: Silver vs Bronze (supports composite keys)
# =========================================================
# Default key = first column. List exceptions here.
KEY_OVERRIDES = {
    "crm_customer_service_reference": ["reference_type", "code"],
}

rows = []
for t in TABLES:
    b = spark.read.table(f"{BRONZE}.{t}")
    s = spark.read.table(f"{SILVER}.{t}")
    keys = KEY_OVERRIDES.get(t, [b.columns[0]])          # a list of 1 or more columns
    dup_keys = s.groupBy(*keys).count().filter("count > 1").count()
    rows.append((t, ", ".join(keys), b.count(), s.count(), dup_keys))

result = spark.createDataFrame(
    rows, ["table", "key_columns", "bronze_rows", "silver_rows", "duplicate_keys"])
result = result.withColumn(
    "status",
    F.when((F.col("bronze_rows") == F.col("silver_rows")) &
           (F.col("duplicate_keys") == 0), "PASS").otherwise("CHECK"))

display(result)
print("Silver total:", result.agg(F.sum("silver_rows")).first()[0])  # expected: 263

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("lh_silver.silver.crm_customer_service_reference")
df.printSchema()
display(df.orderBy(df.columns[0]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
