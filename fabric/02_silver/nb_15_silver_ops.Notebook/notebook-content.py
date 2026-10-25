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
from pyspark.sql import Window

BRONZE = "lh_bronze.bronze"
SILVER = "lh_silver.silver"

TABLES = {
    # silver name                         bronze name                          primary key
    "finance_branch_monthly_expenses":  ("finance_branch_monthly_expenses",  ["branch_expense_id"]),
    "marketing_campaigns":              ("marketing_campaigns",              ["campaign_id"]),
    "marketing_promotions":             ("marketing_promotions",             ["promotion_id"]),
    "delivery_deliveries":              ("delivery_deliveries",              ["delivery_id"]),
}
print(f"{len(TABLES)} tables configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Explore + investigate DEL-002
# =========================================================
total = 0
for silver_name, (bronze_name, pk) in TABLES.items():
    n = spark.read.table(f"{BRONZE}.{bronze_name}").count()
    total += n
    print(f"{silver_name:34s} {n:>8,}")
print(f"{'TOTAL':34s} {total:>8,}")            # expected: 1,605,720

promo = spark.read.table(f"{BRONZE}.marketing_promotions")
print("\nPromotions: loyalty_only values + discount_pct range")
promo.groupBy("loyalty_only").count().show()
promo.agg(F.min("discount_pct"), F.max("discount_pct")).show()

# ---- DEL-002 ----
d = spark.read.table(f"{BRONZE}.delivery_deliveries")
mins = lambda a, b: (F.col(b).cast("long") - F.col(a).cast("long")) / 60   # minutes from a to b

d = (d.withColumn("order_to_delivered",    mins("order_timestamp",    "delivered_timestamp"))
      .withColumn("dispatch_to_delivered", mins("dispatch_timestamp", "delivered_timestamp"))
      .withColumn("reversed", F.col("delivered_timestamp") < F.col("dispatch_timestamp")))

print("Delivery status, split by reversed (delivered before dispatch):")
d.groupBy("delivery_status", "reversed").count().orderBy("delivery_status", "reversed").show()

print("Which interval does actual_minutes match? (normal rows vs reversed rows)")
(d.filter(F.col("delivered_timestamp").isNotNull())
  .groupBy("reversed")
  .agg(F.count("*").alias("rows"),
       F.avg(F.abs(F.col("actual_minutes") - F.col("order_to_delivered"))).alias("avg_gap_vs_order"),
       F.avg(F.abs(F.col("actual_minutes") - F.col("dispatch_to_delivered"))).alias("avg_gap_vs_dispatch"))
  .show())

print("Sample reversed rows:")
(d.filter("reversed")
  .select("delivery_id", "order_timestamp", "dispatch_timestamp", "delivered_timestamp",
          "actual_minutes", "promised_minutes", "delivery_status").show(5, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2b — Test hypothesis (d): two systems / clock mismatch
# =========================================================
methods = spark.read.table(f"{SILVER}.delivery_delivery_methods")
print("Delivery methods:")
methods.show(truncate=False)

print("Reversed rate by delivery method:")
(d.filter(F.col("delivered_timestamp").isNotNull())
  .groupBy("delivery_method_id")
  .agg(F.count("*").alias("rows"),
       F.sum(F.col("reversed").cast("int")).alias("reversed_rows"))
  .withColumn("reversed_pct", F.round(F.col("reversed_rows") / F.col("rows") * 100, 2))
  .orderBy("delivery_method_id").show())

print("How far back is 'delivered' before 'dispatch'? (minutes) — fixed or random?")
(d.filter("reversed")
  .withColumn("minutes_reversed", -F.col("dispatch_to_delivered"))
  .select(F.min("minutes_reversed"), F.expr("percentile_approx(minutes_reversed, 0.5)").alias("median"),
          F.max("minutes_reversed"), F.stddev("minutes_reversed").alias("stddev"))
  .show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 3 — Standardize and write to Silver
# =========================================================
def standardize(df):
    for c, t in df.dtypes:
        if t == "string":
            trimmed = F.trim(F.col(c))
            df = df.withColumn(c, F.when(trimmed == "", None).otherwise(trimmed))
        elif c.endswith("_sar") and t in ("double", "int"):
            df = df.withColumn(c, F.col(c).cast("decimal(18,2)"))
        elif c.endswith("_pct") and t == "double":
            df = df.withColumn(c, F.col(c).cast("decimal(5,4)"))     # also fixes -0.0
    return df


def table_specific(silver_name, df):
    if silver_name == "marketing_promotions":
        df = (df.withColumn("is_loyalty_only", F.col("loyalty_only") == 1)
                .drop("loyalty_only"))
    if silver_name == "delivery_deliveries":
        # DEL-002: dispatch was logged after the actual hand-over (see investigation).
        # Keep the original timestamps; just flag the rows.
        df = df.withColumn("is_dispatch_logged_late",
                           F.coalesce(F.col("delivered_timestamp") < F.col("dispatch_timestamp"),
                                      F.lit(False)))
    return df


def load(silver_name):
    bronze_name, pk = TABLES[silver_name]
    src = f"{BRONZE}.{bronze_name}"
    df = table_specific(silver_name, standardize(spark.read.table(src)))
    df = (df.withColumn("_silver_loaded_at", F.current_timestamp())
            .withColumn("_source_table", F.lit(src)))
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{silver_name}"))
    print(f"OK  {silver_name}")


for name in TABLES:
    load(name)

spark.read.table(f"{SILVER}.delivery_deliveries").groupBy("is_dispatch_logged_late").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: rows, duplicate keys, money totals
# =========================================================
MONEY = {"finance_branch_monthly_expenses": "expense_amount_sar",
         "delivery_deliveries": "delivery_fee_sar"}

rows = []
for silver_name, (bronze_name, pk) in TABLES.items():
    b = spark.read.table(f"{BRONZE}.{bronze_name}")
    s = spark.read.table(f"{SILVER}.{silver_name}")
    dups = s.groupBy(*pk).count().filter("count > 1").count()
    m = MONEY.get(silver_name)
    diff = None
    if m:
        diff = float(s.agg(F.sum(m)).first()[0]) - float(b.agg(F.round(F.sum(m), 2)).first()[0])
    rows.append((silver_name, ", ".join(pk), b.count(), s.count(), dups, diff))

display(spark.createDataFrame(rows,
    "table string, key_columns string, bronze_rows long, silver_rows long, "
    "duplicate_keys long, sum_diff double"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 5 — Ops business rules
# =========================================================
exp   = spark.read.table(f"{SILVER}.finance_branch_monthly_expenses")
cat   = spark.read.table(f"{SILVER}.finance_branch_expense_categories")
camp  = spark.read.table(f"{SILVER}.marketing_campaigns")
prom  = spark.read.table(f"{SILVER}.marketing_promotions")
dlv   = spark.read.table(f"{SILVER}.delivery_deliveries")
pharm = spark.read.table(f"{SILVER}.erp_pharmacies")
cust  = spark.read.table(f"{SILVER}.crm_customers")
tx    = spark.read.table(f"{SILVER}.pos_sales_transactions").select("transaction_id")

pc = prom.join(camp.select("campaign_id",
                           F.col("start_date").alias("camp_start"),
                           F.col("end_date").alias("camp_end")), "campaign_id", "left")

checks = {
    "expenses: same pharmacy + month + category twice":
        exp.groupBy("pharmacy_id", "accounting_month", "expense_category_id")
           .count().filter("count > 1").count(),
    "expenses: negative amount [info]":
        exp.filter(F.col("expense_amount_sar") < 0).count(),
    "expenses with unknown category (orphans)":
        exp.join(cat.select("expense_category_id"), "expense_category_id", "left_anti").count(),
    "expenses with unknown pharmacy (orphans)":
        exp.join(pharm.select("pharmacy_id"), "pharmacy_id", "left_anti").count(),
    "promotions: outside their campaign dates":
        pc.filter((F.col("start_date") < F.col("camp_start")) |
                  (F.col("end_date") > F.col("camp_end"))).count(),
    "promotions with unknown campaign (orphans)":
        pc.filter(F.col("camp_start").isNull()).count(),
    "deliveries: DELIVERED but no delivered time":
        dlv.filter((F.col("delivery_status") == "DELIVERED") &
                   F.col("delivered_timestamp").isNull()).count(),
    "deliveries: has delivered time but status not DELIVERED":
        dlv.filter((F.col("delivery_status") != "DELIVERED") &
                   F.col("delivered_timestamp").isNotNull()).count(),
    "deliveries: dispatch logged after delivery [known issue]":
        dlv.filter("is_dispatch_logged_late").count(),
    "deliveries with unknown transaction (orphans)":
        dlv.join(tx, "transaction_id", "left_anti").count(),
    "deliveries with unknown customer (orphans)":
        dlv.join(cust.select("customer_id"), "customer_id", "left_anti").count(),
    "deliveries with unknown pharmacy (orphans)":
        dlv.join(pharm.select("pharmacy_id"), "pharmacy_id", "left_anti").count(),
}

for name, n in checks.items():
    status = ("INFO"  if "[info]" in name else
              "KNOWN" if "[known issue]" in name else
              "PASS"  if n == 0 else "CHECK")
    print(f"{status:5s}  {name:58s} {n:>8,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
