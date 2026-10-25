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
# CELL 1 — Settings: Silver name -> (Bronze name, primary key)
# =========================================================
from pyspark.sql import functions as F

BRONZE = "lh_bronze.bronze"
SILVER = "lh_silver.silver"

TABLES = {
    # silver name                      bronze name                               primary key
    "insurance_plans":               ("insurance_plans",                       "insurance_plan_id"),
    "insurance_customer_membership": ("insurance_customer_membership",         "insurance_member_id"),
    "insurance_eligibility_history": ("insurance_eligibility_history",         "eligibility_id"),
    "insurance_claims":              ("insurance_insurance_claims_fixed",      "claim_id"),
    "insurance_claim_lines":         ("insurance_insurance_claim_lines_final", "claim_line_id"),
}
print(f"{len(TABLES)} tables configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Explore: counts + look at values BEFORE converting them
# =========================================================
total = 0
for silver_name, (bronze_name, pk) in TABLES.items():
    n = spark.read.table(f"{BRONZE}.{bronze_name}").count()
    total += n
    print(f"{silver_name:32s} {n:>10,}")
print(f"{'TOTAL':32s} {total:>10,}")          # expected: 1,173,405

# Two columns we plan to convert — inspect them first
plans = spark.read.table(f"{BRONZE}.insurance_plans")
print("\nprescription_coverage values:")
plans.groupBy("prescription_coverage").count().show()
print("default_coverage_pct range:")
plans.agg(F.min("default_coverage_pct"), F.max("default_coverage_pct")).show()

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
        # Money: any numeric *_sar column (double OR integer) -> exact decimal
        elif c.endswith("_sar") and t in ("double", "int"):
            df = df.withColumn(c, F.col(c).cast("decimal(18,2)"))
        # Percentages stored as fractions (0.6 .. 0.95)
        elif c.endswith("_pct") and t == "double":
            df = df.withColumn(c, F.col(c).cast("decimal(5,4)"))
    return df


def table_specific(silver_name, df):
    # Plans: 0/1 flag -> clear boolean with a readable name
    if silver_name == "insurance_plans":
        df = (df.withColumn("covers_prescriptions", F.col("prescription_coverage") == 1)
                .drop("prescription_coverage"))
    return df


def load(silver_name):
    bronze_name, pk = TABLES[silver_name]
    src = f"{BRONZE}.{bronze_name}"
    df = spark.read.table(src)
    df = standardize(df)
    df = table_specific(silver_name, df)
    df = (df.withColumn("_silver_loaded_at", F.current_timestamp())
            .withColumn("_source_table", F.lit(src)))
    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{silver_name}"))
    print(f"OK  {silver_name}   <- {bronze_name}")


# ---- Run ONE first ----
#load("insurance_plans")

# ---- Then comment the line above and uncomment the loop ----
for name in TABLES:
     load(name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: rows, duplicate keys, money totals
# =========================================================
MONEY = {"insurance_claims": "claimed_amount_sar",
         "insurance_claim_lines": "claimed_amount_sar"}

rows = []
for silver_name, (bronze_name, pk) in TABLES.items():
    b = spark.read.table(f"{BRONZE}.{bronze_name}")
    s = spark.read.table(f"{SILVER}.{silver_name}")
    dups = s.groupBy(pk).count().filter("count > 1").count()
    m = MONEY.get(silver_name)
    diff = None
    if m:
        diff = float(s.agg(F.sum(m)).first()[0]) - float(b.agg(F.round(F.sum(m), 2)).first()[0])
    rows.append((silver_name, b.count(), s.count(), dups, diff))

display(spark.createDataFrame(rows,
    "table string, bronze_rows long, silver_rows long, duplicate_keys long, sum_diff double"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 5 — Business rules + overlapping coverage periods
# =========================================================
from pyspark.sql import Window

elig   = spark.read.table(f"{SILVER}.insurance_eligibility_history")
claims = spark.read.table(f"{SILVER}.insurance_claims")
lines  = spark.read.table(f"{SILVER}.insurance_claim_lines")
plans  = spark.read.table(f"{SILVER}.insurance_plans")

# --- Overlaps: sort each member's periods by start date,
#     then compare each period with the one BEFORE it (lag)
w = Window.partitionBy("insurance_member_id").orderBy("coverage_start_date")
e = (elig.withColumn("prev_end", F.lag("coverage_end_date").over(w))
         .withColumn("prev_exists", F.lag("eligibility_id").over(w).isNotNull()))
overlaps = e.filter(
    F.col("prev_exists") &
    (F.col("prev_end").isNull() | (F.col("prev_end") >= F.col("coverage_start_date")))
).count()

# --- Claim header sums from its lines
line_sums = lines.groupBy("claim_id").agg(F.sum("claimed_amount_sar").alias("lines_claimed"))

checks = {
    "eligibility: end before start":
        elig.filter(F.col("coverage_end_date") < F.col("coverage_start_date")).count(),
    "eligibility: open-ended (no end date) [info]":
        elig.filter(F.col("coverage_end_date").isNull()).count(),
    "eligibility: overlapping periods per member":
        overlaps,
    "claims: claimed <> approved + rejected + copay":
        claims.filter(F.col("claimed_amount_sar") !=
                      F.col("approved_amount_sar") + F.col("rejected_amount_sar")
                      + F.col("customer_copay_sar")).count(),
    "claim lines: amounts not reconcilable [known issue]":
        lines.filter(F.col("claimed_amount_sar") !=
                     F.col("approved_amount_sar") + F.col("rejected_amount_sar")).count(),
    "claims: header <> sum of lines [known issue]":
        claims.join(line_sums, "claim_id", "left")
              .filter(F.coalesce(F.col("lines_claimed"), F.lit(0)) != F.col("claimed_amount_sar")).count(),
    "claims: adjudicated before submitted":
        claims.filter(F.col("adjudicated_ts") < F.col("submitted_ts")).count(),
    "claim lines without a claim (orphans)":
        lines.join(claims.select("claim_id"), "claim_id", "left_anti").count(),
    "claims with unknown plan (orphans)":
        claims.join(plans.select("insurance_plan_id"), "insurance_plan_id", "left_anti").count(),
}

for name, n in checks.items():
    status = ("INFO"  if "[info]" in name else
              "KNOWN" if "[known issue]" in name else
              "PASS"  if n == 0 else "CHECK")
    print(f"{status:5s}  {name:52s} {n:>10,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 6 — Investigate: what is the real formula?
# =========================================================
c = claims.withColumn(
        "gap", F.col("claimed_amount_sar")
               - F.col("approved_amount_sar") - F.col("rejected_amount_sar"))

# 1) Look at real rows with a gap
print("Sample claims with a gap:")
c.filter("gap <> 0").select(
    "claim_id", "claimed_amount_sar", "approved_amount_sar",
    "rejected_amount_sar", "customer_copay_sar", "gap").show(10)

# 2) Test the hypothesis: is the gap exactly the copay?
print("Hypothesis — claimed = approved + rejected + copay:")
print("  rows that still do NOT match:",
      c.filter(F.col("gap") != F.col("customer_copay_sar")).count())

# 3) Header vs lines: how big is the difference?
hv = (claims.join(line_sums, "claim_id", "left")
            .withColumn("diff", F.col("claimed_amount_sar") - F.col("lines_claimed")))
print("Header minus lines:")
hv.select(F.min("diff"), F.max("diff"), F.avg("diff")).show()
hv.select("claim_id", "claimed_amount_sar", "lines_claimed", "diff",
          "customer_copay_sar").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("lh_silver.silver.insurance_plans").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
