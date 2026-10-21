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

BRONZE = "lh_bronze.bronze"
SILVER = "lh_silver.silver"
YEARS  = [2020, 2021, 2022, 2023, 2024, 2025]

# For each Silver target: how to build the Bronze table name for a given year.
# Note: the 2025 sales-lines table has an extra "_final" in its name.
SOURCES = {
    "pos_sales_transactions":    lambda y: f"pos_sales_transactions_{y}",
    "pos_sales_lines":           lambda y: f"pos_canonical_sales_lines_{y}"
                                           + ("_final" if y == 2025 else ""),
    "pos_payments":              lambda y: f"pos_payments_{y}",
}

for target, name_of in SOURCES.items():
    print(target, "->", [name_of(y) for y in YEARS])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Explore: row counts per year + schema consistency
# =========================================================
for target, name_of in SOURCES.items():
    print(f"\n=== {target} ===")
    first_cols = None
    total = 0
    for y in YEARS:
        df = spark.read.table(f"{BRONZE}.{name_of(y)}")
        n = df.count()
        total += n
        # Do all years have exactly the same columns?
        if first_cols is None:
            first_cols = df.columns
            same = "baseline"
        else:
            same = "same columns" if df.columns == first_cols else "DIFFERENT COLUMNS!"
        print(f"  {y}: {n:>12,}   {same}")
    print(f"  TOTAL {total:>12,}")

# Returns is one table (not per year)
r = spark.read.table(f"{BRONZE}.pos_returns_final").count()
print(f"\n=== pos_returns ===\n  TOTAL {r:>12,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 3 — Union all years, standardize, write to Silver
# =========================================================
def standardize(df):
    for c, t in df.dtypes:
        # Text: trim spaces, empty string -> null
        if t == "string":
            trimmed = F.trim(F.col(c))
            df = df.withColumn(c, F.when(trimmed == "", None).otherwise(trimmed))
        # Money: every *_sar column -> exact decimal with 2 places
        elif c.endswith("_sar") and t == "double":
            df = df.withColumn(c, F.col(c).cast("decimal(18,2)"))
        # VAT rate (e.g. 0.15) -> exact decimal with 4 places
        elif c == "vat_rate":
            df = df.withColumn(c, F.col(c).cast("decimal(5,4)"))
    return df


def add_audit(df, source):
    return (df.withColumn("_silver_loaded_at", F.current_timestamp())
              .withColumn("_source_table", F.lit(source)))


def build_yearly(target):
    name_of = SOURCES[target]
    parts = []
    for y in YEARS:
        src = f"{BRONZE}.{name_of(y)}"
        df = (spark.read.table(src)
                .withColumn("source_year", F.lit(y))      # remember which year it came from
                .withColumn("_source_table", F.lit(src)))
        parts.append(df)

    # Stack the 6 years on top of each other, matching columns BY NAME
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)

    df = standardize(df).withColumn("_silver_loaded_at", F.current_timestamp())

    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .partitionBy("source_year")                       # one folder per year
       .saveAsTable(f"{SILVER}.{target}"))
    print(f"OK  {target}")


def build_returns():
    src = f"{BRONZE}.pos_returns_final"
    df = add_audit(standardize(spark.read.table(src)), src)
    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.pos_returns"))
    print("OK  pos_returns")


# ---- Run ONE first ----
#build_yearly("pos_sales_transactions")

# ---- When it works, comment the line above and uncomment these ----
build_yearly("pos_sales_lines")
build_yearly("pos_payments")
build_returns()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: rows, duplicate keys, money totals
# =========================================================
CHECKS = [
    # (silver table,          primary key,      money column to compare)
    ("pos_sales_transactions", "transaction_id", None),
    ("pos_sales_lines",        "sales_line_id",  "net_amount_sar"),
    ("pos_payments",           "payment_id",     "payment_amount_sar"),
    ("pos_returns",            "return_id",      "refund_amount_sar"),
]

def read_bronze(target):
    if target == "pos_returns":
        return spark.read.table(f"{BRONZE}.pos_returns_final")
    name_of = SOURCES[target]
    df = spark.read.table(f"{BRONZE}.{name_of(YEARS[0])}")
    for y in YEARS[1:]:
        df = df.unionByName(spark.read.table(f"{BRONZE}.{name_of(y)}"))
    return df

rows = []
for target, pk, money in CHECKS:
    b = read_bronze(target)
    s = spark.read.table(f"{SILVER}.{target}")
    dups = s.groupBy(pk).count().filter("count > 1").count()
    if money:
        b_sum = b.agg(F.round(F.sum(money), 2)).first()[0]
        s_sum = s.agg(F.sum(money)).first()[0]
        diff = float(s_sum) - float(b_sum)
    else:
        b_sum = s_sum = diff = None
    rows.append((target, b.count(), s.count(), dups,
                 None if b_sum is None else float(b_sum),
                 None if s_sum is None else float(s_sum),
                 diff))

result = spark.createDataFrame(rows,
    "table string, bronze_rows long, silver_rows long, duplicate_keys long, "
    "bronze_sum double, silver_sum double, sum_diff double")
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 5 — Orphans: does every child point to an existing parent?
# =========================================================
tx    = spark.read.table(f"{SILVER}.pos_sales_transactions").select("transaction_id")
lines = spark.read.table(f"{SILVER}.pos_sales_lines")
pays  = spark.read.table(f"{SILVER}.pos_payments")
rets  = spark.read.table(f"{SILVER}.pos_returns")

# left_anti = "rows on the left that have NO match on the right"
checks = {
    "lines without transaction":
        lines.join(tx, "transaction_id", "left_anti").count(),
    "payments without transaction":
        pays.join(tx, "transaction_id", "left_anti").count(),
    "returns without original transaction":
        rets.join(tx.withColumnRenamed("transaction_id", "original_transaction_id"),
                  "original_transaction_id", "left_anti").count(),
    "returns without original sales line":
        rets.join(lines.select(F.col("sales_line_id").alias("original_sales_line_id")),
                  "original_sales_line_id", "left_anti").count(),
}

for name, n in checks.items():
    print(f"{'PASS' if n == 0 else 'CHECK':5s}  {name:40s} {n:>10,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
