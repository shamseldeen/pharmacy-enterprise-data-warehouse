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
# CELL 1 — Settings: Silver name -> (Bronze name, primary key columns)
# =========================================================
from pyspark.sql import functions as F
from pyspark.sql import Window

BRONZE = "lh_bronze.bronze"
SILVER = "lh_silver.silver"

TABLES = {
    # silver name                   bronze name                                         primary key
    "inventory_monthly_snapshot": ("inventory_inventory_monthly_snapshot_2025_final", ["month_start", "pharmacy_id", "product_id"]),
    "inventory_movements":        ("inventory_inventory_movements_final_v2",          ["movement_id"]),
    "inventory_cycle_counts":     ("inventory_inventory_cycle_counts_2025_final",     ["cycle_count_id"]),
    "inventory_stock_transfers":  ("inventory_stock_transfers_final",                 ["transfer_id"]),
}
print(f"{len(TABLES)} tables configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Explore: counts + inspect the 'month' timestamps
# =========================================================
total = 0
for silver_name, (bronze_name, pk) in TABLES.items():
    n = spark.read.table(f"{BRONZE}.{bronze_name}").count()
    total += n
    print(f"{silver_name:30s} {n:>10,}")
print(f"{'TOTAL':30s} {total:>10,}")          # expected: 805,988

# What do the 'month' values really look like?
snap = spark.read.table(f"{BRONZE}.inventory_inventory_monthly_snapshot_2025_final")
print("\nDistinct 'month' values in the snapshot:")
(snap.groupBy("month").count()
     .withColumn("as_text", F.date_format("month", "yyyy-MM-dd HH:mm:ss"))
     .orderBy("month").show(20, truncate=False))

print("Spark session time zone:", spark.conf.get("spark.sql.session.timeZone"))

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
    return df


def table_specific(silver_name, df):
    # 'month' is a timestamp at 00:00 UTC -> make it a real DATE with a clear name
    if "month" in df.columns:
        df = df.withColumn("month_start", F.to_date("month")).drop("month")
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


# This time all 4 at once — you know the pattern works
for name in TABLES:
    load(name)

# Quick check of the conversion
spark.read.table(f"{SILVER}.inventory_monthly_snapshot").select("month_start").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: rows, duplicate keys, money totals
# =========================================================
MONEY = {"inventory_stock_transfers": "transfer_value_sar"}

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
# CELL 5 — Inventory business rules
# =========================================================
snap  = spark.read.table(f"{SILVER}.inventory_monthly_snapshot")
mov   = spark.read.table(f"{SILVER}.inventory_movements")
cyc   = spark.read.table(f"{SILVER}.inventory_cycle_counts")
trf   = spark.read.table(f"{SILVER}.inventory_stock_transfers")
batch = spark.read.table(f"{SILVER}.product_batches")
prod  = spark.read.table(f"{SILVER}.products")
pharm = spark.read.table(f"{SILVER}.erp_pharmacies")

# Continuity: this month's opening must equal LAST month's closing
w = Window.partitionBy("pharmacy_id", "product_id").orderBy("month_start")
continuity_breaks = (snap.withColumn("prev_closing", F.lag("closing_stock").over(w))
                         .filter(F.col("prev_closing").isNotNull() &
                                 (F.col("opening_stock") != F.col("prev_closing")))
                         .count())

# Movements that happened AFTER the batch expired (selling/moving expired stock?)
mov_exp = (mov.join(batch.select("batch_id", "expiry_date"), "batch_id", "inner")
              .filter(F.col("movement_date") > F.col("expiry_date")))

checks = {
    "snapshot: closing <> opening + net movement":
        snap.filter(F.col("closing_stock") !=
                    F.col("opening_stock") + F.col("net_movement_qty")).count(),
    "snapshot: opening <> previous month closing":
        continuity_breaks,
    "snapshot: negative closing stock":
        snap.filter(F.col("closing_stock") < 0).count(),
    "cycle counts: variance <> counted - system":
        cyc.filter(F.col("variance_qty") !=
                   F.col("counted_qty") - F.col("system_qty")).count(),
    "cycle counts: has a variance [info]":
        cyc.filter(F.col("variance_qty") != 0).count(),
    "transfers: value <> quantity x unit cost (> 0.01)":
        trf.filter(F.abs(F.col("transfer_value_sar") -
                         F.round(F.col("quantity") * F.col("unit_cost_sar"), 2)) > 0.01).count(),
    "transfers: source = destination":
        trf.filter(F.col("source_pharmacy_id") == F.col("destination_pharmacy_id")).count(),
    "transfers: quantity <= 0":
        trf.filter(F.col("quantity") <= 0).count(),
    "movements: unknown batch (orphans)":
        mov.filter(F.col("batch_id").isNotNull())
           .join(batch.select("batch_id"), "batch_id", "left_anti").count(),
    "snapshot: unknown product (orphans)":
        snap.join(prod.select("product_id"), "product_id", "left_anti").count(),
    "snapshot: unknown pharmacy (orphans)":
        snap.join(pharm.select("pharmacy_id"), "pharmacy_id", "left_anti").count(),
    "movements after batch expiry date [info]":
        mov_exp.count(),
}

for name, n in checks.items():
    status = ("INFO" if "[info]" in name else "PASS" if n == 0 else "CHECK")
    print(f"{status:5s}  {name:52s} {n:>10,}")

# If any expired movements exist, what kind are they?
print("\nMovements after expiry, by type:")
mov_exp.groupBy("movement_type").count().orderBy(F.desc("count")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
