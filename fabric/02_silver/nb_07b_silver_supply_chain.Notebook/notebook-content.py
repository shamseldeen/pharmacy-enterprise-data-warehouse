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
    # silver name                          bronze name                                 primary key
    "supply_chain_suppliers":            ("supply_chain_suppliers",                  ["supplier_id"]),
    "supply_chain_warehouses":           ("supply_chain_warehouses",                 ["warehouse_id"]),
    "supply_chain_supplier_contracts":   ("supply_chain_supplier_contracts",         ["contract_id"]),
    "supply_chain_purchase_orders":      ("supply_chain_purchase_orders_fixed",      ["purchase_order_id"]),
    "supply_chain_purchase_order_lines": ("supply_chain_purchase_order_lines_final", ["po_line_id"]),
    "supply_chain_purchase_receipts":    ("supply_chain_purchase_receipts_fixed",    ["receipt_id"]),
}
print(f"{len(TABLES)} tables configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Explore: counts + flag values + rate ranges
# =========================================================
total = 0
for silver_name, (bronze_name, pk) in TABLES.items():
    n = spark.read.table(f"{BRONZE}.{bronze_name}").count()
    total += n
    print(f"{silver_name:36s} {n:>8,}")
print(f"{'TOTAL':36s} {total:>8,}")            # expected: 192,148

# Every integer column that LOOKS like a yes/no flag: what values does it hold?
print("\nFlag-like columns (distinct values):")
for silver_name, (bronze_name, pk) in TABLES.items():
    df = spark.read.table(f"{BRONZE}.{bronze_name}")
    for c, t in df.dtypes:
        if t == "int" and (c.endswith("_flag") or c.endswith("_capable")):
            vals = sorted(r[0] for r in df.select(c).distinct().collect())
            print(f"  {silver_name}.{c}: {vals}")

# Rate / percentage columns: fraction (0-1) or percent (0-100)?
print("\nRate columns (min / max):")
con = spark.read.table(f"{BRONZE}.supply_chain_supplier_contracts")
con.agg(*[F.min(c).alias(f"min_{c}") for c in ["target_fill_rate", "target_otif_rate", "rebate_pct"]],
        *[F.max(c).alias(f"max_{c}") for c in ["target_fill_rate", "target_otif_rate", "rebate_pct"]]).show()

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
        # Text: trim, empty -> null
        if t == "string":
            trimmed = F.trim(F.col(c))
            df = df.withColumn(c, F.when(trimmed == "", None).otherwise(trimmed))
        # Money -> exact decimal
        elif c.endswith("_sar") and t in ("double", "int"):
            df = df.withColumn(c, F.col(c).cast("decimal(18,2)"))
        # Rates / percentages (fractions 0..1) -> exact decimal (also fixes -0.0)
        elif (c.endswith("_rate") or c.endswith("_pct")) and t == "double":
            df = df.withColumn(c, F.col(c).cast("decimal(5,4)"))
        # 0/1 flags -> boolean with a clear "is_" name
        elif t == "int" and (c.endswith("_flag") or c.endswith("_capable")):
            new_name = "is_" + c.replace("_flag", "")
            df = df.withColumn(new_name, F.col(c) == 1).drop(c)
    return df


def load(silver_name):
    bronze_name, pk = TABLES[silver_name]
    src = f"{BRONZE}.{bronze_name}"
    df = standardize(spark.read.table(src))
    df = (df.withColumn("_silver_loaded_at", F.current_timestamp())
            .withColumn("_source_table", F.lit(src)))
    (df.write
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{silver_name}"))
    print(f"OK  {silver_name}   <- {bronze_name}")


for name in TABLES:
    load(name)

# Check the renamed flags and converted rates
spark.read.table(f"{SILVER}.supply_chain_purchase_receipts").printSchema()
spark.read.table(f"{SILVER}.supply_chain_supplier_contracts") \
     .select("target_fill_rate", "target_otif_rate", "rebate_pct").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: rows, duplicate keys, money totals
# =========================================================
MONEY = {"supply_chain_purchase_order_lines": "net_cost_sar"}

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
# CELL 5 — Supply chain business rules
# =========================================================
sup   = spark.read.table(f"{SILVER}.supply_chain_suppliers")
wh    = spark.read.table(f"{SILVER}.supply_chain_warehouses")
con   = spark.read.table(f"{SILVER}.supply_chain_supplier_contracts")
po    = spark.read.table(f"{SILVER}.supply_chain_purchase_orders")
pol   = spark.read.table(f"{SILVER}.supply_chain_purchase_order_lines")
rec   = spark.read.table(f"{SILVER}.supply_chain_purchase_receipts")
batch = spark.read.table(f"{SILVER}.product_batches")
prod  = spark.read.table(f"{SILVER}.products")

# Bring order info onto each receipt: receipt -> PO line -> PO
rx = (rec.join(pol.select("po_line_id", "purchase_order_id", "ordered_qty"), "po_line_id", "left")
         .join(po.select("purchase_order_id", "order_date", "expected_delivery_date"),
               "purchase_order_id", "left"))

# Is the on-time flag honest?  Hypothesis: on time = received on/before expected date
actually_on_time = F.col("receipt_date") <= F.col("expected_delivery_date")
# Is the in-full flag honest?   Hypothesis: in full = received at least what was ordered
actually_in_full = F.col("received_qty") >= F.col("ordered_qty")

# Contracts of the same supplier that overlap in time
w = Window.partitionBy("supplier_id").orderBy("contract_start_date")
con_overlaps = (con.withColumn("prev_end", F.lag("contract_end_date").over(w))
                   .filter(F.col("prev_end") >= F.col("contract_start_date")).count())

checks = {
    "PO lines: gross <> qty x unit cost (> 0.01)":
        pol.filter(F.abs(F.col("gross_cost_sar") -
                         F.col("ordered_qty") * F.col("unit_cost_sar")) > 0.01).count(),
    "PO lines: net <> gross - discount + tax (> 0.01)":
        pol.filter(F.abs(F.col("net_cost_sar") -
                         (F.col("gross_cost_sar") - F.col("discount_sar") + F.col("tax_sar"))) > 0.01).count(),
    "receipts: received before the order was placed":
        rx.filter(F.col("receipt_date") < F.col("order_date")).count(),
    "receipts: rejected qty > received qty":
        rx.filter(F.col("rejected_qty") > F.col("received_qty")).count(),
    "receipts: is_on_time flag disagrees with dates":
        rx.filter(F.col("is_on_time") != actually_on_time).count(),
    "receipts: is_in_full flag disagrees with quantities":
        rx.filter(F.col("is_in_full") != actually_in_full).count(),
    "PO lines never received [info]":
        pol.join(rec.select("po_line_id").distinct(), "po_line_id", "left_anti").count(),
    "contracts: overlapping per supplier [info]":
        con_overlaps,
    "POs with unknown supplier (orphans)":
        po.join(sup.select("supplier_id"), "supplier_id", "left_anti").count(),
    "POs with unknown warehouse (orphans)":
        po.join(wh.select("warehouse_id"), "warehouse_id", "left_anti").count(),
    "PO lines with unknown PO (orphans)":
        pol.join(po.select("purchase_order_id"), "purchase_order_id", "left_anti").count(),
    "PO lines with unknown product (orphans)":
        pol.join(prod.select("product_id"), "product_id", "left_anti").count(),
    "receipts with unknown PO line (orphans)":
        rec.join(pol.select("po_line_id"), "po_line_id", "left_anti").count(),
    "receipts with unknown batch (orphans)":
        rec.filter(F.col("batch_id").isNotNull())
           .join(batch.select("batch_id"), "batch_id", "left_anti").count(),
}

for name, n in checks.items():
    status = ("INFO" if "[info]" in name else "PASS" if n == 0 else "CHECK")
    print(f"{status:5s}  {name:52s} {n:>10,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
