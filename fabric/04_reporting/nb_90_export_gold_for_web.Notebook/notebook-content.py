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

# =========================================================
# CELL 1 — Settings: read Gold (warehouse) through OneLake
# nb_90_export_gold_for_web
# Exports small, aggregated CSV files from wh_gold for a free web
# dashboard on shamsinsights.com, before the Fabric trial ends.
# Default lakehouse: lh_silver. Not part of the pipeline. Simulated data.
# =========================================================
import os
from pyspark.sql import functions as F

WORKSPACE = "Pharmacy-Enterprise-DW-DEV"
GOLD = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/wh_gold.Warehouse/Tables/gold"
OUT = "/lakehouse/default/Files/export_web"
os.makedirs(OUT, exist_ok=True)


def gold(table):
    return spark.read.format("delta").load(f"{GOLD}/{table}")


# Test: if this line fails, stop and send the error.
print("scorecard rows:", gold("agg_branch_scorecard_monthly").count())




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Build the 6 small datasets
# =========================================================
sc = gold("agg_branch_scorecard_monthly")
ph = gold("dim_pharmacy").select("pharmacy_key", "pharmacy_id", "pharmacy_name")

money = [
    F.round(F.sum("revenue_sar"), 2).alias("revenue_sar"),
    F.round(F.sum("margin_sar"), 2).alias("margin_sar"),
    F.sum("transactions").alias("transactions"),
    F.round(F.sum("expense_sar"), 2).alias("expense_sar"),
]

datasets = {
    # 1. one row per month, whole network
    "monthly_network": sc.groupBy("month_start").agg(*money).orderBy("month_start"),

    # 2. one row per month x region
    "monthly_region": sc.groupBy("month_start", "region_name").agg(*money)
                        .orderBy("month_start", "region_name"),

    # 3. one row per branch x year
    "branch_year": (sc.withColumn("year", F.year("month_start"))
                      .groupBy("pharmacy_key", "year", "region_name", "city_name",
                               "branch_type", "size_band", "peer_group")
                      .agg(*money,
                           F.round(F.sum("worked_hours"), 0).alias("worked_hours"),
                           F.round(F.avg("overall_score"), 4).alias("avg_score"))
                      .join(ph, "pharmacy_key", "left")
                      .orderBy("year", "pharmacy_key")),

    # 4. one row per product
    "product_loss": gold("agg_product_loss_summary").select(
        "product_id", "trade_name", "category", "is_cost_ratio_suspicious",
        "revenue_sar", "margin_sar", "loss_sar", "loss_lines_pct", "loss_pharmacies"),

    # 5. one row per category
    "category_margin": (gold("agg_product_loss_summary").groupBy("category")
                          .agg(F.round(F.sum("revenue_sar"), 2).alias("revenue_sar"),
                               F.round(F.sum("margin_sar"), 2).alias("margin_sar"),
                               F.round(F.sum("loss_sar"), 2).alias("loss_sar"))
                          .orderBy(F.desc("revenue_sar"))),

    # 6. the checks page
    "data_checks": gold("ref_data_checks"),
}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# =========================================================
# CELL 3 — Write CSV files and print a reconciliation
# =========================================================
for name, df in datasets.items():
    pdf = df.toPandas()
    pdf.to_csv(f"{OUT}/{name}.csv", index=False, encoding="utf-8")
    size_kb = os.path.getsize(f"{OUT}/{name}.csv") / 1024
    print(f"{name:18s} {len(pdf):>8,} rows  {size_kb:>8,.0f} KB")

net = datasets["monthly_network"].agg(F.sum("revenue_sar"), F.sum("margin_sar")).first()
print(f"\nCheck: revenue {net[0]:,.2f} SAR, margin {net[1]:,.2f} SAR")
print("Expected (README): revenue 3,126,598,408.75 SAR, margin 918,000,519.14 SAR")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
