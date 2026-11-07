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
# CELL 1 — Settings and builders
# nb_16_silver_core_rebuild
# Rebuilds the 10 Silver tables that were first created by the
# one-time notebooks nb_04 / nb_05 / nb_06 / nb_07 (errorifexists).
# Same columns, same rules, but safe to run again and again (overwrite),
# so the Data Pipeline can call it every run.
# =========================================================
from pyspark.sql import functions as F

BRONZE = "lh_bronze.bronze"
SILVER = "lh_silver.silver"


def bronze(name):
    return spark.read.table(f"{BRONZE}.{name}")


def clean_text(col_name):
    """Trim text; an empty string becomes NULL (same rule as nb_06)."""
    t = F.trim(F.col(col_name))
    return F.when(t == "", F.lit(None).cast("string")).otherwise(t).alias(col_name)


def build_erp_pharmacies():
    return bronze("erp_pharmacies").select(
        "pharmacy_id", "pharmacy_name", "region_id", "region_name", "city_name",
        "district_name", "branch_type", "opening_date", "identity_classification")


def build_erp_employees():
    return bronze("erp_employees").select(
        "employee_id", "full_name", "gender", "nationality_group", "birth_date",
        "hire_date", "termination_date", "primary_job_role_id",
        "primary_job_role_name", "identity_classification")


def build_erp_job_roles():
    return bronze("erp_job_roles").select("job_role_id", "job_role_name")


def build_erp_branch_lifecycle_events():
    return bronze("erp_branch_lifecycle_events").select(
        "branch_lifecycle_event_id", "pharmacy_id", "event_date", "event_type",
        "previous_value", "branch_type", "operating_profile_id", "source_system")


def build_products():
    return bronze("reference_products_final_11274").select(
        clean_text("product_id"), clean_text("trade_name"), clean_text("generic_name"),
        clean_text("strength"), clean_text("strength_unit"), clean_text("dosage_form"),
        clean_text("category"),
        F.col("list_price_sar").cast("decimal(18,2)").alias("list_price_sar"),
        clean_text("source_type"))


def build_product_batches():
    return bronze("supply_chain_product_batches_final").select(
        "batch_id", "product_id", "supplier_id", "manufacture_date", "expiry_date",
        F.col("unit_cost_sar").cast("decimal(18,2)").alias("unit_cost_sar"),
        "received_date", "temperature_class",
        F.col("recall_flag").cast("boolean").alias("is_recalled"))


def build_rx_prescriptions():
    return bronze("rx_prescriptions_fixed")          # all columns, as in nb_04


def build_rx_doctors():
    return bronze("rx_doctors")                       # all columns, as in nb_04


def build_rx_prescription_items():
    return bronze("rx_prescription_items_final").select(
        "prescription_item_id", "prescription_id", "product_id", "prescribed_quantity",
        "dosage_frequency", "duration_days", "authorized_refills", "route")


def build_rx_dispenses():
    d = bronze("rx_prescription_dispenses_fixed").alias("d")
    i = bronze("rx_prescription_items_final").select(
        "prescription_item_id", "prescription_id", "product_id", "prescribed_quantity",
        "dosage_frequency", "duration_days", "authorized_refills", "route").alias("i")
    r = bronze("rx_prescriptions_fixed").select(
        "prescription_id", "prescribed_ts", "customer_id", "doctor_id").alias("r")
    return (
        d.join(i, F.col("d.prescription_item_id") == F.col("i.prescription_item_id"), "left")
         .join(r, F.col("i.prescription_id") == F.col("r.prescription_id"), "left")
         .select(
            F.col("d.dispense_id").alias("dispense_id"),
            F.col("d.prescription_item_id").alias("prescription_item_id"),
            F.col("i.prescription_id").alias("prescription_id"),
            F.col("i.product_id").alias("product_id"),
            F.col("r.customer_id").alias("customer_id"),
            F.col("r.doctor_id").alias("doctor_id"),
            F.col("d.pharmacy_id").alias("pharmacy_id"),
            F.col("r.prescribed_ts").alias("prescribed_ts"),
            F.col("d.dispense_ts").alias("dispense_ts"),
            F.col("i.prescribed_quantity").alias("prescribed_quantity"),
            F.col("d.dispensed_quantity").alias("dispensed_quantity"),
            F.col("i.dosage_frequency").alias("dosage_frequency"),
            F.col("i.duration_days").alias("duration_days"),
            F.col("i.authorized_refills").alias("authorized_refills"),
            F.col("i.route").alias("route"),
            F.col("d.pharmacist_employee_id").alias("dispensing_pharmacist_employee_id"),
            F.col("d.linked_transaction_id").alias("dispense_linked_transaction_id")))


# silver table              -> (builder, primary key, expected rows or None)
# None = no fixed number: Silver must simply equal the Bronze source.
TABLES = {
    "erp_pharmacies":              (build_erp_pharmacies,              "pharmacy_id",               None),
    "erp_employees":               (build_erp_employees,               "employee_id",               10000),
    "erp_job_roles":               (build_erp_job_roles,               "job_role_id",               None),
    "erp_branch_lifecycle_events": (build_erp_branch_lifecycle_events, "branch_lifecycle_event_id", 1081),
    "products":                    (build_products,                    "product_id",                11274),
    "product_batches":             (build_product_batches,             "batch_id",                  40000),
    "rx_prescriptions":            (build_rx_prescriptions,            "prescription_id",           None),
    "rx_doctors":                  (build_rx_doctors,                  "doctor_id",                 None),
    "rx_prescription_items":       (build_rx_prescription_items,       "prescription_item_id",      203899),
    "rx_dispenses":                (build_rx_dispenses,                "dispense_id",               173913),
}
print(f"{len(TABLES)} tables configured")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# =========================================================
# CELL 2 — Build, check BEFORE writing, write, check AFTER writing
# =========================================================
results = []

for name, (builder, pk, expected) in TABLES.items():
    df = builder()
    rows = df.count()
    distinct_pk = df.select(pk).distinct().count()
    blank_pk = df.filter(F.col(pk).isNull() | (F.trim(F.col(pk).cast("string")) == "")).count()

    problems = []
    if expected is not None and rows != expected:
        problems.append(f"expected {expected:,} rows, found {rows:,}")
    if distinct_pk != rows:
        problems.append(f"{rows - distinct_pk:,} duplicate {pk}")
    if blank_pk > 0:
        problems.append(f"{blank_pk:,} NULL/blank {pk}")

    if problems:
        # Gate: a bad build never replaces a good Silver table.
        results.append((name, rows, None, "FAIL", "; ".join(problems)))
        continue

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{name}"))

    silver_rows = spark.read.table(f"{SILVER}.{name}").count()
    status = "PASS" if silver_rows == rows else "FAIL"
    note = "" if status == "PASS" else f"wrote {rows:,}, read back {silver_rows:,}"
    results.append((name, rows, silver_rows, status, note))

for name, rows, silver_rows, status, note in results:
    shown = f"{silver_rows:,}" if silver_rows is not None else "not written"
    print(f"{status:4s}  {name:30s} source {rows:>9,}  silver {shown:>11}  {note}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# =========================================================
# CELL 3 — Final gate for the Data Pipeline
# Any FAIL raises an error, so the pipeline marks this activity as
# Failed and does NOT run the Gold build after it.
# =========================================================
failed = [r[0] for r in results if r[3] != "PASS"]
if failed:
    raise RuntimeError(f"nb_16 failed for: {', '.join(failed)}")
print(f"STATUS: PASS — {len(results)} Silver tables rebuilt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
