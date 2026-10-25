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

# ============================================================
# CELL 01 — READ-ONLY CROSS-LAKEHOUSE CONNECTION CHECK
# ============================================================

# ------------------------------------------------------------
# HISTORY
# v1: wrote Silver tables to dbo.* (rx_dispenses, rx_prescriptions,
#     rx_prescription_items, rx_doctors, erp_pharmacies).
#     Tables were then moved MANUALLY from dbo to silver.
# v2 (2026-10-25): code now writes silver.* directly (no manual step).
# NOTE: tables already exist -> the BLOCKED guards stop overwrites.
# ------------------------------------------------------------

source_table = (
    "`Pharmacy-Enterprise-DW-DEV`."
    "lh_bronze.bronze.rx_prescription_dispenses_fixed"
)

dispense_source_df = spark.table(source_table)

print("Bronze source read: PASS")
print(f"Columns discovered: {len(dispense_source_df.columns)}")

dispense_source_df.printSchema()

display(
    dispense_source_df.select(
        "dispense_id",
        "prescription_item_id",
        "dispense_ts"
    ).limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 02 — DISPENSE QUANTITY PROFILE
# ============================================================

from pyspark.sql import functions as F

quantity_profile_df = dispense_source_df.agg(
    F.count("*").alias("total_rows"),

    F.sum(
        F.when(F.col("dispensed_quantity").isNull(), 1).otherwise(0)
    ).alias("null_quantity_rows"),

    F.sum(
        F.when(F.col("dispensed_quantity") == 0, 1).otherwise(0)
    ).alias("zero_quantity_rows"),

    F.sum(
        F.when(F.col("dispensed_quantity") < 0, 1).otherwise(0)
    ).alias("negative_quantity_rows"),

    F.sum(
        F.when(F.col("dispensed_quantity") > 0, 1).otherwise(0)
    ).alias("positive_quantity_rows"),

    F.min("dispensed_quantity").alias("minimum_quantity"),
    F.max("dispensed_quantity").alias("maximum_quantity")
)

display(quantity_profile_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 03 — PRESCRIPTION ITEM BRIDGE SCHEMA REVIEW
# ============================================================

item_source_table = (
    "`Pharmacy-Enterprise-DW-DEV`."
    "lh_bronze.bronze.rx_prescription_items_final"
)

rx_items_source_df = spark.table(item_source_table)

print("Prescription item table: Bronze source")
print(f"Columns discovered: {len(rx_items_source_df.columns)}")

rx_items_source_df.printSchema()

display(rx_items_source_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 04 — PRESCRIPTION ITEM INTEGRITY PROFILE
# ============================================================

from pyspark.sql import functions as F

rx_headers_table = (
    "`Pharmacy-Enterprise-DW-DEV`."
    "lh_bronze.bronze.rx_prescriptions_fixed"
)

rx_headers_df = (
    spark.table(rx_headers_table)
        .select("prescription_id")
)

# Find item rows whose non-null prescription_id has no matching header.
orphan_item_links_df = (
    rx_items_source_df
        .filter(F.col("prescription_id").isNotNull())
        .select("prescription_item_id", "prescription_id")
        .join(
            rx_headers_df,
            on="prescription_id",
            how="left_anti"
        )
)

# Profile required identifiers and prescribed quantity.
item_quality_df = rx_items_source_df.agg(
    F.count("*").alias("total_item_rows"),

    F.sum(
        F.when(F.col("prescription_item_id").isNull(), 1).otherwise(0)
    ).alias("null_item_id_rows"),

    F.sum(
        F.when(F.col("prescription_id").isNull(), 1).otherwise(0)
    ).alias("null_prescription_id_rows"),

    F.sum(
        F.when(F.col("product_id").isNull(), 1).otherwise(0)
    ).alias("null_product_id_rows"),

    F.sum(
        F.when(F.col("prescribed_quantity").isNull(), 1).otherwise(0)
    ).alias("null_prescribed_quantity_rows"),

    F.sum(
        F.when(F.col("prescribed_quantity") <= 0, 1).otherwise(0)
    ).alias("non_positive_prescribed_quantity_rows"),

    F.min("prescribed_quantity").alias("minimum_prescribed_quantity"),
    F.max("prescribed_quantity").alias("maximum_prescribed_quantity")
)

display(item_quality_df)

print(
    "Item rows without a matching prescription header:",
    orphan_item_links_df.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orphan_item_link_count = orphan_item_links_df.count()

print("Orphan prescription links:", orphan_item_link_count)
print(
    "STATUS:",
    "PASS" if orphan_item_link_count == 0 else "REVIEW"
)

if orphan_item_link_count > 0:
    display(orphan_item_links_df.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

disp = spark.table(
    "`Pharmacy-Enterprise-DW-DEV`.lh_bronze.bronze.rx_prescription_dispenses_fixed"
)

items = spark.table(
    "`Pharmacy-Enterprise-DW-DEV`.lh_bronze.bronze.rx_prescription_items_final"
)

rx = spark.table(
    "`Pharmacy-Enterprise-DW-DEV`.lh_bronze.bronze.rx_prescriptions_fixed"
)

joined = (
    disp.alias("d")
    .join(
        items.select("prescription_item_id", "prescription_id").alias("i"),
        F.col("d.prescription_item_id") == F.col("i.prescription_item_id"),
        "left"
    )
    .join(
        rx.select("prescription_id", "pharmacy_id").alias("r"),
        F.col("i.prescription_id") == F.col("r.prescription_id"),
        "left"
    )
    .select(
        F.col("d.dispense_id").alias("dispense_id"),
        F.col("i.prescription_id").alias("item_prescription_id"),
        F.col("r.prescription_id").alias("matched_prescription_id"),
        F.col("d.pharmacy_id").alias("dispense_pharmacy_id"),
        F.col("r.pharmacy_id").alias("prescription_pharmacy_id")
    )
)

pharmacy_mismatch = (
    F.col("dispense_pharmacy_id").isNotNull()
    & F.col("prescription_pharmacy_id").isNotNull()
    & (
        F.col("dispense_pharmacy_id")
        != F.col("prescription_pharmacy_id")
    )
)

join_profile = joined.agg(
    F.count("*").alias("joined_rows"),
    F.countDistinct("dispense_id").alias("distinct_dispense_ids"),
    F.sum(
        F.when(F.col("item_prescription_id").isNull(), 1).otherwise(0)
    ).alias("missing_item_links"),
    F.sum(
        F.when(F.col("matched_prescription_id").isNull(), 1).otherwise(0)
    ).alias("missing_prescription_links"),
    F.sum(
        F.when(pharmacy_mismatch, 1).otherwise(0)
    ).alias("pharmacy_mismatch_rows")
)

display(join_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

disp = spark.table(
    "`Pharmacy-Enterprise-DW-DEV`.lh_bronze.bronze.rx_prescription_dispenses_fixed"
)

items = spark.table(
    "`Pharmacy-Enterprise-DW-DEV`.lh_bronze.bronze.rx_prescription_items_final"
)

rx = spark.table(
    "`Pharmacy-Enterprise-DW-DEV`.lh_bronze.bronze.rx_prescriptions_fixed"
)

rx_dispense_candidate_df = (
    disp.alias("d")
    .join(
        items.select(
            "prescription_item_id",
            "prescription_id",
            "product_id",
            "prescribed_quantity",
            "dosage_frequency",
            "duration_days",
            "authorized_refills",
            "route"
        ).alias("i"),
        F.col("d.prescription_item_id")
        == F.col("i.prescription_item_id"),
        "left"
    )
    .join(
        rx.select(
            "prescription_id",
            "prescribed_ts",
            "customer_id",
            "doctor_id"
        ).alias("r"),
        F.col("i.prescription_id")
        == F.col("r.prescription_id"),
        "left"
    )
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
        F.col("d.pharmacist_employee_id").alias(
            "dispensing_pharmacist_employee_id"
        ),
        F.col("d.linked_transaction_id").alias(
            "dispense_linked_transaction_id"
        )
    )
)

candidate_profile = rx_dispense_candidate_df.agg(
    F.count("*").alias("candidate_rows"),
    F.countDistinct("dispense_id").alias("distinct_dispense_ids")
)

display(candidate_profile)
rx_dispense_candidate_df.printSchema()
display(rx_dispense_candidate_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

check_columns = [
    "dispense_id",
    "prescription_item_id",
    "prescription_id",
    "product_id",
    "pharmacy_id",
    "prescribed_ts",
    "dispense_ts",
    "prescribed_quantity",
    "dispensed_quantity"
]

checks = [
    F.count("*").alias("candidate_rows"),
    F.countDistinct("dispense_id").alias("distinct_dispense_ids")
]

for column_name in check_columns:
    checks.append(
        F.sum(
            F.when(F.col(column_name).isNull(), 1).otherwise(0)
        ).alias(f"null_{column_name}")
    )

checks.extend([
    F.sum(
        F.when(
            F.col("dispense_ts") < F.col("prescribed_ts"), 1
        ).otherwise(0)
    ).alias("dispense_before_prescription"),

    F.sum(
        F.when(F.col("prescribed_quantity") <= 0, 1).otherwise(0)
    ).alias("nonpositive_prescribed_quantity"),

    F.sum(
        F.when(F.col("dispensed_quantity") <= 0, 1).otherwise(0)
    ).alias("nonpositive_dispensed_quantity"),

    F.sum(
        F.when(
            F.col("dispensed_quantity") < F.col("prescribed_quantity"), 1
        ).otherwise(0)
    ).alias("dispensed_below_prescribed"),

    F.sum(
        F.when(
            F.col("dispensed_quantity") == F.col("prescribed_quantity"), 1
        ).otherwise(0)
    ).alias("dispensed_equal_prescribed"),

    F.sum(
        F.when(
            F.col("dispensed_quantity") > F.col("prescribed_quantity"), 1
        ).otherwise(0)
    ).alias("dispensed_above_prescribed")
])

display(rx_dispense_candidate_df.agg(*checks))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

additional_columns = [
    "customer_id",
    "doctor_id",
    "dosage_frequency",
    "duration_days",
    "authorized_refills",
    "route",
    "dispensing_pharmacist_employee_id",
    "dispense_linked_transaction_id"
]

null_profile = rx_dispense_candidate_df.agg(
    F.count("*").alias("candidate_rows"),
    *[
        F.sum(
            F.when(F.col(column_name).isNull(), 1).otherwise(0)
        ).alias(f"null_{column_name}")
        for column_name in additional_columns
    ]
)

display(null_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_rows = 173913

candidate_rows = rx_dispense_candidate_df.count()
distinct_dispense_ids = rx_dispense_candidate_df.select(
    "dispense_id"
).distinct().count()

if candidate_rows != expected_rows:
    raise ValueError(f"Unexpected row count: {candidate_rows}")

if distinct_dispense_ids != expected_rows:
    raise ValueError(
        f"dispense_id is not unique: {distinct_dispense_ids}"
    )

(
    rx_dispense_candidate_df.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable("silver.rx_dispenses")
)

print("Created Silver Delta table: silver.rx_dispenses")
print(f"Rows written: {candidate_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_rx_df = spark.table("silver.rx_dispenses")

readback_profile = silver_rx_df.agg(
    F.count("*").alias("rows_read_back"),
    F.countDistinct("dispense_id").alias("distinct_dispense_ids"),
    F.sum(
        F.when(F.col("dispense_id").isNull(), 1).otherwise(0)
    ).alias("null_dispense_ids"),
    F.sum(
        F.when(
            F.col("dispense_ts") < F.col("prescribed_ts"),
            1
        ).otherwise(0)
    ).alias("chronology_violations")
)

display(readback_profile)
silver_rx_df.printSchema()
display(silver_rx_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tables_df = (
    spark.sql("SHOW TABLES IN silver")
        .orderBy("tableName")
)

display(silver_tables_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rx_items_source_df = spark.table(
    "`Pharmacy-Enterprise-DW-DEV`.lh_bronze.bronze.rx_prescription_items_final"
)

rx_items_silver_df = rx_items_source_df.select(
    "prescription_item_id",
    "prescription_id",
    "product_id",
    "prescribed_quantity",
    "dosage_frequency",
    "duration_days",
    "authorized_refills",
    "route"
)

expected_rows = 203899
item_rows = rx_items_silver_df.count()
distinct_item_ids = rx_items_silver_df.select(
    "prescription_item_id"
).distinct().count()

if item_rows != expected_rows:
    raise ValueError(f"Unexpected item row count: {item_rows}")

if distinct_item_ids != expected_rows:
    raise ValueError(
        f"Prescription item IDs are not unique: {distinct_item_ids}"
    )

(
    rx_items_silver_df.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable("silver.rx_prescription_items")
)

print("Created Silver Delta table: silver.rx_prescription_items")
print(f"Rows written: {item_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_items_df = spark.table("silver.rx_prescription_items")

items_readback_profile = silver_items_df.agg(
    F.count("*").alias("rows_read_back"),
    F.countDistinct("prescription_item_id").alias("distinct_item_ids"),
    F.sum(
        F.when(F.col("prescription_item_id").isNull(), 1).otherwise(0)
    ).alias("null_item_ids"),
    F.sum(
        F.when(F.col("prescription_id").isNull(), 1).otherwise(0)
    ).alias("null_prescription_ids"),
    F.sum(
        F.when(F.col("product_id").isNull(), 1).otherwise(0)
    ).alias("null_product_ids"),
    F.sum(
        F.when(F.col("prescribed_quantity").isNull(), 1).otherwise(0)
    ).alias("null_prescribed_quantities"),
    F.sum(
        F.when(F.col("prescribed_quantity") <= 0, 1).otherwise(0)
    ).alias("nonpositive_prescribed_quantities")
)

display(items_readback_profile)
silver_items_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_dispenses = spark.table("silver.rx_dispenses")
silver_item_keys = (
    spark.table("silver.rx_prescription_items")
    .select("prescription_item_id")
    .filter(F.col("prescription_item_id").isNotNull())
    .distinct()
)

dispense_rows = silver_dispenses.count()
distinct_dispense_ids = silver_dispenses.select("dispense_id").distinct().count()

null_item_links = (
    silver_dispenses
    .filter(F.col("prescription_item_id").isNull())
    .count()
)

orphan_dispenses = (
    silver_dispenses
    .join(silver_item_keys, on="prescription_item_id", how="left_anti")
    .count()
)

print("Dispense rows:", dispense_rows)
print("Distinct dispense IDs:", distinct_dispense_ids)
print("NULL prescription item links:", null_item_links)
print("Dispenses without a matching Silver item:", orphan_dispenses)

if (
    dispense_rows == 173913
    and distinct_dispense_ids == 173913
    and null_item_links == 0
    and orphan_dispenses == 0
):
    print("STATUS: PASS")
else:
    print("STATUS: REVIEW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

rx_headers = spark.sql("""
    SELECT *
    FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`rx_prescriptions_fixed`
""")

rx_linkage_by_status = (
    rx_headers
    .groupBy("prescription_status")
    .agg(
        F.count("*").alias("prescription_count"),
        F.sum(
            F.when(F.col("linked_transaction_id").isNull(), 1).otherwise(0)
        ).alias("null_transaction_links"),
        F.sum(
            F.when(F.col("linked_transaction_id").isNotNull(), 1).otherwise(0)
        ).alias("present_transaction_links")
    )
    .withColumn(
        "null_link_percentage",
        F.round(
            100.0 * F.col("null_transaction_links")
            / F.col("prescription_count"),
            2
        )
    )
    .orderBy("prescription_status")
)

display(rx_linkage_by_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

source_rx_headers = spark.sql("""
    SELECT *
    FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`rx_prescriptions_fixed`
""")

target_exists = (
    spark.sql("SHOW TABLES IN silver")
    .filter(F.col("tableName") == "rx_prescriptions")
    .limit(1)
    .count() > 0
)

if target_exists:
    print("BLOCKED: silver.rx_prescriptions already exists. Do not overwrite it.")
else:
    source_row_count = source_rx_headers.count()

    source_rx_headers.write \
        .format("delta") \
        .mode("errorifexists") \
        .saveAsTable("silver.rx_prescriptions")

    print("Created Silver Delta table: silver.rx_prescriptions")
    print("Source rows:", source_row_count)
    print("Source columns:", len(source_rx_headers.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_rx_headers = spark.table("silver.rx_prescriptions")

rx_readback_profile = silver_rx_headers.agg(
    F.count("*").alias("rows_read_back"),
    F.countDistinct("prescription_id").alias("distinct_prescription_ids"),
    F.sum(
        F.when(F.col("prescription_id").isNull(), 1).otherwise(0)
    ).alias("null_prescription_ids"),
    F.sum(
        F.when(F.col("linked_transaction_id").isNull(), 1).otherwise(0)
    ).alias("null_transaction_links"),
    F.sum(
        F.when(F.col("linked_transaction_id").isNotNull(), 1).otherwise(0)
    ).alias("present_transaction_links")
)

display(rx_readback_profile)

display(
    silver_rx_headers
    .groupBy("prescription_status")
    .agg(
        F.count("*").alias("prescription_count"),
        F.sum(
            F.when(F.col("linked_transaction_id").isNull(), 1).otherwise(0)
        ).alias("null_transaction_links"),
        F.sum(
            F.when(F.col("linked_transaction_id").isNotNull(), 1).otherwise(0)
        ).alias("present_transaction_links")
    )
    .orderBy("prescription_status")
)

silver_rx_headers.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_rx_items = spark.table("silver.rx_prescription_items")
silver_rx_headers = spark.table("silver.rx_prescriptions")

header_keys = (
    silver_rx_headers
    .select("prescription_id")
    .filter(F.col("prescription_id").isNotNull())
    .distinct()
)

orphan_items = (
    silver_rx_items
    .join(header_keys, on="prescription_id", how="left_anti")
    .count()
)

null_prescription_links = (
    silver_rx_items
    .filter(F.col("prescription_id").isNull())
    .count()
)

print("Prescription item rows:", silver_rx_items.count())
print("NULL prescription links:", null_prescription_links)
print("Items without a matching prescription header:", orphan_items)

if null_prescription_links == 0 and orphan_items == 0:
    print("STATUS: PASS")
else:
    print("STATUS: REVIEW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

rx_headers = spark.table("silver.rx_prescriptions")
rx_items = spark.table("silver.rx_prescription_items")

actual_item_counts = (
    rx_items
    .groupBy("prescription_id")
    .agg(F.count("*").alias("actual_item_count"))
)

item_count_reconciliation = (
    rx_headers
    .select("prescription_id", "prescription_status", "item_count")
    .join(actual_item_counts, on="prescription_id", how="left")
    .withColumn(
        "actual_item_count",
        F.coalesce(F.col("actual_item_count"), F.lit(0))
    )
)

mismatches = item_count_reconciliation.filter(
    F.col("item_count").isNull()
    | (F.col("item_count") != F.col("actual_item_count"))
)

print("Prescription headers:", item_count_reconciliation.count())
print(
    "Headers with missing item_count:",
    item_count_reconciliation.filter(F.col("item_count").isNull()).count()
)
print("Item-count mismatches:", mismatches.count())

display(
    item_count_reconciliation
    .groupBy("prescription_status")
    .agg(
        F.count("*").alias("prescription_count"),
        F.sum(
            F.when(
                F.col("item_count") == F.col("actual_item_count"), 1
            ).otherwise(0)
        ).alias("matching_counts"),
        F.sum(
            F.when(
                F.col("item_count").isNull()
                | (F.col("item_count") != F.col("actual_item_count")),
                1
            ).otherwise(0)
        ).alias("mismatches")
    )
    .orderBy("prescription_status")
)

display(mismatches.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

rx_doctors_source = spark.sql("""
    SELECT *
    FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`rx_doctors`
""")

doctor_profile = rx_doctors_source.agg(
    F.count("*").alias("doctor_rows"),
    F.countDistinct("doctor_id").alias("distinct_doctor_ids"),
    F.sum(
        F.when(F.col("doctor_id").isNull(), 1).otherwise(0)
    ).alias("null_doctor_ids"),
    F.sum(
        F.when(F.col("active_from").isNull(), 1).otherwise(0)
    ).alias("null_active_from"),
    F.sum(
        F.when(F.col("active_to").isNull(), 1).otherwise(0)
    ).alias("null_active_to"),
    F.sum(
        F.when(
            F.col("active_from").isNotNull()
            & F.col("active_to").isNotNull()
            & (F.col("active_to") < F.col("active_from")),
            1
        ).otherwise(0)
    ).alias("temporal_violations")
)

duplicate_doctor_id_values = (
    rx_doctors_source
    .filter(F.col("doctor_id").isNotNull())
    .groupBy("doctor_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

display(doctor_profile)
print("Duplicate doctor_id values:", duplicate_doctor_id_values)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

rx_doctors_source = spark.sql("""
    SELECT *
    FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`rx_doctors`
""")

target_exists = (
    spark.sql("SHOW TABLES IN silver")
    .filter(F.col("tableName") == "rx_doctors")
    .limit(1)
    .count() > 0
)

if target_exists:
    print("BLOCKED: silver.rx_doctors already exists. Do not overwrite it.")
else:
    source_row_count = rx_doctors_source.count()
    source_column_count = len(rx_doctors_source.columns)

    rx_doctors_source.write \
        .format("delta") \
        .mode("errorifexists") \
        .saveAsTable("silver.rx_doctors")

    print("Created Silver Delta table: silver.rx_doctors")
    print("Source rows:", source_row_count)
    print("Source columns:", source_column_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_rx_doctors = spark.table("silver.rx_doctors")

doctor_readback_profile = silver_rx_doctors.agg(
    F.count("*").alias("rows_read_back"),
    F.countDistinct("doctor_id").alias("distinct_doctor_ids"),
    F.sum(
        F.when(F.col("doctor_id").isNull(), 1).otherwise(0)
    ).alias("null_doctor_ids"),
    F.sum(
        F.when(F.col("active_from").isNull(), 1).otherwise(0)
    ).alias("null_active_from"),
    F.sum(
        F.when(F.col("active_to").isNull(), 1).otherwise(0)
    ).alias("null_active_to"),
    F.sum(
        F.when(
            F.col("active_from").isNotNull()
            & F.col("active_to").isNotNull()
            & (F.col("active_to") < F.col("active_from")),
            1
        ).otherwise(0)
    ).alias("temporal_violations")
)

display(doctor_readback_profile)
silver_rx_doctors.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_rx_headers = spark.table("silver.rx_prescriptions")
silver_rx_doctors = spark.table("silver.rx_doctors")

doctor_keys = (
    silver_rx_doctors
    .select("doctor_id")
    .filter(F.col("doctor_id").isNotNull())
    .distinct()
)

null_doctor_links = (
    silver_rx_headers
    .filter(F.col("doctor_id").isNull())
    .count()
)

orphan_doctor_links = (
    silver_rx_headers
    .filter(F.col("doctor_id").isNotNull())
    .join(doctor_keys, on="doctor_id", how="left_anti")
    .count()
)

print("Prescription headers with NULL doctor_id:", null_doctor_links)
print("Prescription headers without a matching doctor:", orphan_doctor_links)

if null_doctor_links == 0 and orphan_doctor_links == 0:
    print("STATUS: PASS")
else:
    print("STATUS: REVIEW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_tables_df = (
    spark.sql("SHOW TABLES IN silver")
        .filter(F.col("isTemporary") == False)
        .select(F.col("tableName").alias("silver_table"))
        .orderBy("silver_table")
)

print(f"Persisted silver tables: {silver_tables_df.count()}")
display(silver_tables_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pharmacy_stage_df = spark.sql("""
SELECT
    pharmacy_id,
    pharmacy_name,
    region_id,
    region_name,
    city_name,
    district_name,
    branch_type,
    opening_date,
    identity_classification
FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`erp_pharmacies`
""")

print("Selected columns:", len(pharmacy_stage_df.columns))
pharmacy_stage_df.printSchema()
display(pharmacy_stage_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

pharmacy_stats = pharmacy_stage_df.agg(
    F.count("*").alias("source_rows"),
    F.countDistinct("pharmacy_id").alias("distinct_pharmacy_ids"),
    F.sum(
        F.when(F.col("pharmacy_id").isNull(), 1).otherwise(0)
    ).alias("null_pharmacy_ids"),
    F.sum(
        F.when(
            F.col("pharmacy_id").isNotNull()
            & (F.trim(F.col("pharmacy_id")) == ""),
            1
        ).otherwise(0)
    ).alias("blank_pharmacy_ids")
).first().asDict()

duplicate_pharmacy_keys_df = (
    pharmacy_stage_df
        .filter(F.col("pharmacy_id").isNotNull())
        .groupBy("pharmacy_id")
        .count()
        .filter(F.col("count") > 1)
        .orderBy("pharmacy_id")
)

duplicate_key_groups = duplicate_pharmacy_keys_df.count()

for metric, value in pharmacy_stats.items():
    print(f"{metric}: {value}")

print(f"duplicate_pharmacy_key_groups: {duplicate_key_groups}")

key_pass = (
    pharmacy_stats["null_pharmacy_ids"] == 0
    and pharmacy_stats["blank_pharmacy_ids"] == 0
    and pharmacy_stats["distinct_pharmacy_ids"] == pharmacy_stats["source_rows"]
    and duplicate_key_groups == 0
)

print("PHARMACY KEY STATUS:", "PASS" if key_pass else "REVIEW")
display(duplicate_pharmacy_keys_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = "silver.erp_pharmacies"

source_rows = pharmacy_stage_df.count()

(
    pharmacy_stage_df
        .write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(target_table)
)

silver_pharmacies_df = spark.table(target_table)
silver_rows = silver_pharmacies_df.count()

print(f"Created Silver table: {target_table}")
print(f"Source rows: {source_rows}")
print(f"Silver rows read back: {silver_rows}")
print(
    "ROW COUNT CHECK:",
    "PASS" if source_rows == silver_rows else "REVIEW"
)

silver_pharmacies_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver_pharmacies_df = spark.table("silver.erp_pharmacies")

invalid_key_condition = (
    F.col("pharmacy_id").isNull()
    | (F.trim(F.col("pharmacy_id").cast("string")) == "")
)

key_summary = silver_pharmacies_df.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("pharmacy_id").alias("distinct_pharmacy_ids"),
    F.sum(
        F.when(invalid_key_condition, 1).otherwise(0)
    ).alias("invalid_pharmacy_ids")
).first()

duplicate_key_groups = (
    silver_pharmacies_df
        .groupBy("pharmacy_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

print("=== SILVER PHARMACY KEY VALIDATION ===")
print("Rows:", key_summary["row_count"])
print("Distinct pharmacy IDs:", key_summary["distinct_pharmacy_ids"])
print("NULL or blank IDs:", key_summary["invalid_pharmacy_ids"])
print("Duplicate key groups:", duplicate_key_groups)

status = (
    key_summary["row_count"] == 1000
    and key_summary["distinct_pharmacy_ids"] == 1000
    and key_summary["invalid_pharmacy_ids"] == 0
    and duplicate_key_groups == 0
)

print("PHARMACY SILVER KEY STATUS:", "PASS" if status else "REVIEW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tables_df = (
    spark.sql("SHOW TABLES IN silver")
         .filter("isTemporary = false")
         .select("namespace", "tableName")
         .orderBy("tableName")
)

print(f"Silver tables found: {silver_tables_df.count()}")
display(silver_tables_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

rx_headers = (
    spark.table("silver.rx_prescriptions")
         .withColumn(
             "_prescription_key",
             F.trim(F.col("prescription_id").cast("string"))
         )
)

summary = rx_headers.agg(
    F.count("*").alias("row_count"),
    F.countDistinct(
        F.when(
            F.col("_prescription_key").isNotNull()
            & (F.col("_prescription_key") != ""),
            F.col("_prescription_key")
        )
    ).alias("distinct_nonblank_ids"),
    F.sum(
        F.when(
            F.col("_prescription_key").isNull()
            | (F.col("_prescription_key") == ""),
            1
        ).otherwise(0)
    ).alias("null_or_blank_ids")
).first()

duplicate_key_groups = (
    rx_headers
        .filter(
            F.col("_prescription_key").isNotNull()
            & (F.col("_prescription_key") != "")
        )
        .groupBy("_prescription_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

row_count = summary["row_count"]
distinct_ids = summary["distinct_nonblank_ids"] or 0
null_or_blank = summary["null_or_blank_ids"] or 0

status = (
    "PASS"
    if row_count > 0
    and row_count == distinct_ids
    and null_or_blank == 0
    and duplicate_key_groups == 0
    else "REVIEW"
)

print("=== SILVER RX PRESCRIPTION KEY VALIDATION ===")
print("Rows:", row_count)
print("Distinct nonblank prescription IDs:", distinct_ids)
print("NULL or blank IDs:", null_or_blank)
print("Duplicate key groups:", duplicate_key_groups)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

items = spark.table("silver.rx_prescription_items")
headers = spark.table("silver.rx_prescriptions")

item_rows = items.count()

null_or_blank_links = items.filter(
    F.col("prescription_id").isNull()
    | (F.trim(F.col("prescription_id")) == "")
).count()

orphan_rows = (
    items.alias("i")
         .join(
             headers.select("prescription_id").alias("h"),
             F.col("i.prescription_id") == F.col("h.prescription_id"),
             "left_anti"
         )
         .count()
)

expected_item_rows = 203899

status = (
    "PASS"
    if item_rows == expected_item_rows
    and null_or_blank_links == 0
    and orphan_rows == 0
    else "REVIEW"
)

print("=== SILVER RX ITEM TO HEADER VALIDATION ===")
print("Expected item rows:", expected_item_rows)
print("Silver item rows:", item_rows)
print("NULL or blank prescription links:", null_or_blank_links)
print("Items without a matching Silver header:", orphan_rows)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

doctors = (
    spark.table("silver.rx_doctors")
         .withColumn(
             "_doctor_key",
             F.trim(F.col("doctor_id").cast("string"))
         )
)

summary = doctors.agg(
    F.count("*").alias("row_count"),
    F.countDistinct(
        F.when(
            F.col("_doctor_key").isNotNull()
            & (F.col("_doctor_key") != ""),
            F.col("_doctor_key")
        )
    ).alias("distinct_nonblank_ids"),
    F.sum(
        F.when(
            F.col("_doctor_key").isNull()
            | (F.col("_doctor_key") == ""),
            1
        ).otherwise(0)
    ).alias("null_or_blank_ids")
).first()

duplicate_key_groups = (
    doctors
        .filter(
            F.col("_doctor_key").isNotNull()
            & (F.col("_doctor_key") != "")
        )
        .groupBy("_doctor_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

row_count = summary["row_count"]
distinct_ids = summary["distinct_nonblank_ids"] or 0
null_or_blank = summary["null_or_blank_ids"] or 0

status = (
    "PASS"
    if row_count == 20000
    and row_count == distinct_ids
    and null_or_blank == 0
    and duplicate_key_groups == 0
    else "REVIEW"
)

print("=== SILVER RX DOCTOR KEY VALIDATION ===")
print("Expected rows:", 20000)
print("Silver doctor rows:", row_count)
print("Distinct nonblank doctor IDs:", distinct_ids)
print("NULL or blank IDs:", null_or_blank)
print("Duplicate key groups:", duplicate_key_groups)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

prescriptions = spark.table("silver.rx_prescriptions")
doctors = spark.table("silver.rx_doctors").select("doctor_id")

prescription_rows = prescriptions.count()

null_or_blank_doctor_ids = prescriptions.filter(
    F.col("doctor_id").isNull()
    | (F.trim(F.col("doctor_id")) == "")
).count()

orphan_prescriptions = (
    prescriptions.alias("p")
        .join(
            doctors.alias("d"),
            F.col("p.doctor_id") == F.col("d.doctor_id"),
            "left_anti"
        )
        .count()
)

status = (
    "PASS"
    if prescription_rows == 100000
    and null_or_blank_doctor_ids == 0
    and orphan_prescriptions == 0
    else "REVIEW"
)

print("=== SILVER RX PRESCRIPTION TO DOCTOR VALIDATION ===")
print("Prescription rows:", prescription_rows)
print("NULL or blank doctor IDs:", null_or_blank_doctor_ids)
print("Prescriptions without a matching Silver doctor:", orphan_prescriptions)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

prescriptions = spark.table("silver.rx_prescriptions")
pharmacies = spark.table("silver.erp_pharmacies").select("pharmacy_id")

prescription_rows = prescriptions.count()

null_or_blank_pharmacy_ids = prescriptions.filter(
    F.col("pharmacy_id").isNull()
    | (F.trim(F.col("pharmacy_id")) == "")
).count()

orphan_prescriptions = (
    prescriptions.alias("p")
        .join(
            pharmacies.alias("ph"),
            F.col("p.pharmacy_id") == F.col("ph.pharmacy_id"),
            "left_anti"
        )
        .count()
)

status = (
    "PASS"
    if prescription_rows == 100000
    and null_or_blank_pharmacy_ids == 0
    and orphan_prescriptions == 0
    else "REVIEW"
)

print("=== SILVER RX PRESCRIPTION TO PHARMACY VALIDATION ===")
print("Prescription rows:", prescription_rows)
print("NULL or blank pharmacy IDs:", null_or_blank_pharmacy_ids)
print("Prescriptions without a matching Silver pharmacy:", orphan_prescriptions)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

items = (
    spark.table("silver.rx_prescription_items")
         .withColumn(
             "_item_key",
             F.trim(F.col("prescription_item_id").cast("string"))
         )
)

summary = items.agg(
    F.count("*").alias("row_count"),
    F.countDistinct(
        F.when(
            F.col("_item_key").isNotNull()
            & (F.col("_item_key") != ""),
            F.col("_item_key")
        )
    ).alias("distinct_nonblank_ids"),
    F.sum(
        F.when(
            F.col("_item_key").isNull()
            | (F.col("_item_key") == ""),
            1
        ).otherwise(0)
    ).alias("null_or_blank_ids")
).first()

duplicate_key_groups = (
    items
        .filter(
            F.col("_item_key").isNotNull()
            & (F.col("_item_key") != "")
        )
        .groupBy("_item_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

row_count = summary["row_count"]
distinct_ids = summary["distinct_nonblank_ids"] or 0
null_or_blank = summary["null_or_blank_ids"] or 0

status = (
    "PASS"
    if row_count == 203899
    and row_count == distinct_ids
    and null_or_blank == 0
    and duplicate_key_groups == 0
    else "REVIEW"
)

print("=== SILVER RX PRESCRIPTION ITEM KEY VALIDATION ===")
print("Expected rows:", 203899)
print("Silver item rows:", row_count)
print("Distinct nonblank item IDs:", distinct_ids)
print("NULL or blank IDs:", null_or_blank)
print("Duplicate key groups:", duplicate_key_groups)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

dispenses = spark.table("silver.rx_dispenses")
pharmacies = spark.table("silver.erp_pharmacies").select("pharmacy_id")

dispense_rows = dispenses.count()

null_or_blank_pharmacy_ids = dispenses.filter(
    F.col("pharmacy_id").isNull()
    | (F.trim(F.col("pharmacy_id")) == "")
).count()

orphan_dispenses = (
    dispenses.alias("d")
        .join(
            pharmacies.alias("ph"),
            F.col("d.pharmacy_id") == F.col("ph.pharmacy_id"),
            "left_anti"
        )
        .count()
)

status = (
    "PASS"
    if dispense_rows == 173913
    and null_or_blank_pharmacy_ids == 0
    and orphan_dispenses == 0
    else "REVIEW"
)

print("=== SILVER RX DISPENSE TO PHARMACY VALIDATION ===")
print("Dispense rows:", dispense_rows)
print("NULL or blank pharmacy IDs:", null_or_blank_pharmacy_ids)
print("Dispenses without a matching Silver pharmacy:", orphan_dispenses)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

employees = spark.sql("""
SELECT employee_id
FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`erp_employees`
""")

employee_rows = employees.count()

null_or_blank_ids = employees.filter(
    F.col("employee_id").isNull()
    | (F.trim(F.col("employee_id")) == "")
).count()

valid_ids = (
    employees
        .filter(
            F.col("employee_id").isNotNull()
            & (F.trim(F.col("employee_id")) != "")
        )
        .select(F.trim(F.col("employee_id")).alias("employee_id"))
)

distinct_employee_ids = valid_ids.distinct().count()

duplicate_id_groups = (
    valid_ids
        .groupBy("employee_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

status = (
    "PASS"
    if employee_rows == 10000
    and null_or_blank_ids == 0
    and duplicate_id_groups == 0
    else "REVIEW"
)

print("=== BRONZE ERP EMPLOYEE KEY VALIDATION ===")
print("Employee rows:", employee_rows)
print("Distinct nonblank employee IDs:", distinct_employee_ids)
print("NULL or blank employee IDs:", null_or_blank_ids)
print("Duplicate employee ID groups:", duplicate_id_groups)
print("STATUS:", status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
