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

for schema_name in ["silver", "dbo"]:
    print(f"Tables in {schema_name}:")
    spark.sql(f"SHOW TABLES IN {schema_name}").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_tables = [
    "erp_pharmacies",
    "rx_dispenses",
    "rx_doctors",
    "rx_prescription_items",
    "rx_prescriptions"
]

for table_name in silver_tables:
    row_count = spark.table(f"silver.{table_name}").count()
    print(f"{table_name}: {row_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee_stage_df = spark.sql("""
SELECT
    employee_id,
    full_name,
    gender,
    nationality_group,
    birth_date,
    hire_date,
    termination_date,
    primary_job_role_id,
    primary_job_role_name,
    identity_classification
FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`erp_employees`
""")

employee_source_rows = employee_stage_df.count()

print("ERP employee source rows:", employee_source_rows)
print("Selected columns:", len(employee_stage_df.columns))

employee_stage_df.printSchema()
display(employee_stage_df.limit(10))

if employee_source_rows != 10000:
    raise ValueError(
        f"Expected 10000 employee rows, found {employee_source_rows}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = "silver.erp_employees"

# Stop safely if the target already exists.
target_exists = (
    spark.sql("SHOW TABLES IN silver")
         .filter("tableName = 'erp_employees'")
         .limit(1)
         .count() > 0
)

if target_exists:
    raise RuntimeError(
        f"{target_table} already exists. No overwrite was performed."
    )

source_rows = employee_stage_df.count()
source_termination_nulls = (
    employee_stage_df
        .filter("termination_date IS NULL")
        .count()
)

if source_rows != 10000:
    raise ValueError(
        f"Expected 10000 source rows, found {source_rows}"
    )

# Create a Delta table in the existing Silver schema.
employee_stage_df.write \
    .format("delta") \
    .mode("errorifexists") \
    .saveAsTable(target_table)

# Read back the saved table and verify row count and NULL preservation.
silver_employee_df = spark.table(target_table)

silver_rows = silver_employee_df.count()
silver_termination_nulls = (
    silver_employee_df
        .filter("termination_date IS NULL")
        .count()
)

print("Created Silver table:", target_table)
print("Source rows:", source_rows)
print("Silver rows:", silver_rows)
print("Source termination_date NULLs:", source_termination_nulls)
print("Silver termination_date NULLs:", silver_termination_nulls)
print("Silver columns:", len(silver_employee_df.columns))

if silver_rows != source_rows:
    raise ValueError("Row-count validation failed.")

if silver_termination_nulls != source_termination_nulls:
    raise ValueError("termination_date NULL-preservation validation failed.")

print("STATUS: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

employees_silver_df = spark.table("silver.erp_employees")

employee_checks = employees_silver_df.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("employee_id").alias("distinct_employee_ids"),

    F.sum(
        F.when(
            F.col("employee_id").isNull()
            | (F.trim(F.col("employee_id")) == ""),
            1
        ).otherwise(0)
    ).alias("invalid_employee_ids"),

    F.sum(
        F.when(
            F.col("birth_date").isNull(), 1
        ).otherwise(0)
    ).alias("birth_date_nulls"),

    F.sum(
        F.when(
            F.col("hire_date").isNull(), 1
        ).otherwise(0)
    ).alias("hire_date_nulls"),

    F.sum(
        F.when(
            F.col("birth_date").isNotNull()
            & F.col("hire_date").isNotNull()
            & (F.col("hire_date") < F.col("birth_date")),
            1
        ).otherwise(0)
    ).alias("hire_before_birth"),

    F.sum(
        F.when(
            F.col("termination_date").isNotNull()
            & F.col("hire_date").isNotNull()
            & (F.col("termination_date") < F.col("hire_date")),
            1
        ).otherwise(0)
    ).alias("termination_before_hire")
).first()

duplicate_key_rows = (
    employee_checks["row_count"]
    - employee_checks["distinct_employee_ids"]
)

print("Silver employee rows:", employee_checks["row_count"])
print("Distinct employee IDs:", employee_checks["distinct_employee_ids"])
print("Duplicate key rows:", duplicate_key_rows)
print("Null or blank employee IDs:", employee_checks["invalid_employee_ids"])
print("NULL birth dates:", employee_checks["birth_date_nulls"])
print("NULL hire dates:", employee_checks["hire_date_nulls"])
print("Hire-before-birth violations:", employee_checks["hire_before_birth"])
print(
    "Termination-before-hire violations:",
    employee_checks["termination_before_hire"]
)

if employee_checks["row_count"] != 10000:
    raise ValueError("Unexpected Silver employee row count.")

if duplicate_key_rows != 0 or employee_checks["invalid_employee_ids"] != 0:
    raise ValueError("Employee ID integrity validation failed.")

if (
    employee_checks["hire_before_birth"] != 0
    or employee_checks["termination_before_hire"] != 0
):
    raise ValueError("Employee date validation failed.")

print("STATUS: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

branch_events_stage_df = spark.sql("""
SELECT
    branch_lifecycle_event_id,
    pharmacy_id,
    event_date,
    event_type,
    previous_value,
    branch_type,
    operating_profile_id,
    source_system
FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`erp_branch_lifecycle_events`
""")

event_key_profile = branch_events_stage_df.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("branch_lifecycle_event_id").alias("distinct_event_ids"),
    F.sum(
        F.when(
            F.col("branch_lifecycle_event_id").isNull()
            | (F.trim(F.col("branch_lifecycle_event_id")) == ""),
            1
        ).otherwise(0)
    ).alias("invalid_event_ids")
).first()

print("Branch lifecycle source rows:", event_key_profile["row_count"])
print("Distinct event IDs:", event_key_profile["distinct_event_ids"])
print("NULL or blank event IDs:", event_key_profile["invalid_event_ids"])

event_semantics_df = (
    branch_events_stage_df
        .groupBy("event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(
                F.when(F.col("previous_value").isNull(), 1).otherwise(0)
            ).alias("previous_value_nulls")
        )
        .orderBy("event_type")
)

display(event_semantics_df)
display(branch_events_stage_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = "silver.erp_branch_lifecycle_events"

target_exists = (
    spark.sql("SHOW TABLES IN silver")
         .filter("tableName = 'erp_branch_lifecycle_events'")
         .limit(1)
         .count() > 0
)

if target_exists:
    raise RuntimeError(
        f"{target_table} already exists. No overwrite was performed."
    )

source_rows = branch_events_stage_df.count()

if source_rows != 1081:
    raise ValueError(
        f"Expected 1081 source rows, found {source_rows}"
    )

source_profile = {
    row["event_type"]: (
        row["event_count"],
        row["previous_value_nulls"]
    )
    for row in event_semantics_df.collect()
}

branch_events_stage_df.write \
    .format("delta") \
    .mode("errorifexists") \
    .saveAsTable(target_table)

branch_events_silver_df = spark.table(target_table)

silver_rows = branch_events_silver_df.count()
silver_distinct_ids = branch_events_silver_df.select(
    "branch_lifecycle_event_id"
).distinct().count()

silver_semantics_df = (
    branch_events_silver_df
        .groupBy("event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.sum(
                F.when(
                    F.col("previous_value").isNull(), 1
                ).otherwise(0)
            ).alias("previous_value_nulls")
        )
)

silver_profile = {
    row["event_type"]: (
        row["event_count"],
        row["previous_value_nulls"]
    )
    for row in silver_semantics_df.collect()
}

print("Created Silver table:", target_table)
print("Source rows:", source_rows)
print("Silver rows:", silver_rows)
print("Distinct Silver event IDs:", silver_distinct_ids)
print("Event profile preserved:", source_profile == silver_profile)

display(silver_semantics_df.orderBy("event_type"))

if silver_rows != source_rows:
    raise ValueError("Row-count validation failed.")

if silver_distinct_ids != 1081:
    raise ValueError("Event ID uniqueness validation failed.")

if source_profile != silver_profile:
    raise ValueError("Event-type or previous_value preservation failed.")

print("STATUS: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

job_roles_stage_df = spark.sql("""
SELECT
    job_role_id,
    job_role_name
FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`erp_job_roles`
""")

role_key_metrics = job_roles_stage_df.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("job_role_id").alias("distinct_role_ids"),
    F.sum(
        F.when(
            F.col("job_role_id").isNull()
            | (F.trim(F.col("job_role_id")) == ""),
            1
        ).otherwise(0)
    ).alias("invalid_role_ids")
).first()

duplicate_role_id_groups = (
    job_roles_stage_df
        .groupBy("job_role_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

employee_role_ids = (
    spark.table("silver.erp_employees")
        .select(
            F.trim(F.col("primary_job_role_id")).alias("job_role_id")
        )
        .filter(
            F.col("job_role_id").isNotNull()
            & (F.col("job_role_id") != "")
        )
        .distinct()
)

known_role_ids = (
    job_roles_stage_df
        .select(F.trim(F.col("job_role_id")).alias("job_role_id"))
        .filter(
            F.col("job_role_id").isNotNull()
            & (F.col("job_role_id") != "")
        )
        .distinct()
)

orphan_employee_roles_df = employee_role_ids.join(
    known_role_ids,
    on="job_role_id",
    how="left_anti"
)

print("Job-role source rows:", role_key_metrics["row_count"])
print("Distinct role IDs:", role_key_metrics["distinct_role_ids"])
print("NULL or blank role IDs:", role_key_metrics["invalid_role_ids"])
print("Duplicate role-ID groups:", duplicate_role_id_groups)
print("Distinct employee roles:", employee_role_ids.count())
print("Employee roles missing from reference:", orphan_employee_roles_df.count())

display(job_roles_stage_df.orderBy("job_role_id"))
display(orphan_employee_roles_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = "silver.erp_job_roles"

target_exists = (
    spark.sql("SHOW TABLES IN silver")
         .filter("tableName = 'erp_job_roles'")
         .limit(1)
         .count() > 0
)

if target_exists:
    raise RuntimeError(
        f"{target_table} already exists. No overwrite was performed."
    )

job_roles_stage_df.write \
    .format("delta") \
    .mode("errorifexists") \
    .saveAsTable(target_table)

job_roles_silver_df = spark.table(target_table)

source_rows = job_roles_stage_df.count()
silver_rows = job_roles_silver_df.count()

source_only_rows = job_roles_stage_df.exceptAll(
    job_roles_silver_df
).count()

silver_only_rows = job_roles_silver_df.exceptAll(
    job_roles_stage_df
).count()

print("Created Silver table:", target_table)
print("Source rows:", source_rows)
print("Silver rows:", silver_rows)
print("Rows missing from Silver:", source_only_rows)
print("Unexpected Silver rows:", silver_only_rows)

if source_rows != 10 or silver_rows != source_rows:
    raise ValueError("Job-role row-count validation failed.")

if source_only_rows != 0 or silver_only_rows != 0:
    raise ValueError("Job-role source-to-Silver comparison failed.")

print("STATUS: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
