# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c353d594-1dea-4854-bbee-ea4e6fe22064",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "f12cee0f-bc6d-49ed-88bd-125dbae6fa11",
# META       "known_lakehouses": [
# META         {
# META           "id": "c353d594-1dea-4854-bbee-ea4e6fe22064"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# CELL 01 — NOTEBOOK SETUP & IMPORTS
# ============================================================
# Run this cell after every new or restarted Spark session.
# It contains the shared imports required by this notebook.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

print("✅ Notebook environment initialized.")
print("ℹ️ Shared PySpark imports are ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# > **Runtime Note**
# >
# > After starting or restarting the Fabric Spark session, run
# > **Cell 01 — Notebook Setup & Imports** before executing individual
# > downstream cells.
# >
# > Persisted Lakehouse data is not affected by a Spark session restart,
# > but in-memory Python variables and imports must be recreated.

# CELL ********************

# Discover Bronze tables directly from the Lakehouse catalog

bronze_catalog_df = (
    spark.sql("SHOW TABLES IN bronze")
        .filter(F.col("isTemporary") == False)
        .select("namespace", "tableName")
        .orderBy("tableName")
)

bronze_table_count = bronze_catalog_df.count()

print(f"Bronze tables discovered: {bronze_table_count}")

display(bronze_catalog_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build a complete schema inventory for all Bronze tables

bronze_table_names = [
    f"bronze.{row['tableName']}"
    for row in bronze_catalog_df.collect()
]

schema_inventory = []

for full_table_name in bronze_table_names:

    df = spark.table(full_table_name)

    table_name = full_table_name.split(".")[-1]

    # Domain is derived from the table naming convention
    domain = table_name.split("_")[0]

    for position, field in enumerate(df.schema.fields, start=1):

        schema_inventory.append({
            "domain": domain,
            "bronze_table": full_table_name,
            "column_position": position,
            "column_name": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable
        })

schema_inventory_df = (
    spark.createDataFrame(schema_inventory)
         .orderBy(
             "domain",
             "bronze_table",
             "column_position"
         )
)

print(f"Bronze tables : {len(bronze_table_names)}")
print(f"Columns       : {schema_inventory_df.count()}")

display(schema_inventory_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from collections import defaultdict

# Detect tables whose names end with a year (2020-2029),
# optionally followed by _final

yearly_groups = defaultdict(list)

for table_name in bronze_table_names:

    base_name = re.sub(
        r"_(20\d{2})(?:_final)?$",
        "",
        table_name
    )

    if base_name != table_name:
        yearly_groups[base_name].append(table_name)

# Keep only entities represented by multiple yearly tables
yearly_groups = {
    entity: sorted(tables)
    for entity, tables in yearly_groups.items()
    if len(tables) > 1
}

print(f"Yearly entity groups discovered: {len(yearly_groups)}")

for entity, tables in yearly_groups.items():
    print(f"\n{entity}")
    for table in tables:
        print(f"  -> {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compare schemas across all yearly table groups

schema_comparison_results = []

for entity, tables in yearly_groups.items():

    reference_table = tables[0]
    reference_df = spark.table(reference_table)

    reference_schema = {
        field.name: field.dataType.simpleString()
        for field in reference_df.schema.fields
    }

    for table in tables:

        df = spark.table(table)

        current_schema = {
            field.name: field.dataType.simpleString()
            for field in df.schema.fields
        }

        missing_columns = sorted(
            set(reference_schema) - set(current_schema)
        )

        extra_columns = sorted(
            set(current_schema) - set(reference_schema)
        )

        type_mismatches = sorted([
            column
            for column in set(reference_schema) & set(current_schema)
            if reference_schema[column] != current_schema[column]
        ])

        status = (
            "PASS"
            if not missing_columns
            and not extra_columns
            and not type_mismatches
            else "SCHEMA_DRIFT"
        )

        schema_comparison_results.append({
            "entity": entity,
            "reference_table": reference_table,
            "table": table,
            "missing_columns": ", ".join(missing_columns) or "None",
            "extra_columns": ", ".join(extra_columns) or "None",
            "type_mismatches": ", ".join(type_mismatches) or "None",
            "status": status
        })

schema_comparison_df = spark.createDataFrame(
    schema_comparison_results
).orderBy("entity", "table")

display(schema_comparison_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    schema_comparison_df
        .groupBy("entity", "status")
        .count()
        .orderBy("entity", "status")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build the initial Bronze-to-Silver entity map

yearly_table_lookup = {
    table: entity
    for entity, tables in yearly_groups.items()
    for table in tables
}

silver_entity_map = []

for bronze_table in bronze_table_names:

    if bronze_table in yearly_table_lookup:
        silver_entity = yearly_table_lookup[bronze_table]
        mapping_type = "YEARLY_UNION"
    else:
        silver_entity = bronze_table
        mapping_type = "ONE_TO_ONE"

    silver_entity_map.append({
        "bronze_table": bronze_table,
        "silver_entity_candidate": silver_entity.replace(
            "bronze.", "silver."
        ),
        "mapping_type": mapping_type
    })

silver_entity_map_df = spark.createDataFrame(
    silver_entity_map
)

display(
    silver_entity_map_df
        .groupBy("mapping_type")
        .agg(
            F.count("*").alias("bronze_tables"),
            F.countDistinct("silver_entity_candidate").alias(
                "silver_entity_candidates"
            )
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build one record per candidate Silver entity

silver_candidates_df = (
    silver_entity_map_df
        .groupBy(
            "silver_entity_candidate",
            "mapping_type"
        )
        .agg(
            F.count("*").alias("source_table_count"),
            F.sort_array(
                F.collect_list("bronze_table")
            ).alias("source_tables")
        )
        .orderBy(
            "mapping_type",
            "silver_entity_candidate"
        )
)

candidate_count = silver_candidates_df.count()

print(f"Candidate Silver entities: {candidate_count}")

display(silver_candidates_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def classify_column(column_name, data_type):
    name = column_name.lower()
    dtype = data_type.lower()

    # Date and timestamp columns
    if dtype.startswith(("date", "timestamp")):
        return "DATE_TIME"

    # Business identifiers should be classified by semantic meaning,
    # not only by their physical storage type
    if (
        name.endswith("_id")
        or name.endswith("_key")
        or name.endswith("_code")
    ):
        return "IDENTIFIER"

    # True numeric measures and attributes
    if dtype.startswith(
        ("int", "bigint", "smallint", "tinyint",
         "float", "double", "decimal")
    ):
        return "NUMERIC"

    # General text attributes
    if dtype.startswith("string"):
        return "STRING"

    return "OTHER"


# Apply the classification to the complete Bronze schema inventory
classify_column_udf = F.udf(
    classify_column,
    T.StringType()
)

classified_schema_df = (
    schema_inventory_df
        .withColumn(
            "column_class",
            classify_column_udf(
                F.col("column_name"),
                F.col("data_type")
            )
        )
)


# Validate the classification result
print(f"Columns classified: {classified_schema_df.count()}")

display(
    classified_schema_df
        .groupBy("column_class")
        .count()
        .orderBy(F.desc("count"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generic metadata-driven profiler
# One aggregation per table instead of one Spark job per column

def profile_table(table_name):

    df = spark.table(table_name)

    schema_rows = (
        classified_schema_df
            .filter(F.col("bronze_table") == table_name)
            .select(
                "column_name",
                "column_class"
            )
            .collect()
    )

    expressions = []

    for row in schema_rows:

        col_name = row["column_name"]
        col_class = row["column_class"]

        # NULL count for every column
        expressions.append(
            F.sum(
                F.when(F.col(col_name).isNull(), 1).otherwise(0)
            ).alias(f"{col_name}__null_count")
        )

        # Empty / whitespace strings
        if col_class in ("STRING", "IDENTIFIER"):
            expressions.append(
                F.sum(
                    F.when(
                        F.trim(F.col(col_name).cast("string")) == "",
                        1
                    ).otherwise(0)
                ).alias(f"{col_name}__empty_count")
            )

        # Range profiling
        if col_class in ("NUMERIC", "DATE_TIME"):
            expressions.append(
                F.min(F.col(col_name)).alias(
                    f"{col_name}__min"
                )
            )

            expressions.append(
                F.max(F.col(col_name)).alias(
                    f"{col_name}__max"
                )
            )

    result = (
        df.agg(
            F.count("*").alias("__row_count"),
            *expressions
        )
        .first()
        .asDict()
    )

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_table = "bronze.reference_regions"

test_profile = profile_table(test_table)

print(f"Table     : {test_table}")
print(f"Row count : {test_profile['__row_count']}")

for metric, value in test_profile.items():
    if metric != "__row_count":
        print(f"{metric}: {value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def profile_to_long(table_name, profile):

    schema_rows = (
        classified_schema_df
            .filter(F.col("bronze_table") == table_name)
            .select(
                "domain",
                "column_name",
                "data_type",
                "column_class"
            )
            .collect()
    )

    row_count = profile["__row_count"]
    rows = []

    for column in schema_rows:

        column_name = column["column_name"]
        column_class = column["column_class"]

        rows.append({
            "domain": column["domain"],
            "bronze_table": table_name,
            "column_name": column_name,
            "data_type": column["data_type"],
            "column_class": column_class,
            "row_count": row_count,

            "null_count":
                profile.get(f"{column_name}__null_count"),

            "empty_count":
                profile.get(f"{column_name}__empty_count"),

            "min_value":
                str(profile.get(f"{column_name}__min"))
                if profile.get(f"{column_name}__min") is not None
                else None,

            "max_value":
                str(profile.get(f"{column_name}__max"))
                if profile.get(f"{column_name}__max") is not None
                else None
        })

    return rows


# Convert the smoke-test result to long format
test_profile_rows = profile_to_long(
    test_table,
    test_profile
)

print(f"Test profile rows: {len(test_profile_rows)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

profile_output_schema = T.StructType([
    T.StructField("domain", T.StringType(), False),
    T.StructField("bronze_table", T.StringType(), False),
    T.StructField("column_name", T.StringType(), False),
    T.StructField("data_type", T.StringType(), False),
    T.StructField("column_class", T.StringType(), False),

    T.StructField("row_count", T.LongType(), False),
    T.StructField("null_count", T.LongType(), True),
    T.StructField("empty_count", T.LongType(), True),

    T.StructField("min_value", T.StringType(), True),
    T.StructField("max_value", T.StringType(), True)
])

test_profile_df = spark.createDataFrame(
    test_profile_rows,
    schema=profile_output_schema
)

display(test_profile_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
from tqdm.auto import tqdm

all_profile_rows = []
profiling_errors = []

profiling_start = time.perf_counter()

for table_name in tqdm(
    bronze_table_names,
    total=len(bronze_table_names),
    desc="Bronze profiling"
):
    try:
        profile = profile_table(table_name)

        table_rows = profile_to_long(
            table_name,
            profile
        )

        all_profile_rows.extend(table_rows)

    except Exception as e:
        profiling_errors.append({
            "bronze_table": table_name,
            "error": str(e)
        })

profiling_duration = round(
    time.perf_counter() - profiling_start,
    2
)

print(f"Tables attempted : {len(bronze_table_names)}")
print(f"Profile rows     : {len(all_profile_rows)}")
print(f"Errors           : {len(profiling_errors)}")
print(f"Runtime          : {profiling_duration} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_profile_df = spark.createDataFrame(
    all_profile_rows,
    schema=profile_output_schema
)

print(f"Profile records : {bronze_profile_df.count()}")

display(
    bronze_profile_df
        .groupBy("domain")
        .agg(
            F.countDistinct("bronze_table").alias("tables"),
            F.count("*").alias("columns"),
            F.sum("null_count").alias("total_null_values"),
            F.sum(
                F.coalesce(
                    F.col("empty_count"),
                    F.lit(0)
                )
            ).alias("total_empty_values")
        )
        .orderBy("domain")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_findings_df = (
    bronze_profile_df
        .filter(F.col("null_count") > 0)
        .withColumn(
            "null_percentage",
            F.round(
                F.col("null_count") * 100.0 /
                F.col("row_count"),
                4
            )
        )
        .select(
            "domain",
            "bronze_table",
            "column_name",
            "column_class",
            "row_count",
            "null_count",
            "null_percentage"
        )
        .orderBy(
            F.desc("null_percentage"),
            "domain",
            "bronze_table",
            "column_name"
        )
)

print(
    f"Columns containing NULLs: "
    f"{null_findings_df.count()}"
)

display(null_findings_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_findings_df = (
    bronze_profile_df
        .filter(F.col("column_class") == "DATE_TIME")
        .select(
            "domain",
            "bronze_table",
            "column_name",
            "row_count",
            "null_count",
            "min_value",
            "max_value"
        )
        .orderBy(
            "domain",
            "bronze_table",
            "column_name"
        )
)

print(
    f"Date/time columns profiled: "
    f"{date_findings_df.count()}"
)

display(date_findings_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summarize findings that may require a Silver decision

dq_findings_df = (
    bronze_profile_df
        .withColumn(
            "null_percentage",
            F.round(
                F.col("null_count") * 100.0 /
                F.col("row_count"),
                4
            )
        )
        .withColumn(
            "empty_count",
            F.coalesce(
                F.col("empty_count"),
                F.lit(0)
            )
        )
        .withColumn(
            "finding_type",
            F.when(
                F.col("null_percentage") == 100,
                "ALL_NULL"
            )
            .when(
                F.col("null_count") > 0,
                "HAS_NULLS"
            )
            .when(
                F.col("empty_count") > 0,
                "HAS_EMPTY_VALUES"
            )
            .otherwise("CLEAN_BASIC_PROFILE")
        )
)

display(
    dq_findings_df
        .filter(
            F.col("finding_type") != "CLEAN_BASIC_PROFILE"
        )
        .select(
            "domain",
            "bronze_table",
            "column_name",
            "column_class",
            "row_count",
            "null_count",
            "null_percentage",
            "empty_count",
            "finding_type"
        )
        .orderBy(
            F.desc("null_percentage"),
            "domain",
            "bronze_table"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    dq_findings_df
        .groupBy("finding_type")
        .count()
        .orderBy("finding_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Collect the small DQ finding set once to the driver

finding_rows = (
    null_findings_df
        .select(
            "bronze_table",
            "column_name"
        )
        .collect()
)

# Group findings by table locally
findings_by_table = {}

for row in finding_rows:
    findings_by_table.setdefault(
        row["bronze_table"],
        []
    ).append(row["column_name"])

print(f"Finding columns : {len(finding_rows)}")
print(f"Affected tables : {len(findings_by_table)}")

# Inspect the surrounding schema for each affected table
for table_name, finding_columns in sorted(
    findings_by_table.items()
):

    table_columns = spark.table(table_name).columns

    print("\n" + "=" * 80)
    print(f"TABLE    : {table_name}")
    print(f"FINDINGS : {', '.join(finding_columns)}")
    print(f"COLUMNS  : {', '.join(table_columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Discover business-state values around the current DQ findings

context_columns = {
    "bronze.crm_customer_service_interactions": [
        "status_id",
        "interaction_type_id",
        "service_channel_id",
        "priority"
    ],

    "bronze.delivery_deliveries": [
        "delivery_status",
        "delivery_method_id"
    ],

    "bronze.erp_branch_lifecycle_events": [
        "event_type"
    ],

    "bronze.insurance_insurance_claims_fixed": [
        "claim_status_id"
    ],

    "bronze.marketing_promotions": [
        "scope_type",
        "promotion_type_id",
        "loyalty_only"
    ],

    "bronze.pos_sales_transactions_2020": [
        "transaction_status",
        "channel_id",
        "identity_classification"
    ],

    "bronze.rx_prescriptions_fixed": [
        "prescription_status",
        "prescription_source"
    ]
}

for table_name, columns in context_columns.items():

    print("\n" + "=" * 80)
    print(f"TABLE: {table_name}")

    df = spark.table(table_name)

    for column_name in columns:

        print(f"\n{column_name}")

        (
            df.groupBy(column_name)
              .count()
              .orderBy(F.desc("count"))
              .show(50, truncate=False)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Profile NULL findings by known business state

conditional_checks = [
    {
        "table": "bronze.delivery_deliveries",
        "state_column": "delivery_status",
        "finding_columns": [
            "delivered_timestamp",
            "actual_minutes"
        ]
    },
    {
        "table": "bronze.marketing_promotions",
        "state_column": "scope_type",
        "finding_columns": [
            "category_scope"
        ]
    },
    {
        "table": "bronze.rx_prescriptions_fixed",
        "state_column": "prescription_status",
        "finding_columns": [
            "linked_transaction_id"
        ]
    },
    {
        "table": "bronze.pos_sales_transactions_2020",
        "state_column": "transaction_status",
        "finding_columns": [
            "customer_id"
        ]
    }
]

for check in conditional_checks:

    table_name = check["table"]
    state_column = check["state_column"]
    finding_columns = check["finding_columns"]

    df = spark.table(table_name)

    print("\n" + "=" * 80)
    print(f"TABLE : {table_name}")
    print(f"STATE : {state_column}")

    aggregations = [
        F.count("*").alias("row_count")
    ]

    for column_name in finding_columns:
        aggregations.append(
            F.sum(
                F.when(
                    F.col(column_name).isNull(),
                    1
                ).otherwise(0)
            ).alias(f"{column_name}__null_count")
        )

    result_df = (
        df.groupBy(state_column)
          .agg(*aggregations)
          .orderBy(state_column)
    )

    display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Discover reference and master tables relevant to unresolved business codes

search_terms = [
    "customer",
    "status",
    "claim",
    "insurance",
    "interaction",
    "channel",
    "reason"
]

candidate_reference_tables = [
    table_name
    for table_name in bronze_table_names
    if any(
        term in table_name.lower()
        for term in search_terms
    )
]

print(
    f"Candidate reference/master tables: "
    f"{len(candidate_reference_tables)}"
)

for table_name in sorted(candidate_reference_tables):
    print(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Decode unresolved business codes from authoritative Bronze reference tables

reference_tables = [
    "bronze.crm_customer_service_reference",
    "bronze.insurance_claim_statuses",
    "bronze.insurance_claim_rejection_reasons"
]

for table_name in reference_tables:

    print("\n" + "=" * 80)
    print(f"REFERENCE TABLE: {table_name}")

    df = spark.table(table_name)

    print(f"Rows    : {df.count()}")
    print(f"Columns : {', '.join(df.columns)}")

    display(df.orderBy(*df.columns[:1]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 25 — CRM & INSURANCE CONDITIONAL BUSINESS EVIDENCE
# ============================================================

# ------------------------------------------------------------
# 1. CRM SERVICE INTERACTIONS
# ------------------------------------------------------------

crm_df = spark.table(
    "bronze.crm_customer_service_interactions"
)

crm_result = (
    crm_df
        .groupBy("status_id")
        .agg(
            F.count("*").alias("row_count"),

            F.sum(
                F.when(
                    F.col("closed_timestamp").isNull(),
                    1
                ).otherwise(0)
            ).alias("closed_timestamp_null"),

            F.sum(
                F.when(
                    F.col("resolution_code").isNull(),
                    1
                ).otherwise(0)
            ).alias("resolution_code_null"),

            F.sum(
                F.when(
                    F.col("satisfaction_score").isNull(),
                    1
                ).otherwise(0)
            ).alias("satisfaction_score_null"),

            F.sum(
                F.when(
                    F.col("pharmacy_id").isNull(),
                    1
                ).otherwise(0)
            ).alias("pharmacy_id_null")
        )
        .orderBy("status_id")
)

print("=" * 80)
print("CRM — NULL findings by business status")
print("CS01 = RESOLVED")
print("CS02 = CLOSED_NO_ACTION")
print("CS03 = ESCALATED")
print("CS04 = OPEN")

display(crm_result)


# ------------------------------------------------------------
# 2. INSURANCE CLAIMS
# ------------------------------------------------------------

claims_df = spark.table(
    "bronze.insurance_insurance_claims_fixed"
)

claims_result = (
    claims_df
        .groupBy("claim_status_id")
        .agg(
            F.count("*").alias("row_count"),

            F.sum(
                F.when(
                    F.col("rejection_reason_id").isNull(),
                    1
                ).otherwise(0)
            ).alias("rejection_reason_null"),

            F.sum(
                F.when(
                    F.col("rejection_reason_id").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("rejection_reason_present")
        )
        .orderBy("claim_status_id")
)

print("\n" + "=" * 80)
print("INSURANCE — Rejection reason by claim status")
print("CL01 = SUBMITTED")
print("CL02 = UNDER_REVIEW")
print("CL03 = APPROVED")
print("CL04 = PARTIALLY_APPROVED")
print("CL05 = REJECTED")
print("CL06 = SETTLED")
print("CL07 = CANCELLED")
print("CL08 = RESUBMITTED")

display(claims_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 26 — INSURANCE REJECTION REASON INVESTIGATION
# ============================================================

claims_df = spark.table(
    "bronze.insurance_insurance_claims_fixed"
)

reasons_df = spark.table(
    "bronze.insurance_claim_rejection_reasons"
)

# Inspect the reference schema first
print("=" * 80)
print("REJECTION REASON REFERENCE")
print(f"Columns: {', '.join(reasons_df.columns)}")

display(reasons_df.orderBy("rejection_reason_id"))


# ------------------------------------------------------------
# Distribution of rejection reasons across claim statuses
# ------------------------------------------------------------

reason_by_status_df = (
    claims_df
        .filter(
            F.col("rejection_reason_id").isNotNull()
        )
        .groupBy(
            "claim_status_id",
            "rejection_reason_id"
        )
        .agg(
            F.count("*").alias("claim_count")
        )
        .orderBy(
            "claim_status_id",
            F.desc("claim_count")
        )
)

print("\n" + "=" * 80)
print("REJECTION REASONS BY CLAIM STATUS")

display(reason_by_status_df)


# ------------------------------------------------------------
# Explicitly test the expected business relationship
# ------------------------------------------------------------

insurance_rule_check_df = (
    claims_df
        .agg(

            # Rejected claims without a rejection reason
            F.sum(
                F.when(
                    (F.col("claim_status_id") == "CL05") &
                    (F.col("rejection_reason_id").isNull()),
                    1
                ).otherwise(0)
            ).alias(
                "rejected_without_reason"
            ),

            # Non-rejected claims carrying a rejection reason
            F.sum(
                F.when(
                    (F.col("claim_status_id") != "CL05") &
                    (F.col("rejection_reason_id").isNotNull()),
                    1
                ).otherwise(0)
            ).alias(
                "non_rejected_with_reason"
            ),

            # Rejected claims with a reason
            F.sum(
                F.when(
                    (F.col("claim_status_id") == "CL05") &
                    (F.col("rejection_reason_id").isNotNull()),
                    1
                ).otherwise(0)
            ).alias(
                "rejected_with_reason"
            )
        )
)

print("\n" + "=" * 80)
print("EXPECTED BUSINESS RULE TEST")
print(
    "Hypothesis: REJECTED claims should carry "
    "a rejection reason."
)

display(insurance_rule_check_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 27 — INSURANCE CLAIM-LINE SEMANTIC DISCOVERY
# ============================================================

claim_lines_table = (
    "bronze.insurance_insurance_claim_lines_final"
)

claim_lines_df = spark.table(claim_lines_table)

print("=" * 80)
print("CLAIM LINE STRUCTURE")
print(f"Table   : {claim_lines_table}")
print(f"Columns : {len(claim_lines_df.columns)}")

for column_name in claim_lines_df.columns:
    print(column_name)


# ------------------------------------------------------------
# Inspect a small evidence sample
# ------------------------------------------------------------

print("\n" + "=" * 80)
print("CLAIM LINE SAMPLE")

display(
    claim_lines_df.limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🛡️ Silver Decision — SIL-DQ-INS-001
# 
# ### Finding
# 
# The Bronze claim header contains `rejection_reason_id`, but its population
# is not consistently aligned with the current `claim_status_id`.
# 
# Observed evidence showed that rejection reasons occur across multiple
# claim statuses, while many `REJECTED` claims do not contain a documented
# rejection reason.
# 
# Claim-line discovery confirmed that insurance claim lines separately store:
# 
# - `claimed_amount_sar`
# - `approved_amount_sar`
# - `rejected_amount_sar`
# 
# This supports partial financial adjudication at claim-line grain.
# 
# ---
# 
# ### Business Interpretation
# 
# `claim_status_id` represents the current claim lifecycle state.
# 
# An existing rejection reason may also provide useful context from an earlier
# rejection/follow-up process, particularly for:
# 
# - `UNDER_REVIEW`
# - `RESUBMITTED`
# - `CANCELLED`
# 
# A `CANCELLED` claim is interpreted as cancellation of an active/in-process
# claim. A terminal `REJECTED` claim is not assumed to transition directly
# to `CANCELLED`.
# 
# The preserved rejection reason on a cancelled claim must therefore NOT be
# interpreted as the cancellation reason.
# 
# ---
# 
# ### Silver v1 Decision
# 
# | Claim Status | Existing Reason | Missing Reason |
# |---|---|---|
# | SUBMITTED | Preserve | NOT_APPLICABLE |
# | UNDER_REVIEW | Preserve | NOT_APPLICABLE |
# | APPROVED | NOT_APPLICABLE | NOT_APPLICABLE |
# | PARTIALLY_APPROVED | Preserve | NOT_MENTIONED |
# | REJECTED | Preserve | NOT_MENTIONED |
# | SETTLED | NOT_APPLICABLE | NOT_APPLICABLE |
# | CANCELLED | Preserve | NOT_APPLICABLE |
# | RESUBMITTED | Preserve | NOT_APPLICABLE |
# 
# `NOT_APPLICABLE` and `NOT_MENTIONED` are intentionally different business
# states.
# 
# - `NOT_APPLICABLE` = a rejection reason is not required for the interpreted state.
# - `NOT_MENTIONED` = rejection exists or is implied, but the source does not
#   provide its reason.
# 
# ---
# 
# ### Bronze Policy
# 
# Bronze remains unchanged and preserves the original source value.
# 
# The Silver transformation is therefore reversible.
# 
# ---
# 
# ### Decision Status
# 
# **Decision ID:** SIL-DQ-INS-001  
# **Version:** 1.0  
# **Status:** APPROVED FOR SILVER  
# **Implementation:** PENDING  
# **Review Trigger:** Better source-system documentation, domain evidence,
# or lifecycle history becomes available.


# CELL ********************

# ============================================================
# CELL 28 — CRM REMAINING NULL SEMANTICS
# ============================================================



crm_df = spark.table(
    "bronze.crm_customer_service_interactions"
)

crm_context_df = (
    crm_df
        .groupBy(
            "interaction_type_id",
            "service_channel_id"
        )
        .agg(
            F.count("*").alias("row_count"),

            F.sum(
                F.when(
                    F.col("pharmacy_id").isNull(),
                    1
                ).otherwise(0)
            ).alias("pharmacy_id_null"),

            F.sum(
                F.when(
                    F.col("satisfaction_score").isNull(),
                    1
                ).otherwise(0)
            ).alias("satisfaction_score_null")
        )
        .withColumn(
            "pharmacy_null_pct",
            F.round(
                100.0 * F.col("pharmacy_id_null")
                / F.col("row_count"),
                2
            )
        )
        .withColumn(
            "satisfaction_null_pct",
            F.round(
                100.0 * F.col("satisfaction_score_null")
                / F.col("row_count"),
                2
            )
        )
        .orderBy(
            "interaction_type_id",
            "service_channel_id"
        )
)

display(crm_context_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 29 — PICKUP / FULFILLMENT CONTEXT DISCOVERY
# ============================================================
# Self-contained metadata discovery.
# Does not depend on previous notebook variables.
# ============================================================

search_terms = [
    "pickup",
    "pick_up",
    "collect",
    "fulfillment",
    "fulfilment",
    "delivery",
    "channel",
    "pharmacy",
    "branch"
]

# Discover Bronze tables directly from the Lakehouse catalog
bronze_tables = [
    row.tableName
    for row in spark.sql("SHOW TABLES IN bronze").collect()
]

matches = []

# Inspect table schemas only — no full data scan
for table_name in bronze_tables:
    for field in spark.table(f"bronze.{table_name}").schema.fields:

        column_name = field.name.lower()

        if any(term in column_name for term in search_terms):
            matches.append(
                (
                    table_name,
                    field.name,
                    field.dataType.simpleString()
                )
            )

# Display results
pickup_context_df = spark.createDataFrame(
    matches,
    ["table_name", "column_name", "data_type"]
).orderBy(
    "table_name",
    "column_name"
)

print(f"Bronze tables inspected : {len(bronze_tables)}")
print(f"Candidate columns found : {len(matches)}")

display(pickup_context_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 29 — POS CUSTOMER IDENTITY BY CHANNEL
# ============================================================
# Business question:
# Can a digital POS transaction exist without customer identity?
# ============================================================

pos_2020_df = spark.table(
    "bronze.pos_sales_transactions_2020"
)

customer_channel_profile = (
    pos_2020_df
        .groupBy("channel_id")
        .agg(
            F.count("*").alias("transaction_count"),

            F.sum(
                F.when(
                    F.col("customer_id").isNull(),
                    1
                ).otherwise(0)
            ).alias("customer_id_null"),

            F.sum(
                F.when(
                    F.col("customer_id").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("customer_id_present")
        )
        .withColumn(
            "customer_null_pct",
            F.round(
                100.0
                * F.col("customer_id_null")
                / F.col("transaction_count"),
                2
            )
        )
        .orderBy("channel_id")
)

display(customer_channel_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🛡️ Silver Decision — SIL-DQ-ERP-001
# 
# ### Finding
# 
# Three nullable ERP lifecycle attributes were investigated:
# 
# - `erp_pharmacies.closing_date`
# - `erp_employees.termination_date`
# - `erp_branch_lifecycle_events.previous_value`
# 
# ### Evidence
# 
# #### Pharmacy Lifecycle
# 
# 1,000 pharmacies were evaluated.
# 
# - `closing_date` is NULL for all 1,000 pharmacies.
# - No `closing_date < opening_date` violation was found.
# 
# The source `closing_date` therefore contains no usable closure information
# in the current dataset version.
# 
# #### Employee Lifecycle
# 
# 10,000 employees were evaluated.
# 
# - 8,360 employees have no recorded `termination_date`.
# - `hire_date < birth_date` violations: 0.
# - `termination_date < hire_date` violations: 0.
# 
# #### Branch Lifecycle Events
# 
# `OPENED`:
# - 1,000 events.
# - 100% have NULL `previous_value`.
# 
# `OPERATING_PROFILE_REVIEW`:
# - 81 events.
# - 0% have NULL `previous_value`.
# 
# The NULL pattern is consistent with lifecycle semantics.
# 
# ---
# 
# ### Silver v1 Decision
# 
# #### Pharmacy `closing_date`
# 
# The source `closing_date` is 100% NULL and provides no usable information
# in the current dataset version.
# 
# Therefore:
# 
# - Preserve the original field in Bronze only.
# - Do not carry the empty `closing_date` column into the Silver v1
#   business-facing pharmacy entity.
# - Do not fabricate an official closure date.
# 
# Silver will instead derive:
# 
# `last_sales_activity_date`
# 
# from POS transactions for each pharmacy.
# 
# This field represents the last observed sales activity and must NOT be
# interpreted as an official closure date.
# 
# A derived `sales_activity_status` may subsequently classify pharmacies
# for analytical use based on the observed activity distribution.
# 
# This allows analytics to distinguish currently active versus non-active
# sales locations without inventing an official management closure decision.
# 
# The original Bronze `closing_date` remains available for lineage and may
# be reintroduced in a future Silver version if authoritative closure data
# becomes available.
# 
# #### Employee `termination_date`
# 
# Preserve NULL `termination_date` as an open-ended employment relationship.
# 
# Validate:
# 
# `birth_date <= hire_date <= termination_date`
# 
# where `termination_date` exists.
# 
# #### Branch Lifecycle `previous_value`
# 
# Preserve NULL `previous_value` for initial `OPENED` events.
# 
# For subsequent lifecycle events, validate the presence of the previous
# state/value where required.
# 
# ---
# 
# ### Data Quality Conclusion
# 
# The investigated ERP NULL patterns do not currently demonstrate a data
# quality defect.
# 
# The 100%-NULL pharmacy `closing_date` is classified as an uninformative
# source attribute for Silver v1 rather than being artificially populated.
# 
# Operational evidence is kept distinct from official source lifecycle
# information.
# 
# ---
# 
# ### Decision Status
# 
# **Decision ID:** SIL-DQ-ERP-001  
# **Version:** 1.1  
# **Status:** APPROVED FOR SILVER  
# **Implementation:** PENDING  
# **Validation:** Discovery checks PASS  
# **Review Trigger:** Authoritative pharmacy closure information becomes
# available or the source `closing_date` begins carrying meaningful values.


# CELL ********************

# ============================================================
# CELL 30 — ERP LIFECYCLE & TEMPORAL EVIDENCE
# ============================================================

pharmacies_df = spark.table("bronze.erp_pharmacies")
employees_df = spark.table("bronze.erp_employees")
lifecycle_df = spark.table("bronze.erp_branch_lifecycle_events")


# 1. Pharmacy lifecycle
pharmacy_profile = pharmacies_df.agg(
    F.count("*").alias("pharmacy_count"),

    F.sum(
        F.when(F.col("closing_date").isNull(), 1).otherwise(0)
    ).alias("closing_date_null"),

    F.sum(
        F.when(
            F.col("closing_date").isNotNull()
            & (F.col("closing_date") < F.col("opening_date")),
            1
        ).otherwise(0)
    ).alias("closing_before_opening")
)

print("=== PHARMACY LIFECYCLE ===")
display(pharmacy_profile)


# 2. Employee lifecycle
employee_profile = employees_df.agg(
    F.count("*").alias("employee_count"),

    F.sum(
        F.when(F.col("termination_date").isNull(), 1).otherwise(0)
    ).alias("termination_date_null"),

    F.sum(
        F.when(F.col("hire_date") < F.col("birth_date"), 1).otherwise(0)
    ).alias("hire_before_birth"),

    F.sum(
        F.when(
            F.col("termination_date").isNotNull()
            & (F.col("termination_date") < F.col("hire_date")),
            1
        ).otherwise(0)
    ).alias("termination_before_hire")
)

print("=== EMPLOYEE LIFECYCLE ===")
display(employee_profile)


# 3. Branch lifecycle event context
branch_event_profile = (
    lifecycle_df
        .groupBy("event_type")
        .agg(
            F.count("*").alias("event_count"),

            F.sum(
                F.when(
                    F.col("previous_value").isNull(),
                    1
                ).otherwise(0)
            ).alias("previous_value_null")
        )
        .withColumn(
            "previous_value_null_pct",
            F.round(
                100.0
                * F.col("previous_value_null")
                / F.col("event_count"),
                2
            )
        )
        .orderBy("event_type")
)

print("=== BRANCH LIFECYCLE EVENTS ===")
display(branch_event_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔍 RX Doctor Activity Period — Temporal Discovery
# 
# ### Objective
# 
# Investigate the high NULL rate in `rx_doctors.active_to`.
# 
# The purpose is not to classify NULL as a defect based on percentage alone.
# Instead, we test whether the field represents an open-ended doctor activity
# period and whether the available dates are temporally consistent.
# 
# ### Questions
# 
# 1. How many doctors have an `active_to` value?
# 2. How many have an open-ended activity period?
# 3. Does any `active_to` occur before `active_from`?
# 4. What is the observed date range?
# 
# This evidence will determine the Silver treatment of the doctor activity
# period.

# CELL ********************

# ============================================================
# CELL 31 — RX DOCTOR ACTIVITY PERIOD DISCOVERY
# ============================================================

doctors_df = spark.table("bronze.rx_doctors")

print("=== RX DOCTORS SCHEMA ===")
doctors_df.printSchema()

# Validate expected temporal columns before profiling.
required_columns = {"active_from", "active_to"}
missing_columns = required_columns - set(doctors_df.columns)

if missing_columns:
    raise ValueError(
        f"Missing expected columns: {sorted(missing_columns)}"
    )

doctor_activity_profile = doctors_df.agg(
    F.count("*").alias("doctor_count"),

    F.sum(
        F.when(F.col("active_from").isNull(), 1).otherwise(0)
    ).alias("active_from_null"),

    F.sum(
        F.when(F.col("active_to").isNull(), 1).otherwise(0)
    ).alias("active_to_null"),

    F.sum(
        F.when(
            F.col("active_from").isNotNull()
            & F.col("active_to").isNotNull()
            & (F.col("active_to") < F.col("active_from")),
            1
        ).otherwise(0)
    ).alias("active_to_before_active_from"),

    F.min("active_from").alias("min_active_from"),
    F.max("active_from").alias("max_active_from"),

    F.min("active_to").alias("min_active_to"),
    F.max("active_to").alias("max_active_to")
)

display(doctor_activity_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🛡️ Silver Decision — SIL-DQ-RX-001
# 
# ### Finding
# 
# `rx_doctors.active_to` contains a high NULL rate.
# 
# ### Evidence
# 
# 20,000 doctor records were evaluated.
# 
# - `active_from` NULL: 0
# - `active_to` NULL: 18,828 (94.14%)
# - `active_to` populated: 1,172 (5.86%)
# - `active_to < active_from` violations: 0
# 
# Observed temporal ranges:
# 
# - `active_from`: 2008-01-01 → 2024-12-31
# - `active_to`: 2020-01-05 → 2025-12-31
# 
# ### Interpretation
# 
# The observed pattern is consistent with an open-ended temporal relationship.
# 
# A NULL `active_to` means that no end date is recorded for the doctor's
# activity period in the available source data.
# 
# The high NULL percentage is therefore not, by itself, a data quality defect.
# 
# ### Silver v1 Decision
# 
# Preserve `active_from`.
# 
# Preserve NULL `active_to` as an open-ended activity period.
# 
# Do not fabricate an end date.
# 
# Validate the temporal rule:
# 
# `active_from <= active_to`
# 
# whenever `active_to` is populated.
# 
# For analytical use, current activity may be derived without modifying
# the source end date semantics.
# 
# ### Data Quality Conclusion
# 
# No temporal violations were detected.
# 
# `active_to` NULL is classified as valid lifecycle semantics rather than
# missing required data.
# 
# ### Bronze Policy
# 
# Bronze remains unchanged.
# 
# ### Decision Status
# 
# **Decision ID:** SIL-DQ-RX-001  
# **Version:** 1.0  
# **Status:** APPROVED FOR SILVER  
# **Implementation:** PENDING  
# **Validation:** Discovery checks PASS  
# **Review Trigger:** Additional authoritative doctor lifecycle rules or
# source-system semantics become available.


# MARKDOWN ********************

# # 🔑 Phase 3 — Key Integrity Discovery
# 
# ## Objective
# 
# Validate whether candidate identifiers can reliably define the grain of
# each Bronze source table before they are promoted into Silver.
# 
# A column ending with `_id` is not automatically a Primary Key.
# 
# Depending on table grain, an identifier may represent:
# 
# - Primary Key
# - Foreign Key
# - Business Key
# - Part of a Composite Key
# - Repeating parent identifier
# - Descriptive identifier
# 
# Therefore, key integrity must be established from evidence rather than
# column naming alone.
# 
# ## Core Primary-Key Tests
# 
# For every candidate key we evaluate:
# 
# 1. NULL values
# 2. Distinct values
# 3. Duplicate values
# 4. Uniqueness ratio
# 
# A strong single-column Primary Key candidate should satisfy:
# 
# `NULL count = 0`
# 
# `Distinct count = Row count`
# 
# `Duplicate rows = 0`
# 
# If no single column satisfies the expected grain, the table may require
# a composite key or further grain analysis.
# 
# ## Engineering Principle
# 
# > Key discovery and key validation are separate problems.
# 
# First discover candidate identifiers automatically.
# 
# Then validate whether each candidate actually represents the expected
# table grain.


# CELL ********************

# ============================================================
# CELL 32 — AUTOMATED KEY CANDIDATE DISCOVERY
# ============================================================

bronze_tables = [
    row.tableName
    for row in spark.sql("SHOW TABLES IN bronze").collect()
]

key_candidates = []

for table_name in bronze_tables:

    full_table_name = f"bronze.{table_name}"
    columns = spark.table(full_table_name).columns

    for column_name in columns:

        column_lower = column_name.lower()

        # Discover identifier-like columns from naming conventions.
        if (
            column_lower == "id"
            or column_lower.endswith("_id")
            or column_lower.endswith("_key")
        ):

            key_candidates.append(
                (
                    full_table_name,
                    column_name
                )
            )

key_candidate_df = spark.createDataFrame(
    key_candidates,
    ["table_name", "column_name"]
)

print("=== KEY CANDIDATE DISCOVERY ===")
print(f"Bronze tables scanned : {len(bronze_tables)}")
print(f"Identifier candidates : {len(key_candidates)}")

display(
    key_candidate_df
        .orderBy("table_name", "column_name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔑 Primary-Key Candidate Ranking
# 
# The previous discovery identified 208 identifier-like columns across
# 72 Bronze tables.
# 
# Most of these identifiers are not Primary Keys. Many represent foreign
# keys or repeating business references.
# 
# The next step therefore ranks likely Primary-Key candidates using table
# and column naming patterns before performing expensive data-level
# uniqueness validation.
# 
# This separates:
# 
# **Metadata Discovery → Candidate Selection → Data Validation**
# 
# and avoids unnecessarily profiling every identifier as though it were
# a Primary Key.

# CELL ********************

# ============================================================
# CELL 33 — PRIMARY KEY CANDIDATE RANKING
# Corrected: fully qualified Bronze table names
# ============================================================

import re


def singularize_table_name(table_name):
    """
    Produce a lightweight singular entity hint for PK ranking.
    This is metadata assistance only, not a business rule.
    """

    name = table_name.lower()

    # Remove known source/version suffixes.
    name = re.sub(
        r"_(final|fixed|v\d+)$",
        "",
        name,
        flags=re.IGNORECASE
    )

    # Remove yearly partition suffixes.
    name = re.sub(
        r"_20\d{2}(_final)?$",
        "",
        name,
        flags=re.IGNORECASE
    )

    entity = name.split("_")[-1]

    # Lightweight singularization.
    if entity.endswith("ies"):
        entity = entity[:-3] + "y"
    elif entity.endswith("s") and not entity.endswith("ss"):
        entity = entity[:-1]

    return entity


candidate_rows = []


for table_name in bronze_tables:

    # bronze_tables contains unqualified table names.
    full_table_name = f"bronze.{table_name}"

    columns = spark.table(full_table_name).columns

    entity_hint = singularize_table_name(table_name)

    identifier_columns = [
        c for c in columns
        if (
            c.lower() == "id"
            or c.lower().endswith("_id")
            or c.lower().endswith("_key")
        )
    ]

    for column_name in identifier_columns:

        col_lower = column_name.lower()

        score = 0
        reasons = []

        # Generic ID is usually a strong PK candidate.
        if col_lower == "id":
            score += 100
            reasons.append("generic_id")

        # Identifier matches the inferred table entity.
        if col_lower == f"{entity_hint}_id":
            score += 80
            reasons.append("entity_name_match")

        # Common grain-level identifiers.
        if any(
            token in col_lower
            for token in [
                "transaction_id",
                "event_id",
                "line_id",
                "account_id",
                "assignment_id",
                "delivery_id",
                "prescription_id",
                "claim_id",
                "payment_id",
                "return_id",
                "order_id",
                "transfer_id",
                "movement_id",
                "promotion_id"
            ]
        ):
            score += 30
            reasons.append("grain_identifier_pattern")

        # Common parent/reference identifiers are less likely
        # to define the grain of another entity.
        if (
            col_lower in {
                "customer_id",
                "pharmacy_id",
                "employee_id",
                "product_id",
                "supplier_id",
                "region_id",
                "status_id",
                "channel_id",
                "job_role_id"
            }
            and col_lower != f"{entity_hint}_id"
        ):
            score -= 20
            reasons.append("common_parent_reference")

        candidate_rows.append(
            (
                full_table_name,
                column_name,
                entity_hint,
                score,
                ", ".join(reasons)
            )
        )


pk_ranking_df = spark.createDataFrame(
    candidate_rows,
    [
        "table_name",
        "column_name",
        "entity_hint",
        "candidate_score",
        "ranking_reason"
    ]
)


ranking_window = (
    Window
        .partitionBy("table_name")
        .orderBy(
            F.desc("candidate_score"),
            F.asc("column_name")
        )
)


pk_ranked_df = (
    pk_ranking_df
        .withColumn(
            "candidate_rank",
            F.row_number().over(ranking_window)
        )
)


top_pk_candidates_df = (
    pk_ranked_df
        .filter(F.col("candidate_rank") == 1)
        .orderBy("table_name")
)


top_candidate_count = top_pk_candidates_df.count()


print("=== PRIMARY KEY CANDIDATE RANKING ===")
print(f"Bronze tables              : {len(bronze_tables)}")
print(f"Identifier candidates      : {len(candidate_rows)}")
print(f"Top PK candidates selected : {top_candidate_count}")


display(top_pk_candidates_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔍 Key Discovery Exceptions & Grain Review
# 
# Automated metadata ranking produced a top identifier candidate for
# 71 of 72 Bronze tables.
# 
# However, identifier ranking does not prove Primary-Key semantics.
# 
# Some source tables represent line, snapshot, schedule, or bridge grains
# where the correct key may be:
# 
# - a different identifier,
# - a composite key,
# - or a business-grain combination.
# 
# Before running data-level uniqueness tests, we identify tables where
# automatic single-column PK inference is weak or unavailable.
# 
# This prevents foreign keys and parent identifiers from being incorrectly
# validated as Primary Keys.

# CELL ********************

# ============================================================
# CELL 34 — KEY DISCOVERY EXCEPTIONS & GRAIN REVIEW
# ============================================================

# Rebuild the table set directly from the Bronze catalog.
all_bronze_tables = {
    f"bronze.{row.tableName}"
    for row in spark.sql("SHOW TABLES IN bronze").collect()
}

# Tables that received a top PK candidate.
ranked_tables = {
    row["table_name"]
    for row in top_pk_candidates_df
        .select("table_name")
        .distinct()
        .collect()
}

# 1. Tables with no identifier candidate at all.
tables_without_candidate = sorted(
    all_bronze_tables - ranked_tables
)

print("=== TABLES WITHOUT PK CANDIDATE ===")
print(f"Count: {len(tables_without_candidate)}")

for table_name in tables_without_candidate:
    print(f"  {table_name}")


# 2. Weak automatic candidates.
# Scores <= 30 usually mean the candidate was selected mainly
# from a generic identifier pattern rather than a strong table match.
weak_candidates_df = (
    top_pk_candidates_df
        .filter(F.col("candidate_score") <= 30)
        .select(
            "table_name",
            "column_name",
            "entity_hint",
            "candidate_score",
            "ranking_reason"
        )
        .orderBy(
            "candidate_score",
            "table_name"
        )
)

print("\n=== WEAK / REVIEW-REQUIRED PK CANDIDATES ===")

display(weak_candidates_df)


# 3. Show complete schemas only for review-required tables.
review_tables = sorted(
    set(tables_without_candidate)
    | {
        row["table_name"]
        for row in weak_candidates_df
            .select("table_name")
            .collect()
    }
)

print("\n=== REVIEW-REQUIRED TABLE SCHEMAS ===")
print(f"Tables requiring grain review: {len(review_tables)}")

for table_name in review_tables:

    print("\n" + "=" * 70)
    print(table_name)
    print("=" * 70)

    print(
        ", ".join(
            spark.table(table_name).columns
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🧪 Primary-Key Integrity Validation
# 
# Metadata discovery identified candidate identifiers, but naming conventions
# alone cannot establish Primary-Key integrity.
# 
# This step validates the strongest single-column candidate for each table
# against the actual data.
# 
# For every candidate we measure:
# 
# - Row count
# - NULL key count
# - Distinct key count
# - Duplicate rows
# - Uniqueness ratio
# 
# A candidate passes single-column PK integrity only when:
# 
# `NULL count = 0`
# 
# and
# 
# `Distinct key count = Row count`
# 
# Candidates that fail are not automatically data-quality defects.
# 
# Failure may indicate:
# 
# - an incorrect automatically selected identifier,
# - a repeating parent/foreign key,
# - or a legitimate composite table grain.
# 
# Those cases will be isolated for targeted grain analysis.

# CELL ********************

# ============================================================
# CELL 35 — ACTUAL SINGLE-COLUMN KEY INTEGRITY VALIDATION
# ============================================================

pk_test_results = []

candidate_rows = (
    top_pk_candidates_df
        .select("table_name", "column_name")
        .collect()
)

print(f"PK candidates to validate: {len(candidate_rows)}")


for i, row in enumerate(candidate_rows, start=1):

    table_name = row["table_name"]
    key_column = row["column_name"]

    df = spark.table(table_name)

    # One aggregation per table.
    result = (
        df.agg(
            F.count("*").alias("row_count"),

            F.sum(
                F.when(
                    F.col(key_column).isNull(),
                    1
                ).otherwise(0)
            ).alias("null_key_count"),

            F.countDistinct(
                F.col(key_column)
            ).alias("distinct_key_count")
        )
        .collect()[0]
    )

    row_count = result["row_count"]
    null_count = result["null_key_count"] or 0
    distinct_count = result["distinct_key_count"]

    # Number of rows exceeding single-column uniqueness.
    duplicate_rows = (
        row_count
        - null_count
        - distinct_count
    )

    uniqueness_pct = (
        round(
            100.0 * distinct_count / (row_count - null_count),
            6
        )
        if (row_count - null_count) > 0
        else None
    )

    pk_status = (
        "PASS"
        if (
            null_count == 0
            and distinct_count == row_count
        )
        else "REVIEW"
    )

    pk_test_results.append(
        (
            table_name,
            key_column,
            row_count,
            null_count,
            distinct_count,
            duplicate_rows,
            uniqueness_pct,
            pk_status
        )
    )

    print(
        f"[{i:02d}/{len(candidate_rows)}] "
        f"{pk_status:<6} "
        f"{table_name}.{key_column}"
    )


pk_test_schema = T.StructType([
    T.StructField("table_name", T.StringType(), False),
    T.StructField("candidate_key", T.StringType(), False),
    T.StructField("row_count", T.LongType(), False),
    T.StructField("null_key_count", T.LongType(), False),
    T.StructField("distinct_key_count", T.LongType(), False),
    T.StructField("duplicate_rows", T.LongType(), False),
    T.StructField("uniqueness_pct", T.DoubleType(), True),
    T.StructField("pk_status", T.StringType(), False)
])


pk_integrity_df = spark.createDataFrame(
    pk_test_results,
    schema=pk_test_schema
)


print("\n=== PRIMARY KEY INTEGRITY SUMMARY ===")

pk_integrity_df.groupBy("pk_status").count().show()


print("=== REVIEW-REQUIRED CANDIDATES ===")

display(
    pk_integrity_df
        .filter(F.col("pk_status") == "REVIEW")
        .orderBy(
            F.desc("null_key_count"),
            F.desc("duplicate_rows"),
            "table_name"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 36 — RESOLVE KEY REVIEW CASES
# ============================================================

# Explicit grain/key contracts derived from schema semantics.
key_contracts = [
    (
        "bronze.insurance_customer_membership",
        ["insurance_member_id"],
        "one row per insurance membership"
    ),
    (
        "bronze.insurance_insurance_claim_lines_final",
        ["claim_line_id"],
        "one row per insurance claim line"
    ),
    (
        "bronze.rx_prescription_items_final",
        ["prescription_item_id"],
        "one row per prescription item"
    ),
    (
        "bronze.inventory_inventory_monthly_snapshot_2025_final",
        ["month", "pharmacy_id", "product_id"],
        "one row per month × pharmacy × product"
    ),
    (
        "bronze.erp_pharmacy_operating_hours",
        ["pharmacy_id", "day_of_week", "effective_start_date"],
        "candidate operating-hours grain"
    ),
    (
        "bronze.crm_customer_service_reference",
        ["reference_type", "code"],
        "one row per reference type × code"
    )
]

results = []

for table_name, key_columns, expected_grain in key_contracts:

    df = spark.table(table_name)

    # Count rows with at least one NULL key component.
    null_condition = None

    for col_name in key_columns:
        condition = F.col(col_name).isNull()
        null_condition = (
            condition
            if null_condition is None
            else null_condition | condition
        )

    row_count = df.count()

    null_key_rows = (
        df.filter(null_condition).count()
        if null_condition is not None
        else 0
    )

    distinct_key_count = (
        df.select(*key_columns)
          .distinct()
          .count()
    )

    duplicate_rows = row_count - distinct_key_count

    status = (
        "PASS"
        if null_key_rows == 0 and duplicate_rows == 0
        else "REVIEW"
    )

    results.append(
        (
            table_name,
            " + ".join(key_columns),
            expected_grain,
            row_count,
            null_key_rows,
            distinct_key_count,
            duplicate_rows,
            status
        )
    )


resolved_keys_df = spark.createDataFrame(
    results,
    [
        "table_name",
        "validated_key",
        "expected_grain",
        "row_count",
        "null_key_rows",
        "distinct_key_count",
        "duplicate_rows",
        "status"
    ]
)

print("=== RESOLVED KEY CONTRACTS ===")
display(resolved_keys_df)


# ============================================================
# CRM INTERACTION ID — TRUE DUPLICATE INVESTIGATION
# ============================================================

crm_df = spark.table(
    "bronze.crm_customer_service_interactions"
)

duplicate_interactions_df = (
    crm_df
        .groupBy("interaction_id")
        .count()
        .filter(F.col("count") > 1)
)

crm_duplicate_profile = (
    duplicate_interactions_df
        .agg(
            F.count("*").alias("duplicated_interaction_ids"),
            F.sum("count").alias("rows_using_duplicated_ids"),
            F.max("count").alias("max_rows_per_interaction_id")
        )
)

print("=== CRM INTERACTION ID DUPLICATE PROFILE ===")
display(crm_duplicate_profile)

print("=== DUPLICATE MULTIPLICITY DISTRIBUTION ===")

display(
    duplicate_interactions_df
        .groupBy("count")
        .agg(
            F.count("*").alias("interaction_id_count")
        )
        .orderBy("count")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 37 — CRM INTERACTION DUPLICATE IDENTITY TEST
# ============================================================

crm_df = spark.table(
    "bronze.crm_customer_service_interactions"
)

non_key_columns = [
    c for c in crm_df.columns
    if c != "interaction_id"
]

# Create a deterministic fingerprint of the non-key attributes.
crm_fingerprinted_df = (
    crm_df
        .withColumn(
            "_row_fingerprint",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(
                            F.col(c).cast("string"),
                            F.lit("<NULL>")
                        )
                        for c in non_key_columns
                    ]
                ),
                256
            )
        )
)

duplicate_identity_df = (
    crm_fingerprinted_df
        .groupBy("interaction_id")
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct("_row_fingerprint").alias(
                "distinct_record_versions"
            )
        )
        .filter(F.col("row_count") > 1)
        .withColumn(
            "duplicate_type",
            F.when(
                F.col("distinct_record_versions") == 1,
                F.lit("EXACT_DUPLICATE")
            ).otherwise(
                F.lit("ID_COLLISION")
            )
        )
)

print("=== CRM DUPLICATE CLASSIFICATION ===")

display(
    duplicate_identity_df
        .groupBy("duplicate_type")
        .agg(
            F.count("*").alias("interaction_id_count"),
            F.sum("row_count").alias("affected_rows")
        )
        .orderBy("duplicate_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 38 — CRM ID COLLISION DIFFERENCE PATTERN
# ============================================================

crm_df = spark.table(
    "bronze.crm_customer_service_interactions"
)

duplicate_ids_df = (
    crm_df
        .groupBy("interaction_id")
        .count()
        .filter(F.col("count") > 1)
        .select("interaction_id")
)

duplicate_rows_df = (
    crm_df
        .join(
            duplicate_ids_df,
            on="interaction_id",
            how="inner"
        )
)

comparison_columns = [
    c for c in crm_df.columns
    if c != "interaction_id"
]

# For each duplicated interaction_id, count distinct values
# observed in every non-key attribute.
difference_expressions = [
    F.countDistinct(
        F.coalesce(
            F.col(c).cast("string"),
            F.lit("<NULL>")
        )
    ).alias(c)
    for c in comparison_columns
]

pair_comparison_df = (
    duplicate_rows_df
        .groupBy("interaction_id")
        .agg(*difference_expressions)
)

# Count how many duplicated IDs differ in each attribute.
difference_summary = []

for column_name in comparison_columns:

    different_count = (
        pair_comparison_df
            .filter(F.col(column_name) > 1)
            .count()
    )

    difference_summary.append(
        (
            column_name,
            different_count,
            round(
                100.0 * different_count / 530000,
                4
            )
        )
    )

difference_summary_df = spark.createDataFrame(
    difference_summary,
    [
        "column_name",
        "collision_ids_with_difference",
        "difference_pct"
    ]
)

print("=== CRM ID COLLISION — ATTRIBUTE DIFFERENCES ===")

display(
    difference_summary_df
        .orderBy(
            F.desc("collision_ids_with_difference"),
            "column_name"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🛡️ Silver Decision — SIL-DQ-CRM-KEY-001
# 
# ### Finding
# 
# `crm_customer_service_interactions.interaction_id` does not uniquely
# identify interaction records.
# 
# ### Evidence
# 
# The Bronze table contains:
# 
# - 2,000,000 rows
# - 1,470,000 distinct `interaction_id` values
# - 530,000 duplicated identifiers
# - 1,060,000 rows affected by duplicated identifiers
# - Maximum occurrence per duplicated identifier: 2
# 
# The duplicated records are not exact duplicates.
# 
# All 530,000 duplicated identifiers contain two different record
# fingerprints.
# 
# Attribute-level comparison showed that duplicated identifiers differ in:
# 
# - `opened_timestamp`: 100.00%
# - `customer_id`: 99.9996%
# - `closed_timestamp`: 99.9617%
# - `pharmacy_id`: 93.3062%
# - `resolution_code`: 86.2421%
# - `interaction_type_id`: 86.0119%
# - `service_channel_id`: 79.8236%
# - `satisfaction_score`: 74.9315%
# - `priority`: 50.3479%
# - `status_id`: 31.4642%
# 
# `source_system` does not differ.
# 
# ### Interpretation
# 
# The evidence does not support interpreting the duplicate rows as repeated
# copies or multiple versions of the same interaction.
# 
# The duplicated identifiers represent materially different interaction
# records.
# 
# Therefore, `interaction_id` is not a reliable unique row identifier in
# the available source data.
# 
# This is classified as a genuine source identifier collision.
# 
# ### Silver v1 Decision
# 
# Do NOT remove records using:
# 
# `dropDuplicates(["interaction_id"])`
# 
# because this would incorrectly discard distinct business events.
# 
# Preserve all valid interaction records.
# 
# The source `interaction_id` must remain available for lineage but must
# not be used alone as the Silver Primary Key.
# 
# Silver will create a deterministic unique interaction key during
# implementation while retaining the original source identifier.
# 
# The generated key must be:
# 
# - deterministic,
# - reproducible,
# - unique at the Silver interaction grain,
# - independent of Spark partition/order,
# - and traceable back to the Bronze record.
# 
# The exact key-generation implementation will be defined during the
# Silver transformation stage.
# 
# ### Data Quality Classification
# 
# **Type:** Identifier collision  
# **Severity:** ERROR  
# **Affected source:** CRM  
# **Affected attribute:** `interaction_id`
# 
# This is a source-key integrity defect, not an exact-row duplicate defect.
# 
# ### Bronze Policy
# 
# Bronze remains unchanged.
# 
# All 2,000,000 source records are preserved.
# 
# ### Decision Status
# 
# **Decision ID:** SIL-DQ-CRM-KEY-001  
# **Version:** 1.0  
# **Status:** APPROVED FOR SILVER  
# **Implementation:** PENDING  
# **Validation:** Discovery COMPLETE  
# **Review Trigger:** Authoritative source-system key semantics or corrected
# CRM interaction identifiers become available.


# MARKDOWN ********************

# # 🔗 Phase 4 — Referential Integrity Discovery
# 
# ## Objective
# 
# Validate whether relationships between Bronze entities are structurally
# consistent before the data is promoted into Silver.
# 
# Primary-Key integrity answers:
# 
# > Can a record be uniquely identified?
# 
# Referential integrity answers:
# 
# > Does a referenced business entity actually exist?
# 
# Examples include:
# 
# - Transaction → Pharmacy
# - Transaction → Customer
# - Sales Line → Transaction
# - Sales Line → Product
# - Prescription → Doctor
# - Prescription Item → Prescription
# - Claim Line → Claim
# - Purchase Order → Supplier
# - Inventory Movement → Product
# 
# ## Important Principle
# 
# Matching column names do not automatically prove a Foreign-Key
# relationship.
# 
# An identifier may represent:
# 
# - a true Foreign Key,
# - an optional relationship,
# - a polymorphic reference,
# - a source-local identifier,
# - or an identifier belonging to another business context.
# 
# Therefore:
# 
# **Discover → Classify → Validate → Investigate Exceptions**
# 
# rather than blindly joining every `_id` column.
# 
# ## Core Referential-Integrity Tests
# 
# For each validated relationship:
# 
# 1. Child FK row count
# 2. NULL FK count
# 3. Non-NULL FK count
# 4. Distinct FK count
# 5. Orphan FK values
# 6. Rows affected by orphan references
# 7. Orphan percentage
# 
# A non-NULL child identifier with no matching parent is a referential
# integrity anomaly only when the relationship is confirmed to require
# that parent.
# 
# NULL itself is evaluated separately according to the business semantics
# of the relationship.


# CELL ********************

# ============================================================
# CELL 39 — AUTOMATIC FK RELATIONSHIP DISCOVERY
# ============================================================

# ------------------------------------------------------------
# 1. Build the validated single-column parent-key registry.
# ------------------------------------------------------------

validated_single_keys_df = (
    pk_integrity_df
        .filter(F.col("pk_status") == "PASS")
        .select(
            "table_name",
            F.col("candidate_key").alias("parent_key")
        )
)

# Add single-column keys that were corrected during grain review.
corrected_single_keys = [
    (
        "bronze.insurance_customer_membership",
        "insurance_member_id"
    ),
    (
        "bronze.insurance_insurance_claim_lines_final",
        "claim_line_id"
    ),
    (
        "bronze.rx_prescription_items_final",
        "prescription_item_id"
    )
]

corrected_single_keys_df = spark.createDataFrame(
    corrected_single_keys,
    ["table_name", "parent_key"]
)

validated_parent_keys_df = (
    validated_single_keys_df
        .unionByName(corrected_single_keys_df)
        .dropDuplicates(
            ["table_name", "parent_key"]
        )
)

print("=== VALIDATED SINGLE-COLUMN PARENT KEYS ===")
print(
    "Validated parent keys:",
    validated_parent_keys_df.count()
)


# ------------------------------------------------------------
# 2. Build lookup:
#    identifier name -> possible parent tables.
# ------------------------------------------------------------

parent_key_rows = validated_parent_keys_df.collect()

parent_lookup = {}

for row in parent_key_rows:

    parent_lookup.setdefault(
        row["parent_key"],
        []
    ).append(
        row["table_name"]
    )


# ------------------------------------------------------------
# 3. Discover same-name identifiers in other Bronze tables.
# ------------------------------------------------------------

relationship_candidates = []

for child_table in bronze_tables:

    full_child_table = f"bronze.{child_table}"

    child_columns = spark.table(
        full_child_table
    ).columns

    for child_column in child_columns:

        if child_column not in parent_lookup:
            continue

        for parent_table in parent_lookup[child_column]:

            # A table's own validated key is not an FK to itself.
            if full_child_table == parent_table:
                continue

            relationship_candidates.append(
                (
                    full_child_table,
                    child_column,
                    parent_table,
                    child_column,
                    "SAME_NAME_VALIDATED_PARENT_KEY"
                )
            )


fk_candidate_df = spark.createDataFrame(
    relationship_candidates,
    [
        "child_table",
        "child_column",
        "parent_table",
        "parent_column",
        "discovery_rule"
    ]
).dropDuplicates()


print("\n=== FK RELATIONSHIP DISCOVERY ===")
print(
    "Candidate relationships:",
    fk_candidate_df.count()
)

display(
    fk_candidate_df
        .orderBy(
            "child_column",
            "child_table",
            "parent_table"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 40 — FK CANDIDATE AMBIGUITY CLASSIFICATION
# ============================================================

# Count how many possible parent tables exist for each FK column name.
parent_ambiguity_df = (
    fk_candidate_df
        .select(
            "child_column",
            "parent_table",
            "parent_column"
        )
        .dropDuplicates()
        .groupBy("child_column")
        .agg(
            F.countDistinct("parent_table").alias(
                "possible_parent_count"
            ),
            F.collect_set("parent_table").alias(
                "possible_parent_tables"
            )
        )
)

# Attach ambiguity information back to every candidate relationship.
fk_classified_df = (
    fk_candidate_df
        .join(
            parent_ambiguity_df,
            on="child_column",
            how="left"
        )
        .withColumn(
            "candidate_class",
            F.when(
                F.col("possible_parent_count") == 1,
                F.lit("UNAMBIGUOUS")
            ).otherwise(
                F.lit("AMBIGUOUS")
            )
        )
)

print("=== FK CANDIDATE CLASSIFICATION ===")

display(
    fk_classified_df
        .groupBy("candidate_class")
        .agg(
            F.count("*").alias("relationship_count")
        )
        .orderBy("candidate_class")
)

print("\n=== AMBIGUOUS IDENTIFIER NAMES ===")

display(
    parent_ambiguity_df
        .filter(F.col("possible_parent_count") > 1)
        .orderBy(
            F.desc("possible_parent_count"),
            "child_column"
        )
)

print("\n=== UNAMBIGUOUS RELATIONSHIP CANDIDATES ===")

display(
    fk_classified_df
        .filter(
            F.col("candidate_class") == "UNAMBIGUOUS"
        )
        .select(
            "child_table",
            "child_column",
            "parent_table",
            "parent_column"
        )
        .orderBy(
            "parent_table",
            "child_table",
            "child_column"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 41 — UNAMBIGUOUS REFERENTIAL INTEGRITY VALIDATION
# ============================================================

unambiguous_fk_df = (
    fk_classified_df
        .filter(F.col("candidate_class") == "UNAMBIGUOUS")
        .select(
            "child_table",
            "child_column",
            "parent_table",
            "parent_column"
        )
        .dropDuplicates()
)

fk_relationships = unambiguous_fk_df.collect()

results = []

print(
    f"Validating {len(fk_relationships)} "
    "unambiguous FK relationships..."
)

for index, relationship in enumerate(
    fk_relationships,
    start=1
):

    child_table = relationship["child_table"]
    child_column = relationship["child_column"]
    parent_table = relationship["parent_table"]
    parent_column = relationship["parent_column"]

    try:

        child_df = (
            spark.table(child_table)
                .select(
                    F.col(child_column).alias("fk_value")
                )
        )

        parent_df = (
            spark.table(parent_table)
                .select(
                    F.col(parent_column).alias("pk_value")
                )
                .dropDuplicates()
        )

        # ----------------------------------------------------
        # Basic child FK statistics
        # ----------------------------------------------------

        child_stats = (
            child_df
                .agg(
                    F.count("*").alias("child_rows"),

                    F.sum(
                        F.when(
                            F.col("fk_value").isNull(),
                            1
                        ).otherwise(0)
                    ).alias("null_fk_rows"),

                    F.countDistinct(
                        "fk_value"
                    ).alias("distinct_non_null_fk")
                )
                .first()
        )

        # ----------------------------------------------------
        # Find non-NULL FK values without a parent match.
        # ----------------------------------------------------

        orphan_values_df = (
            child_df
                .filter(F.col("fk_value").isNotNull())
                .select("fk_value")
                .dropDuplicates()
                .join(
                    parent_df,
                    F.col("fk_value") == F.col("pk_value"),
                    "left_anti"
                )
        )

        orphan_value_count = orphan_values_df.count()

        # ----------------------------------------------------
        # Count rows affected by those orphan values.
        # ----------------------------------------------------

        if orphan_value_count > 0:

            orphan_rows = (
                child_df
                    .filter(F.col("fk_value").isNotNull())
                    .join(
                        orphan_values_df,
                        on="fk_value",
                        how="inner"
                    )
                    .count()
            )

        else:
            orphan_rows = 0

        non_null_fk_rows = (
            child_stats["child_rows"]
            - child_stats["null_fk_rows"]
        )

        orphan_row_pct = (
            round(
                100.0
                * orphan_rows
                / non_null_fk_rows,
                6
            )
            if non_null_fk_rows > 0
            else 0.0
        )

        status = (
            "PASS"
            if orphan_value_count == 0
            else "REVIEW"
        )

        results.append(
            (
                child_table,
                child_column,
                parent_table,
                parent_column,
                child_stats["child_rows"],
                child_stats["null_fk_rows"],
                child_stats["distinct_non_null_fk"],
                orphan_value_count,
                orphan_rows,
                orphan_row_pct,
                status
            )
        )

        print(
            f"[{index:03}/{len(fk_relationships)}] "
            f"{status:<6} "
            f"{child_table}.{child_column}"
        )

    except Exception as error:

        results.append(
            (
                child_table,
                child_column,
                parent_table,
                parent_column,
                None,
                None,
                None,
                None,
                None,
                None,
                "ERROR"
            )
        )

        print(
            f"[{index:03}/{len(fk_relationships)}] "
            f"ERROR  "
            f"{child_table}.{child_column}"
        )

        print(str(error)[:300])


# ============================================================
# CREATE RESULT DATAFRAME
# ============================================================

fk_validation_schema = T.StructType([
    T.StructField("child_table", T.StringType(), False),
    T.StructField("child_column", T.StringType(), False),
    T.StructField("parent_table", T.StringType(), False),
    T.StructField("parent_column", T.StringType(), False),
    T.StructField("child_rows", T.LongType(), True),
    T.StructField("null_fk_rows", T.LongType(), True),
    T.StructField("distinct_non_null_fk", T.LongType(), True),
    T.StructField("orphan_value_count", T.LongType(), True),
    T.StructField("orphan_rows", T.LongType(), True),
    T.StructField("orphan_row_pct", T.DoubleType(), True),
    T.StructField("status", T.StringType(), False)
])

fk_validation_df = spark.createDataFrame(
    results,
    schema=fk_validation_schema
)


# ============================================================
# SUMMARY
# ============================================================

print("\n=== REFERENTIAL INTEGRITY SUMMARY ===")

display(
    fk_validation_df
        .groupBy("status")
        .agg(
            F.count("*").alias("relationship_count")
        )
        .orderBy("status")
)


print("\n=== RELATIONSHIPS REQUIRING REVIEW ===")

display(
    fk_validation_df
        .filter(F.col("status") != "PASS")
        .orderBy(
            F.desc("orphan_rows"),
            "child_table",
            "child_column"
        )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 42 — POS VS MARKETING CHANNEL REFERENCE CHECK
# ============================================================

pos_channels = (
    spark.table("bronze.pos_sales_transactions_2020")
    .select("channel_id")
    .distinct()
    .orderBy("channel_id")
)

marketing_channels = spark.table(
    "bronze.marketing_channels"
)

print("=== POS CHANNEL VALUES ===")
display(pos_channels)

print("=== MARKETING CHANNEL REFERENCE ===")
display(marketing_channels)

print("=== CHANNEL VALUE OVERLAP ===")
display(
    pos_channels.join(
        marketing_channels.select("channel_id").distinct(),
        on="channel_id",
        how="left_anti"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔗 Cell 44 — POS Cross-Year Referential Integrity
# 
# ### Objective
# 
# Validate the relationships between POS sales transactions,
# canonical sales lines, and payments across 2020–2025.
# 
# ### Rules
# 
# - A sales line must reference an existing transaction.
# - A payment must reference an existing transaction.
# - Transaction identifiers must be unique across the combined years.
# - Do not assume that matching transaction IDs belong to the same year.
# - Preserve all Bronze records.
# 
# ### Expected Outcome
# 
# A validated POS relationship contract for Silver consolidation.

# CELL ********************

# ============================================================
# CELL 44 — POS CROSS-YEAR REFERENTIAL INTEGRITY
# ============================================================

from functools import reduce

years = range(2020, 2026)

# Read only the required identifier columns.
transaction_parts = [
    spark.table(f"bronze.pos_sales_transactions_{year}")
    .select("transaction_id")
    for year in years
]

sales_line_parts = [
    spark.table(
        "bronze.pos_canonical_sales_lines_2025_final"
        if year == 2025
        else f"bronze.pos_canonical_sales_lines_{year}"
    ).select("transaction_id")
    for year in years
]

payment_parts = [
    spark.table(f"bronze.pos_payments_{year}")
    .select("transaction_id")
    for year in years
]

# Consolidate yearly identifier sets.
transactions = reduce(
    lambda a, b: a.unionByName(b),
    transaction_parts
)

sales_lines = reduce(
    lambda a, b: a.unionByName(b),
    sales_line_parts
)

payments = reduce(
    lambda a, b: a.unionByName(b),
    payment_parts
)

# Validate global transaction-key uniqueness.
transaction_stats = transactions.agg(
    F.count("*").alias("transaction_rows"),
    F.countDistinct("transaction_id").alias(
        "distinct_transaction_ids"
    ),
    F.sum(
        F.when(F.col("transaction_id").isNull(), 1).otherwise(0)
    ).alias("null_transaction_ids")
).first()

# Parent lookup for referential checks.
parent_keys = (
    transactions
    .filter(F.col("transaction_id").isNotNull())
    .distinct()
)

results = []

for name, child_df in [
    ("SALES_LINES", sales_lines),
    ("PAYMENTS", payments)
]:

    child_stats = child_df.agg(
        F.count("*").alias("child_rows"),
        F.sum(
            F.when(
                F.col("transaction_id").isNull(), 1
            ).otherwise(0)
        ).alias("null_fk_rows")
    ).first()

    orphan_rows = (
        child_df
        .filter(F.col("transaction_id").isNotNull())
        .join(
            parent_keys,
            on="transaction_id",
            how="left_anti"
        )
        .count()
    )

    results.append((
        name,
        child_stats["child_rows"],
        child_stats["null_fk_rows"],
        orphan_rows
    ))

print("=== GLOBAL TRANSACTION KEY VALIDATION ===")

print(
    "Transaction rows:",
    transaction_stats["transaction_rows"]
)
print(
    "Distinct transaction IDs:",
    transaction_stats["distinct_transaction_ids"]
)
print(
    "NULL transaction IDs:",
    transaction_stats["null_transaction_ids"]
)

print("\n=== POS REFERENTIAL INTEGRITY ===")

result_df = spark.createDataFrame(
    results,
    [
        "relationship",
        "child_rows",
        "null_fk_rows",
        "orphan_rows"
    ]
)

display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔍 Cell 45 — Referential Coverage Gap Analysis
# 
# ### Objective
# 
# Identify identifier columns not covered by the existing
# foreign-key candidate inventory.
# 
# ### Approach
# 
# - Reuse the validated relationship candidates.
# - Inspect only Bronze table schemas.
# - Exclude validated primary-key columns.
# - Identify uncovered identifier columns.
# - Prioritize transaction and cross-domain relationships.
# 
# ### Engineering Principle
# 
# An uncovered identifier is not automatically a data-quality defect.
# 
# The objective is to find missing relationship contracts,
# not to manufacture relationships from column names.

# CELL ********************

# ============================================================
# CELL 45 — REFERENTIAL COVERAGE GAP ANALYSIS
# ============================================================

# Rebuild metadata from the persisted Bronze catalog.
bronze_table_names = [
    t.name
    for t in spark.catalog.listTables("bronze")
    if not t.isTemporary
]

# Existing FK candidates.
covered_fk_columns = {
    (r["child_table"], r["child_column"])
    for r in fk_candidate_df
        .select("child_table", "child_column")
        .distinct()
        .collect()
}

# Validated parent keys, including reviewed corrections.
validated_pk_columns = {
    (r["table_name"], r["parent_key"])
    for r in validated_parent_keys_df.collect()
}

# Composite keys validated during Phase 3.
composite_keys = {
    "bronze.inventory_inventory_monthly_snapshot_2025_final":
        ["month", "pharmacy_id", "product_id"],

    "bronze.erp_pharmacy_operating_hours":
        ["pharmacy_id", "day_of_week", "effective_start_date"],

    "bronze.crm_customer_service_reference":
        ["reference_type", "code"]
}

# Do not exclude a composite-key component merely because it
# participates in a composite key: it may also be a valid FK.

uncovered = []

for table_name in bronze_table_names:

    full_table = f"bronze.{table_name}"
    columns = spark.table(full_table).columns

    for column in columns:

        if not (
            column == "id"
            or column.endswith("_id")
            or column.endswith("_key")
        ):
            continue

        if (full_table, column) in covered_fk_columns:
            continue

        if (full_table, column) in validated_pk_columns:
            continue

        # Avoid treating an obvious table-own identifier as a
        # missing FK when it is a known composite-key component.
        if column in composite_keys.get(full_table, []):
            continue

        uncovered.append(
            (full_table, column)
        )

uncovered_schema = T.StructType([
    T.StructField("table_name", T.StringType(), False),
    T.StructField("identifier_column", T.StringType(), False)
])

uncovered_fk_df = spark.createDataFrame(
    uncovered,
    schema=uncovered_schema
)

print("=== REFERENTIAL COVERAGE GAP ===")
print("Uncovered identifier columns:", uncovered_fk_df.count())

display(
    uncovered_fk_df.orderBy(
        "table_name",
        "identifier_column"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 46 — EXPLICIT CROSS-DOMAIN FK VALIDATION
# ============================================================

from functools import reduce

contracts = [
    ("bronze.erp_employees", "primary_job_role_id",
     "bronze.erp_job_roles", "job_role_id"),

    ("bronze.insurance_customer_membership", "initial_plan_id",
     "bronze.insurance_plans", "insurance_plan_id"),

    ("bronze.inventory_stock_transfers_final", "source_pharmacy_id",
     "bronze.erp_pharmacies", "pharmacy_id"),

    ("bronze.inventory_stock_transfers_final", "destination_pharmacy_id",
     "bronze.erp_pharmacies", "pharmacy_id"),

    ("bronze.rx_prescription_dispenses_fixed", "pharmacist_employee_id",
     "bronze.erp_employees", "employee_id"),

    ("bronze.rx_prescriptions_fixed", "pharmacist_employee_id",
     "bronze.erp_employees", "employee_id"),

    ("bronze.supply_chain_purchase_orders_fixed", "buyer_employee_id",
     "bronze.erp_employees", "employee_id"),

    ("bronze.insurance_insurance_claims_fixed", "linked_transaction_id",
     "POS_TRANSACTIONS", "transaction_id"),

    ("bronze.rx_prescription_dispenses_fixed", "linked_transaction_id",
     "POS_TRANSACTIONS", "transaction_id"),

    ("bronze.rx_prescriptions_fixed", "linked_transaction_id",
     "POS_TRANSACTIONS", "transaction_id"),

    ("bronze.pos_returns_final", "original_transaction_id",
     "POS_TRANSACTIONS", "transaction_id"),

    ("bronze.pos_returns_final", "original_sales_line_id",
     "POS_SALES_LINES", "sales_line_id")
]

# Build logical parent sets from yearly Bronze partitions.
parent_cache = {}

for family, prefix, key in [
    ("POS_TRANSACTIONS", "pos_sales_transactions", "transaction_id"),
    ("POS_SALES_LINES", "pos_canonical_sales_lines", "sales_line_id")
]:
    parts = []

    for year in range(2020, 2026):
        suffix = "_final" if family == "POS_SALES_LINES" and year == 2025 else ""
        table = f"bronze.{prefix}_{year}{suffix}"
        parts.append(spark.table(table).select(key))

    parent_cache[family] = (
        reduce(lambda a, b: a.unionByName(b), parts)
        .filter(F.col(key).isNotNull())
        .distinct()
    )

results = []

for child_table, child_key, parent_table, parent_key in contracts:

    child = spark.table(child_table).select(
        F.col(child_key).alias("fk")
    )

    parent_source = (
        parent_cache[parent_table]
        if parent_table in parent_cache
        else spark.table(parent_table)
    )

    parent = (
        parent_source
        .select(F.col(parent_key).alias("fk"))
        .filter(F.col("fk").isNotNull())
        .distinct()
    )

    stats = child.agg(
        F.count("*").alias("total_rows"),
        F.count("fk").alias("non_null_rows")
    ).first()

    orphans = (
        child.filter(F.col("fk").isNotNull())
        .join(parent, "fk", "left_anti")
        .count()
    )

    results.append((
        child_table,
        child_key,
        parent_table,
        stats["total_rows"],
        stats["total_rows"] - stats["non_null_rows"],
        orphans,
        "PASS" if orphans == 0 else "REVIEW"
    ))

fk_explicit_df = spark.createDataFrame(
    results,
    [
        "child_table",
        "child_key",
        "parent_table",
        "total_rows",
        "null_fk_rows",
        "orphan_rows",
        "status"
    ]
)

print("=== EXPLICIT FK VALIDATION SUMMARY ===")

display(
    fk_explicit_df.groupBy("status").count()
)

print("=== RELATIONSHIP RESULTS ===")

display(
    fk_explicit_df.orderBy(
        F.desc("orphan_rows"),
        "child_table"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 47 — CROSS-SYSTEM TRANSACTION ID DIAGNOSIS
# ============================================================

sources = [
    ("RX_DISPENSES",
     "bronze.rx_prescription_dispenses_fixed",
     "linked_transaction_id"),

    ("RX_PRESCRIPTIONS",
     "bronze.rx_prescriptions_fixed",
     "linked_transaction_id"),

    ("INSURANCE_CLAIMS",
     "bronze.insurance_insurance_claims_fixed",
     "linked_transaction_id"),

    ("POS_2020",
     "bronze.pos_sales_transactions_2020",
     "transaction_id"),

    ("POS_2025",
     "bronze.pos_sales_transactions_2025",
     "transaction_id")
]

for label, table_name, key in sources:

    df = (
        spark.table(table_name)
        .select(F.col(key).cast("string").alias("id"))
        .filter(F.col("id").isNotNull())
    )

    print(f"\n=== {label} ===")

    display(
        df.agg(
            F.count("*").alias("rows"),
            F.min("id").alias("min_id"),
            F.max("id").alias("max_id"),
            F.min(F.length("id")).alias("min_length"),
            F.max(F.length("id")).alias("max_length")
        )
    )

    display(df.distinct().limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ⏱️ Phase 5 — Temporal Integrity
# 
# ## Objective
# 
# Validate chronological consistency within business entities
# and across related business processes.
# 
# ### Key Questions
# 
# - Can an event occur before its prerequisite?
# - Are effective dates logically ordered?
# - Do operational lifecycle timestamps agree?
# - Are cross-system timestamps comparable?
# 
# ### Validation Strategy
# 
# 1. Discover temporal columns.
# 2. Define meaningful temporal rules.
# 3. Validate rules automatically.
# 4. Investigate critical violations only.
# 5. Document Silver decisions.
# 
# **Principle:** A temporal anomaly must be interpreted using
# business context, timestamp precision, and source semantics.

# CELL ********************

# ============================================================
# CELL 48 — TEMPORAL METADATA DISCOVERY
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

temporal_columns = []

for table in spark.catalog.listTables("bronze"):

    if table.isTemporary:
        continue

    full_name = f"bronze.{table.name}"

    schema = spark.table(full_name).schema

    for field in schema.fields:

        column_name = field.name.lower()

        is_temporal_type = isinstance(
            field.dataType,
            (T.DateType, T.TimestampType, T.TimestampNTZType)
        )

        is_temporal_name = any(
            token in column_name
            for token in [
                "date", "timestamp", "_ts",
                "_at", "_from", "_to"
            ]
        )

        if is_temporal_type or is_temporal_name:

            temporal_columns.append((
                full_name,
                field.name,
                field.dataType.simpleString(),
                is_temporal_type
            ))

temporal_schema = T.StructType([
    T.StructField("table_name", T.StringType(), False),
    T.StructField("column_name", T.StringType(), False),
    T.StructField("data_type", T.StringType(), False),
    T.StructField("native_temporal_type", T.BooleanType(), False)
])

temporal_inventory_df = spark.createDataFrame(
    temporal_columns,
    temporal_schema
)

print("=== TEMPORAL DISCOVERY ===")

print(
    "Temporal columns:",
    temporal_inventory_df.count()
)

print(
    "Tables containing temporal columns:",
    temporal_inventory_df
        .select("table_name")
        .distinct()
        .count()
)

display(
    temporal_inventory_df.orderBy(
        "table_name",
        "column_name"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 49 — INTRA-TABLE TEMPORAL INTEGRITY VALIDATION
# Pharmacy Enterprise Data Warehouse | Microsoft Fabric
# Read-only validation: Bronze is never modified.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

# ------------------------------------------------------------
# 1. Define temporal business rules
# ------------------------------------------------------------

temporal_rules = [
    ("CRM-001", "bronze.crm_customers",
     "birth_date", "registration_date"),

    ("CRM-002", "bronze.crm_customer_service_interactions",
     "opened_timestamp", "closed_timestamp"),

    ("DEL-001", "bronze.delivery_deliveries",
     "order_timestamp", "dispatch_timestamp"),

    ("DEL-002", "bronze.delivery_deliveries",
     "dispatch_timestamp", "delivered_timestamp"),

    ("ERP-001", "bronze.erp_employees",
     "birth_date", "hire_date"),

    ("ERP-002", "bronze.erp_employees",
     "hire_date", "termination_date"),

    ("ERP-003", "bronze.erp_employee_assignments",
     "assignment_start_date", "assignment_end_date"),

    ("ERP-004", "bronze.erp_employee_shift_rosters",
     "effective_start_date", "effective_end_date"),

    ("ERP-005", "bronze.erp_pharmacy_operating_hours",
     "effective_start_date", "effective_end_date"),

    ("FIN-001", "bronze.finance_vat_rate_timeline",
     "effective_start_date", "effective_end_date"),

    ("INS-001", "bronze.insurance_eligibility_history",
     "coverage_start_date", "coverage_end_date"),

    ("INS-002", "bronze.insurance_insurance_claims_fixed",
     "submitted_ts", "adjudicated_ts"),

    ("MKT-001", "bronze.marketing_campaigns",
     "start_date", "end_date"),

    ("MKT-002", "bronze.marketing_promotions",
     "start_date", "end_date"),

    ("RX-001", "bronze.rx_doctors",
     "active_from", "active_to"),

    ("SC-001", "bronze.supply_chain_product_batches_final",
     "manufacture_date", "received_date"),

    ("SC-002", "bronze.supply_chain_product_batches_final",
     "received_date", "expiry_date"),

    ("SC-003", "bronze.supply_chain_purchase_orders_fixed",
     "order_date", "expected_delivery_date"),

    ("SC-004", "bronze.supply_chain_supplier_contracts",
     "contract_start_date", "contract_end_date")
]

# ------------------------------------------------------------
# 2. Validate rules and collect results
# ------------------------------------------------------------

results = []

for rule_id, table_name, start_col, end_col in temporal_rules:

    total_rows = None
    comparable_rows = None
    incomplete_pairs = None
    violations = None
    status = "ERROR"
    detail = None

    try:
        if not spark.catalog.tableExists(table_name):
            status = "SKIPPED"
            detail = "Table not found"

        else:
            df = spark.table(table_name)
            available_columns = set(df.columns)

            missing_columns = [
                col_name
                for col_name in [start_col, end_col]
                if col_name not in available_columns
            ]

            if missing_columns:
                status = "SKIPPED"
                detail = (
                    "Missing columns: "
                    + ", ".join(missing_columns)
                )

            else:
                start = F.col(start_col)
                end = F.col(end_col)

                incomplete_condition = (
                    start.isNull() | end.isNull()
                )

                violation_condition = (
                    start.isNotNull()
                    & end.isNotNull()
                    & (start > end)
                )

                stats = df.agg(
                    F.count("*").alias("total_rows"),

                    F.sum(
                        F.when(
                            incomplete_condition, 1
                        ).otherwise(0)
                    ).alias("incomplete_pairs"),

                    F.sum(
                        F.when(
                            violation_condition, 1
                        ).otherwise(0)
                    ).alias("violations")
                ).first()

                total_rows = int(stats["total_rows"])
                incomplete_pairs = int(
                    stats["incomplete_pairs"] or 0
                )
                violations = int(
                    stats["violations"] or 0
                )

                comparable_rows = (
                    total_rows - incomplete_pairs
                )

                status = (
                    "PASS" if violations == 0
                    else "REVIEW"
                )

                detail = (
                    "No reversed date pairs"
                    if violations == 0
                    else "Reversed date pairs detected"
                )

    except Exception as error:
        status = "ERROR"
        detail = str(error)[:300]

    results.append((
        rule_id,
        table_name,
        start_col,
        end_col,
        total_rows,
        comparable_rows,
        incomplete_pairs,
        violations,
        status,
        detail
    ))

# ------------------------------------------------------------
# 3. Create structured results DataFrame
# ------------------------------------------------------------

result_schema = T.StructType([
    T.StructField("rule_id", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("start_column", T.StringType(), False),
    T.StructField("end_column", T.StringType(), False),
    T.StructField("total_rows", T.LongType(), True),
    T.StructField("comparable_rows", T.LongType(), True),
    T.StructField("incomplete_pairs", T.LongType(), True),
    T.StructField("violations", T.LongType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("detail", T.StringType(), True)
])

temporal_results_df = spark.createDataFrame(
    results,
    schema=result_schema
)

# ------------------------------------------------------------
# 4. Show validation summary
# ------------------------------------------------------------

print("=== CELL 49 — TEMPORAL VALIDATION SUMMARY ===")

print(f"Total rules: {len(temporal_rules)}")

display(
    temporal_results_df
    .groupBy("status")
    .count()
    .orderBy("status")
)

print("=== DETAILED TEMPORAL RESULTS ===")

display(
    temporal_results_df.orderBy("rule_id")
)

# ------------------------------------------------------------
# 5. Show exceptions requiring attention
# ------------------------------------------------------------

print("=== RULES REQUIRING ATTENTION ===")

display(
    temporal_results_df
    .filter(F.col("status") != "PASS")
    .orderBy("rule_id")
)

# ------------------------------------------------------------
# 6. Export results to Lakehouse Files
# ------------------------------------------------------------

output_path = (
    "Files/silver_discovery/"
    "cell49_temporal_validation"
)

(
    temporal_results_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(output_path)
)

print(f"Results exported to: {output_path}")
print("CELL 49 FINISHED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 50 — DELIVERY TEMPORAL EXCEPTION DIAGNOSIS
# ============================================================

from pyspark.sql import functions as F

delivery = spark.table("bronze.delivery_deliveries")

violations = (
    delivery
    .filter(
        F.col("dispatch_timestamp").isNotNull()
        & F.col("delivered_timestamp").isNotNull()
        & (
            F.col("delivered_timestamp")
            < F.col("dispatch_timestamp")
        )
    )
    .withColumn(
        "reversal_minutes",
        (
            F.unix_timestamp("dispatch_timestamp")
            - F.unix_timestamp("delivered_timestamp")
        ) / 60.0
    )
    .withColumn(
        "reversal_band",
        F.when(
            F.col("reversal_minutes") <= 5,
            "01: <=5 minutes"
        )
        .when(
            F.col("reversal_minutes") <= 60,
            "02: 5-60 minutes"
        )
        .when(
            F.col("reversal_minutes") <= 1440,
            "03: 1-24 hours"
        )
        .otherwise("04: >24 hours")
    )
)

print("=== REVERSAL SEVERITY ===")

display(
    violations
    .groupBy("reversal_band")
    .agg(F.count("*").alias("rows"))
    .orderBy("reversal_band")
)

print("=== REVERSAL STATISTICS ===")

display(
    violations.agg(
        F.count("*").alias("violations"),
        F.round(
            F.min("reversal_minutes"), 2
        ).alias("min_minutes"),
        F.round(
            F.expr(
                "percentile_approx(reversal_minutes, 0.5)"
            ), 2
        ).alias("median_minutes"),
        F.round(
            F.max("reversal_minutes"), 2
        ).alias("max_minutes")
    )
)

print("=== DELIVERY STATUS DISTRIBUTION ===")

if "delivery_status" in delivery.columns:
    display(
        violations
        .groupBy("delivery_status")
        .count()
        .orderBy(F.desc("count"))
    )
else:
    print("Delivery status column requires schema mapping.")

print("=== SAMPLE VIOLATIONS ===")

display(
    violations.select(
        "dispatch_timestamp",
        "delivered_timestamp",
        "reversal_minutes",
        "reversal_band"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 51 — CROSS-TABLE TEMPORAL READINESS
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

tables_to_inspect = [
    "bronze.rx_prescriptions_fixed",
    "bronze.rx_prescription_dispenses_fixed",
    "bronze.pos_returns_final",
    "bronze.pos_sales_transactions_2025",
    "bronze.supply_chain_purchase_orders_fixed",
    "bronze.supply_chain_goods_receipts"
]

rows = []

for table_name in tables_to_inspect:
    if not spark.catalog.tableExists(table_name):
        rows.append((table_name, "TABLE_NOT_FOUND", ""))
        continue

    for field in spark.table(table_name).schema.fields:
        name = field.name.lower()

        if (
            isinstance(
                field.dataType,
                (T.DateType, T.TimestampType, T.TimestampNTZType)
            )
            or "date" in name
            or "timestamp" in name
            or name.endswith("_ts")
        ):
            rows.append((
                table_name,
                field.name,
                field.dataType.simpleString()
            ))

schema = T.StructType([
    T.StructField("table_name", T.StringType()),
    T.StructField("column_name", T.StringType()),
    T.StructField("data_type", T.StringType())
])

cross_temporal_columns_df = spark.createDataFrame(
    rows, schema
)

display(
    cross_temporal_columns_df.orderBy(
        "table_name", "column_name"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cell 52 — Final Temporal Validation Suite
# 
# ## Objectives
# - Validate important business dates against explicit rules.
# - Verify returns occur after their original sales.
# - Verify dispensing occurs after prescription creation.
# - Distinguish missing links from temporal violations.
# - Preserve all Bronze source records.
# 
# ## Interpretation
# PASS: Rule evaluated with no violations.
# REVIEW: Rule evaluated and violations detected.
# BLOCKED: Required relationship or source is unavailable.
# SKIPPED: Required columns or tables are missing.
# 
# This suite complements Cell 49. It does not certify every
# temporal column in the enterprise.

# CELL ********************

# ============================================================
# CELL 52 — FINAL TEMPORAL VALIDATION SUITE
# ============================================================

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import types as T

results = []

# ------------------------------------------------------------
# 1. Shared validation functions
# ------------------------------------------------------------

def record_result(rule_id, description, total, tested,
                  violations, status, detail=""):
    results.append((
        rule_id, description, total, tested,
        violations, status, detail
    ))


def validate_condition(rule_id, description, table_name,
                       required_columns, valid_condition,
                       eligible_condition=None):

    if not spark.catalog.tableExists(table_name):
        record_result(
            rule_id, description, None, None, None,
            "SKIPPED", f"Missing table: {table_name}"
        )
        return

    df = spark.table(table_name)

    missing = set(required_columns) - set(df.columns)

    if missing:
        record_result(
            rule_id, description, None, None, None,
            "SKIPPED", f"Missing columns: {sorted(missing)}"
        )
        return

    if eligible_condition is None:
        eligible_condition = F.lit(True)

    stats = df.agg(
        F.count("*").alias("total"),
        F.sum(
            F.when(eligible_condition, 1).otherwise(0)
        ).alias("tested"),
        F.sum(
            F.when(
                eligible_condition & ~valid_condition,
                1
            ).otherwise(0)
        ).alias("violations")
    ).first()

    total = int(stats["total"])
    tested = int(stats["tested"] or 0)
    violations = int(stats["violations"] or 0)

    status = (
        "BLOCKED" if tested == 0
        else "REVIEW" if violations > 0
        else "PASS"
    )

    record_result(
        rule_id, description, total, tested,
        violations, status,
        f"Excluded or incomplete rows: {total - tested}"
    )


# ------------------------------------------------------------
# 2. Date validity — employee birth and hire dates
# ------------------------------------------------------------

validate_condition(
    "DATE-001",
    "Employee birth date must precede hire date",
    "bronze.erp_employees",
    ["birth_date", "hire_date"],
    F.col("birth_date") < F.col("hire_date"),
    F.col("birth_date").isNotNull()
    & F.col("hire_date").isNotNull()
)

validate_condition(
    "DATE-002",
    "Employee hire date must not be after dataset cutoff",
    "bronze.erp_employees",
    ["hire_date"],
    F.to_date("hire_date") <= F.lit("2025-12-31").cast("date"),
    F.col("hire_date").isNotNull()
)

# ------------------------------------------------------------
# 3. POS transaction date validity — all six years
# ------------------------------------------------------------

pos_tables = [
    f"bronze.pos_sales_transactions_{year}"
    for year in range(2020, 2026)
]

pos_frames = []

for year, table_name in zip(range(2020, 2026), pos_tables):

    if not spark.catalog.tableExists(table_name):
        record_result(
            f"DATE-POS-{year}",
            "POS transaction date within source year",
            None, None, None,
            "SKIPPED", f"Missing table: {table_name}"
        )
        continue

    df = spark.table(table_name)

    required = {"transaction_id", "transaction_ts"}

    if not required.issubset(set(df.columns)):
        record_result(
            f"DATE-POS-{year}",
            "POS transaction date within source year",
            None, None, None,
            "SKIPPED", "Required POS columns missing"
        )
        continue

    validate_condition(
        f"DATE-POS-{year}",
        "POS transaction date within source year",
        table_name,
        ["transaction_ts"],
        F.year("transaction_ts") == year,
        F.col("transaction_ts").isNotNull()
    )

    pos_frames.append(
        df.select(
            F.col("transaction_id").cast("string")
                .alias("transaction_id"),
            F.col("transaction_ts")
                .alias("transaction_ts")
        )
    )

# ------------------------------------------------------------
# 4. Cross-table validation — sale before return
# ------------------------------------------------------------

returns_table = "bronze.pos_returns_final"

if (
    len(pos_frames) == 6
    and spark.catalog.tableExists(returns_table)
):

    returns_df = spark.table(returns_table)

    required = {
        "original_transaction_id",
        "return_ts"
    }

    if required.issubset(set(returns_df.columns)):

        pos_all = reduce(
            lambda a, b: a.unionByName(b),
            pos_frames
        )

        pos_lookup = (
            pos_all
            .groupBy("transaction_id")
            .agg(
                F.min("transaction_ts").alias("sale_ts"),
                F.count("*").alias("parent_matches")
            )
        )

        joined = (
            returns_df
            .select(
                F.col("original_transaction_id")
                    .cast("string").alias("source_id"),
                F.col("return_ts")
            )
            .join(
                pos_lookup,
                F.col("source_id")
                == F.col("transaction_id"),
                "left"
            )
        )

        stats = joined.agg(
            F.count("*").alias("total"),

            F.sum(
                F.when(
                    F.col("source_id").isNotNull()
                    & F.col("sale_ts").isNotNull()
                    & (F.col("parent_matches") == 1)
                    & F.col("return_ts").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("tested"),

            F.sum(
                F.when(
                    F.col("source_id").isNotNull()
                    & F.col("sale_ts").isNotNull()
                    & (F.col("parent_matches") == 1)
                    & F.col("return_ts").isNotNull()
                    & (F.col("return_ts") < F.col("sale_ts")),
                    1
                ).otherwise(0)
            ).alias("violations")
        ).first()

        total = int(stats["total"])
        tested = int(stats["tested"] or 0)
        violations = int(stats["violations"] or 0)

        record_result(
            "CROSS-001",
            "Return must not precede original sale",
            total, tested, violations,
            "BLOCKED" if tested == 0
            else "REVIEW" if violations else "PASS",
            f"Untested or unresolved rows: {total - tested}"
        )

    else:
        record_result(
            "CROSS-001",
            "Return must not precede original sale",
            None, None, None,
            "SKIPPED", "Return columns missing"
        )

else:
    record_result(
        "CROSS-001",
        "Return must not precede original sale",
        None, None, None,
        "BLOCKED", "POS or returns source unavailable"
    )

# ------------------------------------------------------------
# 5. Cross-table validation — prescription before dispense
# ------------------------------------------------------------

rx_table = "bronze.rx_prescriptions_fixed"
dispense_table = "bronze.rx_prescription_dispenses_fixed"

if (
    spark.catalog.tableExists(rx_table)
    and spark.catalog.tableExists(dispense_table)
):

    rx = spark.table(rx_table)
    disp = spark.table(dispense_table)

    print("RX prescription columns:", rx.columns)
    print("RX dispense columns:", disp.columns)

    # Detect a shared prescription identifier.
    key_candidates = [
        "prescription_id",
        "rx_id"
    ]

    shared_keys = [
        key for key in key_candidates
        if key in rx.columns and key in disp.columns
    ]

    if (
        shared_keys
        and "prescribed_ts" in rx.columns
        and "dispense_ts" in disp.columns
    ):

        key = shared_keys[0]

        rx_lookup = (
            rx.groupBy(key)
            .agg(
                F.min("prescribed_ts").alias("prescribed_ts"),
                F.count("*").alias("parent_matches")
            )
        )

        joined = (
            disp.select(key, "dispense_ts")
            .join(rx_lookup, key, "left")
        )

        stats = joined.agg(
            F.count("*").alias("total"),

            F.sum(
                F.when(
                    F.col("prescribed_ts").isNotNull()
                    & F.col("dispense_ts").isNotNull()
                    & (F.col("parent_matches") == 1),
                    1
                ).otherwise(0)
            ).alias("tested"),

            F.sum(
                F.when(
                    F.col("prescribed_ts").isNotNull()
                    & F.col("dispense_ts").isNotNull()
                    & (F.col("parent_matches") == 1)
                    & (
                        F.col("dispense_ts")
                        < F.col("prescribed_ts")
                    ),
                    1
                ).otherwise(0)
            ).alias("violations")
        ).first()

        total = int(stats["total"])
        tested = int(stats["tested"] or 0)
        violations = int(stats["violations"] or 0)

        record_result(
            "CROSS-002",
            "Dispense must not precede prescription",
            total, tested, violations,
            "BLOCKED" if tested == 0
            else "REVIEW" if violations else "PASS",
            f"Untested or unresolved rows: {total - tested}"
        )

    else:
        record_result(
            "CROSS-002",
            "Dispense must not precede prescription",
            None, None, None,
            "BLOCKED",
            "No verified shared prescription key or timestamps"
        )

else:
    record_result(
        "CROSS-002",
        "Dispense must not precede prescription",
        None, None, None,
        "BLOCKED", "Required RX source unavailable"
    )

# ------------------------------------------------------------
# 6. Final results
# ------------------------------------------------------------

schema = T.StructType([
    T.StructField("rule_id", T.StringType()),
    T.StructField("description", T.StringType()),
    T.StructField("total_rows", T.LongType()),
    T.StructField("tested_rows", T.LongType()),
    T.StructField("violations", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("detail", T.StringType())
])

temporal_final_df = spark.createDataFrame(
    results, schema
)

print("=== CELL 52 — VALIDATION SUMMARY ===")

display(
    temporal_final_df
    .groupBy("status")
    .count()
    .orderBy("status")
)

print("=== ALL RULE RESULTS ===")

display(
    temporal_final_df.orderBy("rule_id")
)

print("=== EXCEPTIONS ===")

display(
    temporal_final_df
    .filter(F.col("status") != "PASS")
    .orderBy("rule_id")
)

# Persist the validation evidence.
output_path = (
    "Files/silver_discovery/"
    "cell52_temporal_validation"
)

(
    temporal_final_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(output_path)
)

print(f"Exported to: {output_path}")
print("CELL 52 COMPLETE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 53 — RX PRESCRIPTION TO DISPENSE CHRONOLOGY
# ============================================================

from pyspark.sql import functions as F

rx = spark.table("bronze.rx_prescriptions_fixed")
disp = spark.table("bronze.rx_prescription_dispenses_fixed")

# Discover the actual prescription items table.
candidates = [
    t.name for t in spark.catalog.listTables("bronze")
    if "prescription" in t.name.lower()
    and "item" in t.name.lower()
]

print("Candidate item tables:", candidates)

valid_tables = []

for name in candidates:
    df = spark.table(f"bronze.{name}")
    required = {"prescription_item_id", "prescription_id"}

    if required.issubset(set(df.columns)):
        valid_tables.append(f"bronze.{name}")

if len(valid_tables) != 1:
    print("BLOCKED: Expected exactly one verified items table.")
    print("Matching tables:", valid_tables)

else:
    items = spark.table(valid_tables[0])

    # Verify key uniqueness before joining.
    item_keys = items.groupBy("prescription_item_id").count()
    rx_keys = rx.groupBy("prescription_id").count()

    duplicate_item_keys = item_keys.filter("count > 1").count()
    duplicate_rx_keys = rx_keys.filter("count > 1").count()

    print("Duplicate item keys:", duplicate_item_keys)
    print("Duplicate prescription keys:", duplicate_rx_keys)

    if duplicate_item_keys or duplicate_rx_keys:
        print("BLOCKED: Parent keys are not unique.")

    else:
        # Preserve one row per dispense.
        joined = (
            disp.alias("d")
            .join(
                items.select(
                    "prescription_item_id",
                    "prescription_id"
                ).alias("i"),
                F.col("d.prescription_item_id")
                == F.col("i.prescription_item_id"),
                "left"
            )
            .join(
                rx.select(
                    "prescription_id",
                    "prescribed_ts"
                ).alias("r"),
                F.col("i.prescription_id")
                == F.col("r.prescription_id"),
                "left"
            )
        )

        stats = joined.agg(
            F.count("*").alias("total_dispenses"),

            F.sum(F.when(
                F.col("r.prescribed_ts").isNotNull()
                & F.col("d.dispense_ts").isNotNull(),
                1
            ).otherwise(0)).alias("tested"),

            F.sum(F.when(
                F.col("r.prescribed_ts").isNotNull()
                & F.col("d.dispense_ts").isNotNull()
                & (
                    F.col("d.dispense_ts")
                    < F.col("r.prescribed_ts")
                ),
                1
            ).otherwise(0)).alias("violations")
        ).first()

        total = stats["total_dispenses"]
        tested = stats["tested"]
        violations = stats["violations"]
        unresolved = total - tested

        print("=== RX TEMPORAL VALIDATION ===")
        print("Total dispenses:", total)
        print("Tested:", tested)
        print("Unresolved:", unresolved)
        print("Temporal violations:", violations)

        if unresolved > 0 or violations > 0:
            print("STATUS: REVIEW")
        else:
            print("STATUS: PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 54 — RX DISPENSE GRAIN VALIDATION
# ============================================================

from pyspark.sql import functions as F

disp = spark.table("bronze.rx_prescription_dispenses_fixed")

# Group by the candidate dispense key, including NULL values.
dispense_key_counts = (
    disp.groupBy("dispense_id")
        .count()
)

key_profile = (
    dispense_key_counts
        .agg(
            F.sum("count").alias("total_rows"),

            F.sum(
                F.when(
                    F.col("dispense_id").isNull(),
                    F.col("count")
                ).otherwise(0)
            ).alias("null_id_rows"),

            F.sum(
                F.when(
                    F.col("dispense_id").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("distinct_non_null_ids"),

            F.sum(
                F.when(
                    F.col("dispense_id").isNotNull()
                    & (F.col("count") > 1),
                    1
                ).otherwise(0)
            ).alias("duplicate_id_values"),

            F.sum(
                F.when(
                    F.col("dispense_id").isNotNull()
                    & (F.col("count") > 1),
                    F.col("count") - 1
                ).otherwise(0)
            ).alias("duplicate_extra_rows")
        )
        .first()
)

passed = (
    key_profile["null_id_rows"] == 0
    and key_profile["duplicate_extra_rows"] == 0
)

print("=== RX DISPENSE GRAIN VALIDATION ===")
print("Total rows:", key_profile["total_rows"])
print("Distinct non-null dispense IDs:", key_profile["distinct_non_null_ids"])
print("Rows with NULL dispense_id:", key_profile["null_id_rows"])
print("Duplicate dispense_id values:", key_profile["duplicate_id_values"])
print("Duplicate extra rows:", key_profile["duplicate_extra_rows"])
print("STATUS:", "PASS" if passed else "REVIEW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## RX Dispense Discovery Contract — Draft
# 
# **Status:** Discovery evidence verified; Silver contract and implementation pending.
# 
# ### Source and grain
# 
# - Source: `bronze.rx_prescription_dispenses_fixed`
# - Verified grain: one row per `dispense_id`
# - Rows tested: 173,913
# - Distinct non-null `dispense_id` values: 173,913
# - Null IDs: 0
# - Duplicate IDs: 0
# 
# ### Prescription relationship
# 
# Each dispense is linked through:
# 
# `rx_prescription_dispenses_fixed.prescription_item_id`
# → `rx_prescription_items_final.prescription_item_id`
# → `rx_prescription_items_final.prescription_id`
# → `rx_prescriptions_fixed.prescription_id`
# 
# The parent item and prescription keys were unique in the validation run.
# 
# ### Temporal rule
# 
# `dispense_ts >= prescribed_ts`
# 
# - Dispenses tested: 173,913
# - Unresolved relationships or timestamps: 0
# - Temporal violations: 0
# - Result: PASS
# 
# ### Scope and open items
# 
# - Bronze data remains unchanged.
# - No Silver transformation was implemented by these discovery checks.
# - Quantity rules, final Silver column contract, and Silver target table remain to be defined.


# CELL ********************

# CELL 55 — TARGETED PRODUCT KEY VALIDATION

from pyspark.sql import functions as F

product_ids_df = spark.sql("""
    SELECT CAST(product_id AS STRING) AS product_id
    FROM `Pharmacy-Enterprise-DW-DEV`.`lh_bronze`.`bronze`.`reference_products_final_11274`
""").select(
    F.trim(F.col("product_id")).alias("product_id")
)

metrics = product_ids_df.agg(
    F.count("*").alias("source_rows"),
    F.sum(
        F.when(
            F.col("product_id").isNull() | (F.col("product_id") == ""),
            1
        ).otherwise(0)
    ).alias("null_or_blank_ids"),
    F.countDistinct(
        F.when(
            F.col("product_id").isNotNull() & (F.col("product_id") != ""),
            F.col("product_id")
        )
    ).alias("distinct_valid_ids")
).first()

duplicate_id_groups = (
    product_ids_df
        .filter(
            F.col("product_id").isNotNull()
            & (F.col("product_id") != "")
        )
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

source_rows = metrics["source_rows"]
invalid_ids = metrics["null_or_blank_ids"] or 0
distinct_ids = metrics["distinct_valid_ids"]

print("Source rows:", source_rows)
print("NULL or blank product IDs:", invalid_ids)
print("Distinct valid product IDs:", distinct_ids)
print("Duplicate product ID groups:", duplicate_id_groups)

passed = (
    source_rows == 11274
    and invalid_ids == 0
    and distinct_ids == source_rows
    and duplicate_id_groups == 0
)

print("STATUS:", "PASS" if passed else "REVIEW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver Implementation Record — Products
# 
# - **Bronze source:** `bronze.reference_products_final_11274`
# - **Bronze product key validation:** `CELL 71` — PASS; 11,274 rows, 11,274 distinct `product_id` values, 0 null/blank IDs, 0 duplicate groups.
# - **Silver target:** `lh_silver.silver.products`
# - **Implementation notebook:** `nb_06_silver_product_transform`, CELL 04.
# - **Implementation status:** COMPLETED — PASS.
# - **Rows written:** 11,274.
# - **Transformation:** Text values are trimmed; whitespace-only values become NULL; existing NULLs are preserved; `list_price_sar` is stored as `decimal(18,2)`.
# - **Silver verification:** 11,274 rows; 11,274 distinct product IDs; 0 invalid IDs; 0 duplicate extra rows; 0 price conversion failures.
# 
# The Bronze key test is already complete. Do not repeat it for this implementation record.

# CELL ********************

from pyspark.sql import functions as F

tables = spark.sql("SHOW TABLES IN bronze")

display(
    tables.filter(
        F.lower(F.col("tableName")).contains("batch")
        | F.lower(F.col("tableName")).contains("inventory")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

batch_df = spark.table("bronze.supply_chain_product_batches_final")

print("Source table: bronze.supply_chain_product_batches_final")
print("Column count:", len(batch_df.columns))
batch_df.printSchema()
display(batch_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

batch_source_df = spark.table("bronze.supply_chain_product_batches_final")

batch_id_groups_df = (
    batch_source_df
    .groupBy("batch_id")
    .agg(F.count("*").alias("rows_per_batch_id"))
)

valid_id = (
    F.col("batch_id").isNotNull()
    & (F.trim(F.col("batch_id")) != "")
)

display(
    batch_id_groups_df.agg(
        F.sum("rows_per_batch_id").alias("source_rows"),
        F.sum(
            F.when(~valid_id, F.col("rows_per_batch_id")).otherwise(0)
        ).alias("null_or_blank_id_rows"),
        F.sum(F.when(valid_id, 1).otherwise(0)).alias("distinct_valid_batch_ids"),
        F.sum(
            F.when(valid_id & (F.col("rows_per_batch_id") > 1), 1).otherwise(0)
        ).alias("duplicate_id_groups"),
        F.sum(
            F.when(
                valid_id & (F.col("rows_per_batch_id") > 1),
                F.col("rows_per_batch_id") - 1
            ).otherwise(0)
        ).alias("duplicate_extra_rows")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

batch_keys_df = (
    spark.table("bronze.supply_chain_product_batches_final")
    .select(
        "batch_id",
        F.trim(F.col("product_id")).alias("product_id")
    )
)

product_keys_df = (
    spark.table("bronze.reference_products_final_11274")
    .select(F.trim(F.col("product_id")).alias("product_id"))
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
    .distinct()
    .withColumn("product_match", F.lit(1))
)

checked_df = batch_keys_df.join(product_keys_df, "product_id", "left")

display(
    checked_df.agg(
        F.count("*").alias("batch_rows"),
        F.sum(
            F.when(
                F.col("product_id").isNull() | (F.col("product_id") == ""),
                1
            ).otherwise(0)
        ).alias("null_or_blank_product_ids"),
        F.sum(
            F.when(
                F.col("product_id").isNotNull()
                & (F.col("product_id") != "")
                & F.col("product_match").isNull(),
                1
            ).otherwise(0)
        ).alias("orphan_batch_rows"),
        F.countDistinct(
            F.when(F.col("product_match").isNull(), F.col("product_id"))
        ).alias("unmatched_product_ids")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

supplier_tables_df = (
    spark.sql("SHOW TABLES IN bronze")
    .filter(F.lower(F.col("tableName")).contains("supplier"))
    .select("tableName")
    .orderBy("tableName")
)

display(supplier_tables_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

supplier_df = spark.table("bronze.supply_chain_suppliers")

supplier_df.printSchema()
display(supplier_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

supplier_df = spark.table("bronze.supply_chain_suppliers")

supplier_id_groups_df = (
    supplier_df
    .groupBy("supplier_id")
    .agg(F.count("*").alias("rows_per_supplier_id"))
)

invalid_supplier_id = (
    F.col("supplier_id").isNull()
    | (F.trim(F.col("supplier_id")) == "")
)

display(
    supplier_id_groups_df.agg(
        F.sum("rows_per_supplier_id").alias("supplier_rows"),
        F.sum(
            F.when(invalid_supplier_id, F.col("rows_per_supplier_id")).otherwise(0)
        ).alias("null_or_blank_supplier_id_rows"),
        F.sum(
            F.when(~invalid_supplier_id, 1).otherwise(0)
        ).alias("distinct_valid_supplier_ids"),
        F.sum(
            F.when(
                (~invalid_supplier_id) & (F.col("rows_per_supplier_id") > 1),
                1
            ).otherwise(0)
        ).alias("duplicate_supplier_id_groups"),
        F.sum(
            F.when(
                (~invalid_supplier_id) & (F.col("rows_per_supplier_id") > 1),
                F.col("rows_per_supplier_id") - 1
            ).otherwise(0)
        ).alias("duplicate_extra_rows")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

batch_supplier_df = (
    spark.table("bronze.supply_chain_product_batches_final")
    .select("batch_id", F.trim(F.col("supplier_id")).alias("supplier_id"))
)

supplier_keys_df = (
    spark.table("bronze.supply_chain_suppliers")
    .select(F.trim(F.col("supplier_id")).alias("supplier_id"))
    .filter(
        F.col("supplier_id").isNotNull()
        & (F.col("supplier_id") != "")
    )
    .distinct()
    .withColumn("supplier_match", F.lit(1))
)

checked_df = batch_supplier_df.join(
    supplier_keys_df, "supplier_id", "left"
)

valid_supplier_id = (
    F.col("supplier_id").isNotNull()
    & (F.col("supplier_id") != "")
)

display(
    checked_df.agg(
        F.count("*").alias("batch_rows"),
        F.sum(F.when(~valid_supplier_id, 1).otherwise(0))
         .alias("null_or_blank_supplier_ids"),
        F.sum(
            F.when(
                valid_supplier_id & F.col("supplier_match").isNull(), 1
            ).otherwise(0)
        ).alias("orphan_batch_rows"),
        F.countDistinct(
            F.when(
                valid_supplier_id & F.col("supplier_match").isNull(),
                F.col("supplier_id")
            )
        ).alias("unmatched_supplier_ids")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

batch_dates_df = spark.table("bronze.supply_chain_product_batches_final")

display(
    batch_dates_df.agg(
        F.count("*").alias("batch_rows"),
        F.sum(
            F.when(F.col("manufacture_date").isNull(), 1).otherwise(0)
        ).alias("null_manufacture_dates"),
        F.sum(
            F.when(F.col("expiry_date").isNull(), 1).otherwise(0)
        ).alias("null_expiry_dates"),
        F.sum(
            F.when(F.col("received_date").isNull(), 1).otherwise(0)
        ).alias("null_received_dates"),
        F.sum(
            F.when(
                F.col("manufacture_date").isNotNull()
                & F.col("expiry_date").isNotNull()
                & (F.col("manufacture_date") > F.col("expiry_date")),
                1
            ).otherwise(0)
        ).alias("manufacture_after_expiry"),
        F.sum(
            F.when(
                F.col("manufacture_date").isNotNull()
                & F.col("received_date").isNotNull()
                & (F.col("manufacture_date") > F.col("received_date")),
                1
            ).otherwise(0)
        ).alias("manufacture_after_received"),
        F.sum(
            F.when(
                F.col("received_date").isNotNull()
                & F.col("expiry_date").isNotNull()
                & (F.col("received_date") > F.col("expiry_date")),
                1
            ).otherwise(0)
        ).alias("received_after_expiry")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

batch_cost_df = spark.table(
    "bronze.supply_chain_product_batches_final"
)

cost = F.col("unit_cost_sar")

cost_profile_df = batch_cost_df.agg(
    F.count("*").alias("batch_rows"),

    F.sum(
        F.when(cost.isNull(), 1).otherwise(0)
    ).alias("null_unit_cost"),

    F.sum(
        F.when(cost < 0, 1).otherwise(0)
    ).alias("negative_unit_cost"),

    F.sum(
        F.when(cost == 0, 1).otherwise(0)
    ).alias("zero_unit_cost"),

    F.min(cost).alias("min_unit_cost_sar"),
    F.max(cost).alias("max_unit_cost_sar"),

    F.sum(
        F.when(
            cost.isNotNull()
            & (F.abs(cost - F.round(cost, 2)) > 0.000001),
            1
        ).otherwise(0)
    ).alias("cost_values_with_more_than_2_decimals")
)

display(cost_profile_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

batch_df = spark.table(
    "bronze.supply_chain_product_batches_final"
)

recall = F.col("recall_flag")
temperature = F.col("temperature_class")

batch_domain_summary_df = batch_df.agg(
    F.count("*").alias("batch_rows"),

    F.sum(
        F.when(recall.isNull(), 1).otherwise(0)
    ).alias("null_recall_flag"),

    F.sum(
        F.when(
            recall.isNotNull() & (~recall.isin(0, 1)),
            1
        ).otherwise(0)
    ).alias("recall_flag_outside_0_1"),

    F.sum(
        F.when(temperature.isNull(), 1).otherwise(0)
    ).alias("null_temperature_class"),

    F.sum(
        F.when(
            temperature.isNotNull()
            & (F.trim(temperature) == ""),
            1
        ).otherwise(0)
    ).alias("blank_temperature_class")
)

print("Domain summary")
display(batch_domain_summary_df)

print("Recall flag values")
display(
    batch_df.groupBy("recall_flag")
    .count()
    .orderBy("recall_flag")
)

print("Temperature class values")
display(
    batch_df.groupBy("temperature_class")
    .count()
    .orderBy("temperature_class")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Bronze Discovery Record — Product Batches
# 
# **Source:** `bronze.supply_chain_product_batches_final`  
# **Source rows:** 40,000  
# **Discovery status:** PASS  
# **Silver implementation:** PENDING
# 
# ### Verified evidence
# 
# - `batch_id`: 40,000 distinct, non-null keys; no duplicates.
# - Product references: no null or orphan `product_id` values.
# - Supplier references: no null or orphan `supplier_id` values.
# - Required dates: no nulls; no chronology violations.
# - `unit_cost_sar`: no null, negative, or zero values; range 2.44–271.67 SAR; no values with more than two decimal places.
# - `recall_flag`: valid 0/1 domain; 39,910 rows are 0 and 90 rows are 1.
# - `temperature_class`: no null or blank values; Ambient 37,603, Cold Chain 2,397.
# 
# ### Silver mapping proposal — pending implementation
# 
# - Preserve one row per `batch_id`.
# - Map `unit_cost_sar` to `DECIMAL(18,2)`.
# - Map `recall_flag` to a Boolean field named `is_recalled`.
# - Preserve `temperature_class` values as observed: `Ambient`, `Cold Chain`.
# - Preserve the source dates and batch, product, and supplier identifiers.
# - Do not infer temperature thresholds or alter the Bronze source.
# 
# ### Completion note
# 
# Discovery checks are complete for this Bronze entity. Confirm the Silver Lakehouse and schema target before writing the Silver table.


# CELL ********************

# SUPPLIER FLAG DOMAIN CHECK
# فحص جديد لتحديد هل يمكن تحويل cold_chain_capable إلى Boolean

from pyspark.sql import functions as F

supplier_df = spark.table("bronze.supply_chain_suppliers")

supplier_flag_profile = supplier_df.agg(
    F.count("*").alias("supplier_rows"),
    F.sort_array(
        F.collect_set("cold_chain_capable")
    ).alias("observed_cold_chain_values"),
    F.sum(
        F.when(
            F.col("cold_chain_capable").isNull()
            | (~F.col("cold_chain_capable").isin(0, 1)),
            1
        ).otherwise(0)
    ).alias("null_or_non_binary_rows")
)

display(supplier_flag_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
