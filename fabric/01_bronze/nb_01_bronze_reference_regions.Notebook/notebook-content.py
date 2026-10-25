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

# Define the source CSV path
file_path = "Files/raw/reference/regions.csv"

print(file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the raw CSV file into a Spark DataFrame
df_regions = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(file_path)
)

display(df_regions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect schema and row count
df_regions.printSchema()

row_count = df_regions.count()

print(f"Row count: {row_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check null values in each column
from pyspark.sql.functions import col, sum as spark_sum

null_counts = df_regions.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df_regions.columns
])

display(null_counts)

# Check duplicate region IDs
duplicate_region_ids = (
    df_regions
    .groupBy("region_id")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_region_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the Bronze schema if it does not exist
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the DataFrame as a Bronze Delta table
(
    df_regions.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("bronze.reference_regions")
)

print("Bronze table created: bronze.reference_regions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate RAW vs Bronze row counts

raw_count = df_regions.count()

bronze_df = spark.table("bronze.reference_regions")
bronze_count = bronze_df.count()

print(f"RAW row count    : {raw_count}")
print(f"Bronze row count : {bronze_count}")
print(f"Difference       : {bronze_count - raw_count}")

assert raw_count == bronze_count, "Row count mismatch between RAW and Bronze"

print("PASS: RAW and Bronze row counts match.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate the business key uniqueness

total_rows = bronze_df.count()

distinct_region_ids = (
    bronze_df
    .select("region_id")
    .distinct()
    .count()
)

duplicate_region_ids = total_rows - distinct_region_ids

print(f"Total rows          : {total_rows}")
print(f"Distinct region IDs : {distinct_region_ids}")
print(f"Duplicate IDs       : {duplicate_region_ids}")

assert duplicate_region_ids == 0, "Duplicate region_id values detected"

print("PASS: region_id is unique.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate NULL values in the Bronze table

from pyspark.sql.functions import col, sum as spark_sum

null_counts = bronze_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in bronze_df.columns
])

display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate the critical business key

invalid_region_ids = (
    bronze_df
    .filter(
        col("region_id").isNull() |
        (col("region_id") == "")
    )
    .count()
)

print(f"Invalid region_id values : {invalid_region_ids}")

assert invalid_region_ids == 0, "Invalid region_id values detected"

print("PASS: region_id contains no NULL or empty values.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate region_id format

invalid_format_count = (
    bronze_df
    .filter(~col("region_id").rlike(r"^R\d{2}$"))
    .count()
)

print(f"Invalid region_id format : {invalid_format_count}")

assert invalid_format_count == 0, "Invalid region_id format detected"

print("PASS: all region_id values follow the expected R## format.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify that the Bronze Delta table is registered

spark.sql("""
    SHOW TABLES IN bronze
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Recursively inventory all CSV files in the RAW landing zone

raw_root = "Files/raw"

csv_files = []

def scan_folder(path):
    items = notebookutils.fs.ls(path)

    for item in items:
        if item.isDir:
            scan_folder(item.path)
        elif item.name.lower().endswith(".csv"):
            csv_files.append(item.path)

scan_folder(raw_root)

print(f"Total RAW CSV files in OneLake: {len(csv_files)}")

assert len(csv_files) == 72, (
    f"Expected 72 RAW CSV files, but found {len(csv_files)}"
)

print("PASS: All 72 RAW CSV files are present in OneLake.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Re-discover all RAW CSV files directly from OneLake

raw_root = "Files/raw"

csv_files = []

def find_csv_files(path):
    for item in notebookutils.fs.ls(path):
        if item.isDir:
            find_csv_files(item.path)
        elif item.name.lower().endswith(".csv"):
            csv_files.append(item.path)

find_csv_files(raw_root)

print(f"RAW CSV files found: {len(csv_files)}")

assert len(csv_files) == 72, \
    f"Expected 72 RAW CSV files, but found {len(csv_files)}"

print("PASS: All 72 RAW CSV files are available in OneLake.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
