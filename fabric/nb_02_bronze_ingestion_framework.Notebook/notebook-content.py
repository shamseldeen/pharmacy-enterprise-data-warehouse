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

# MARKDOWN ********************

# # 💊 Pharmacy Enterprise Data Warehouse
# 
# ## 🥉 Notebook 02 — Bronze Metadata-Driven Ingestion Framework
# 
# **Author:** Shamseldeen Ismaiil  
# **Platform:** Microsoft Fabric  
# **Lakehouse:** `lh_bronze`  
# **Layer:** Bronze  
# **Notebook:** `nb_02_bronze_ingestion_framework`  
# **Status:** 🚧 In Development
# 
# ---
# 
# ## 🎯 Objective
# 
# Build a reusable **metadata-driven ingestion framework** that discovers,
# validates, and prepares the finalized pharmacy RAW dataset for ingestion
# from **Microsoft OneLake** into source-aligned **Bronze Delta tables**.
# 
# This notebook documents both the implementation and the engineering decisions
# behind the Bronze ingestion process.
# 
# 
# ## 📦 Dataset Scope
# 
# The finalized RAW dataset represents a large-scale pharmacy enterprise
# environment and is organized into multiple business domains.
# 
# ### Current RAW Scope
# 
# - **72 finalized RAW CSV files**
# - **11 business domains**
# - Historical data covering **2020–2025**
# - RAW landing zone: `Files/raw`
# - Target layer: **Bronze Delta**
# - Architecture pattern: **Medallion Architecture**
# 
# ### 🏢 Business Domains
# 
# `CRM` • `Delivery` • `ERP` • `Finance` • `Insurance` • `Inventory` •
# `Marketing` • `POS` • `Reference` • `RX` • `Supply Chain`
# 
# ### 🏗️ Engineering Decision
# 
# The Bronze layer follows a **source-aligned design**:
# 
# **1 finalized RAW CSV file → 1 Bronze Delta table**
# 
# Year-partitioned source files remain separate in Bronze to preserve
# lineage and simplify reconciliation.
# 
# Logical consolidation and business conformance will be performed later
# in the **Silver layer**.
# 
# 
# ## 🏗️ Bronze Ingestion Architecture
# 
# ```text
# 💊 Pharmacy RAW Data
#         │
#         ▼
# ☁️ Microsoft OneLake
#    Files/raw
#         │
#         ▼
# 🔍 Recursive File Discovery
#         │
#         ▼
# 🧭 Metadata Generation
#         │
#         ▼
# 🛡️ Pre-Ingestion Quality Gates
#         │
#         ▼
# 🧪 Physical File Validation
#         │
#         ▼
# 🥉 Bronze Delta Tables
#         │
#         ▼
# 📋 Audit & Reconciliation 


# MARKDOWN ********************

# ## 🔍 1. RAW File Discovery
# 
# ### 🎯 Purpose
# 
# The first step is to discover all CSV files available in the OneLake
# RAW landing zone.
# 
# The RAW dataset contains nested folders, particularly historical POS files.
# Therefore, a simple one-level directory listing is not sufficient.
# 
# A **recursive discovery function** is used to traverse the complete
# `Files/raw` directory structure and collect every CSV source file.
# 
# ### 🧠 What We Learn
# 
# This step introduces several important Python and Fabric concepts:
# 
# - Working with the OneLake file system using `notebookutils.fs`
# - Traversing folders programmatically
# - Python recursion
# - Filtering files by extension
# - Working with Python lists
# - Extracting metadata from file paths
# 
# ### 🎯 Expected Result
# 
# The finalized RAW dataset contains exactly:
# 
# **72 CSV files**
# 
# A different result indicates that the OneLake RAW landing zone should be
# investigated before ingestion continues.

# CELL ********************

# RAW root path in the Lakehouse
raw_root = "Files/raw"

# Store discovered CSV file paths
csv_files = []


# Recursively discover all CSV files under the RAW folder
def find_csv_files(path):
    items = notebookutils.fs.ls(path)

    for item in items:
        if item.isDir:
            find_csv_files(item.path)

        elif item.name.lower().endswith(".csv"):
            csv_files.append(item.path)


# Start recursive discovery
find_csv_files(raw_root)

# Validate discovered file count
print(f"RAW CSV files found: {len(csv_files)}")


# Extract metadata from the first discovered file
file_source = csv_files[0].split("/raw/")[-1]

domain = file_source.split("/")[0]

file_name = file_source.split("/")[-1]

base_name = file_name.replace(".csv", "")

table_name = f"{domain}_{base_name}"


# Display extracted metadata
print(f"Source     : {file_source}")
print(f"Domain     : {domain}")
print(f"File       : {file_name}")
print(f"Base name  : {base_name}")
print(f"Table name : {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 1.1 Discovery Result
# 
# The recursive discovery process completed successfully.
# 
# **Result: 72 / 72 RAW CSV files discovered — PASS ✅**
# 
# The discovered file count matches the finalized RAW source inventory,
# confirming that the complete expected RAW dataset is available in OneLake.
# 
# The framework also successfully extracted the first file's metadata:
# 
# - **Source:** `crm/customer_consent_history.csv`
# - **Domain:** `crm`
# - **File:** `customer_consent_history.csv`
# - **Base name:** `customer_consent_history`
# - **Generated Bronze table:** `crm_customer_consent_history`
# 
# ### 💡 Learning Note
# 
# `notebookutils.fs.ls()` lists the contents of a directory.
# 
# Because the RAW landing zone contains nested folders, the discovery function
# uses **recursion**: when it encounters another directory, the function calls
# itself to continue exploring that directory.
# 
# This allows a single function to discover CSV files across the entire
# OneLake RAW hierarchy.
# 
# ### 🛡️ Quality Gate
# 
# **Expected files:** 72  
# **Discovered files:** 72  
# **Missing files:** 0  
# **Status:** ✅ PASS


# MARKDOWN ********************

# ## 🧭 2. Metadata Generation
# 
# ### 🎯 Purpose
# 
# After discovering the 72 RAW files, the next step is to convert the physical
# file inventory into structured metadata that can drive the ingestion framework.
# 
# Instead of manually defining the source and target for every file, the
# framework automatically derives the required information from each file path.
# 
# For every RAW CSV file, we generate:
# 
# - `source_path` — Full OneLake path to the physical source file
# - `relative_path` — File location relative to the RAW root
# - `domain` — Business domain such as CRM, POS, or Inventory
# - `file_name` — Original CSV file name
# - `base_name` — File name without the `.csv` extension
# - `table_name` — Generated source-aligned Bronze table name
# 
# ### ⚙️ Metadata-Driven Design
# 
# The resulting metadata will become the control layer for later ingestion.
# 
# Instead of writing separate ingestion code for 72 files:
# 
# **72 RAW files → 72 metadata records → reusable ingestion logic**
# 
# This makes the framework easier to scale, validate, maintain, and audit.
# 
# ### 💡 Learning Note — Why Metadata?
# 
# A metadata-driven framework separates two concerns:
# 
# **What should be processed?**  
# Defined by metadata.
# 
# **How should it be processed?**  
# Defined by reusable ingestion logic.
# 
# For example, the framework does not need separate Python code for CRM,
# Inventory, POS, and Insurance files.
# 
# The same ingestion logic can later iterate through the metadata records and
# process each source using its own path and generated Bronze table name.
# 
# This is a fundamental pattern used in scalable data engineering pipelines.


# CELL ********************

# Create an empty list to store metadata for all RAW CSV files
metadata_list = []

# Loop through every discovered CSV file
for path in csv_files:

    # Get the relative path after /raw/
    file_source = path.split("/raw/")[-1]

    # Extract the business domain
    domain = file_source.split("/")[0]

    # Extract the CSV file name
    file_name = file_source.split("/")[-1]

    # Remove the .csv extension
    base_name = file_name.replace(".csv", "")

    # Generate the Bronze table name
    table_name = f"{domain}_{base_name}"

    # Store the metadata for this file
    metadata_list.append(
        {
            "source_path": path,
            "relative_path": file_source,
            "domain": domain,
            "file_name": file_name,
            "base_name": base_name,
            "table_name": table_name
        }
    )

# Validate the number of metadata records
print(f"Metadata records created: {len(metadata_list)}")

# Display the first metadata record
print(metadata_list[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 2.1 Metadata Generation Result
# 
# The metadata generation process completed successfully.
# 
# **Result: 72 metadata records created — PASS ✅**
# 
# This confirms that every discovered RAW CSV file has a corresponding
# metadata record that can later drive the Bronze ingestion process.
# 
# The first generated metadata record represents:
# 
# - **Domain:** `crm`
# - **Source file:** `customer_consent_history.csv`
# - **Relative path:** `crm/customer_consent_history.csv`
# - **Generated Bronze table:** `crm_customer_consent_history`
# 
# ### 🛡️ Control Check
# 
# **Discovered RAW files:** 72  
# **Generated metadata records:** 72  
# **Difference:** 0  
# **Status:** ✅ PASS
# 
# This establishes the expected relationship:
# 
# **1 RAW file → 1 metadata record**

# MARKDOWN ********************

# ## ⚡ 3. Convert Metadata to a Spark DataFrame
# 
# ### 🎯 Purpose
# 
# The metadata currently exists as a Python list of dictionaries.
# 
# To analyze and validate the metadata using Spark operations, we convert
# the Python collection into a **Spark DataFrame**.
# 
# This gives us a structured tabular representation of the ingestion metadata
# and allows us to use operations such as:
# 
# - `select()`
# - `distinct()`
# - `count()`
# - `groupBy()`
# - `filter()`
# 
# ### 🧠 Why Convert to a DataFrame?
# 
# A Python list is convenient for building and iterating through metadata.
# 
# A Spark DataFrame is more suitable for:
# 
# **validation → analysis → quality checks → scalable processing**
# 
# The DataFrame therefore becomes the validation layer for the metadata
# before any Bronze ingestion begins.

# CELL ********************

# Convert the Python metadata list into a Spark DataFrame
metadata_df = spark.createDataFrame(metadata_list)

# Display the metadata as a table
display(metadata_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 3.1 Metadata DataFrame Result
# 
# The metadata was successfully converted from a Python collection into a
# Spark DataFrame.
# 
# The resulting DataFrame contains **72 metadata records** with the following
# control attributes:
# 
# - `base_name`
# - `domain`
# - `file_name`
# - `relative_path`
# - `source_path`
# - `table_name`
# 
# The metadata is now available in a structured Spark format and can be
# validated before it is used to drive Bronze ingestion.
# 
# ### 💡 Learning Note
# 
# At this stage, no pharmacy business data has been transformed.
# 
# We are validating the **ingestion control metadata** itself.
# 
# This is important because incorrect metadata could cause files to be loaded
# into the wrong tables, create duplicate targets, or break the ingestion
# framework later.

# MARKDOWN ********************

# ## 🛡️ 4. Pre-Ingestion Metadata Quality Gates
# 
# Before any RAW data is written to Bronze, the generated metadata must pass
# a series of quality checks.
# 
# These checks protect the ingestion framework from structural problems before
# large source files are processed.
# 
# ### 🔐 Quality Gate 1 — Bronze Table Name Uniqueness
# 
# Each RAW source file must map to a unique Bronze table name.
# 
# If two different source files generate the same `table_name`, one source
# could overwrite or conflict with another during ingestion.
# 
# Therefore, we compare:
# 
# **Total metadata records**
# 
# with:
# 
# **Distinct generated Bronze table names**
# 
# ### 🎯 Expected Result
# 
# ```text
# Total metadata records      : 72
# Distinct Bronze table names : 72

# MARKDOWN ********************

# If both counts are equal:
# 
# **Quality Gate 1 → PASS ✅**

# CELL ********************

# Count total metadata records
total_records = metadata_df.count()

# Count distinct Bronze table names
distinct_table_names = (
    metadata_df
        .select("table_name")
        .distinct()
        .count()
)

# Display validation counts
print(f"Total metadata records      : {total_records}")
print(f"Distinct Bronze table names : {distinct_table_names}")

# Validate Bronze table name uniqueness
if total_records == distinct_table_names:
    print("PASS: All Bronze table names are unique.")
else:
    print("FAIL: Duplicate Bronze table names found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 4.1 Quality Gate 1 Result
# 
# The Bronze table-name uniqueness validation completed successfully.
# 
# **Total metadata records:** 72  
# **Distinct Bronze table names:** 72  
# **Duplicate table names:** 0  
# 
# **Quality Gate 1 → PASS ✅**
# 
# Every RAW source currently maps to a unique Bronze target table name.
# 
# This confirms the relationship:
# 
# **1 RAW file → 1 unique Bronze table name**

# MARKDOWN ********************

# ### 🔎 4.2 Duplicate Table Name Diagnostic
# 
# Passing the uniqueness count tells us that no duplicate Bronze table names
# currently exist.
# 
# However, an engineering framework should also be able to identify the
# problematic table names if this quality gate fails in the future.
# 
# For this reason, we build a diagnostic check that:
# 
# 1. Groups metadata records by `table_name`
# 2. Counts how many times each table name occurs
# 3. Filters for names appearing more than once
# 4. Displays the conflicting records when duplicates are detected
# 
# ### 💡 Learning Note — `groupBy()` and `filter()`
# 
# The PySpark logic:
# 
# `groupBy("table_name").count()`
# 
# is conceptually similar to:
# 
# `GROUP BY table_name` with `COUNT(*)` in SQL.
# 
# Filtering for:
# 
# `count > 1`
# 
# is conceptually similar to using:
# 
# `HAVING COUNT(*) > 1`
# 
# in SQL.
# 
# This diagnostic pattern does more than detect that a problem exists —
# it helps identify **where the problem is**.

# CELL ********************

# Count occurrences of each Bronze table name
table_name_counts = (
    metadata_df
        .groupBy("table_name")
        .count()
)

# Keep only duplicated table names
duplicate_table_names = (
    table_name_counts
        .filter(table_name_counts["count"] > 1)
)

# Count duplicate table names
duplicate_count = duplicate_table_names.count()

# Validate and display duplicate table names if found
if duplicate_count == 0:
    print("PASS: No duplicate Bronze table names found.")
else:
    print("FAIL: Duplicate Bronze table names found.")
    display(duplicate_table_names)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 4.3 Duplicate Diagnostic Result
# 
# The duplicate table-name diagnostic completed successfully.
# 
# **Duplicate Bronze table names found:** 0
# 
# **Diagnostic Check → PASS ✅**
# 
# No generated Bronze table name is shared by multiple RAW source files.
# 
# This confirms that the current naming logic produces unique target names
# across the complete 72-file RAW inventory.
# 
# > 🛡️ **Engineering Note**
# >
# > The diagnostic remains part of the framework even when no duplicates exist.
# > If a naming collision is introduced in the future, the same check will
# > identify the conflicting table names before Bronze ingestion begins.

# MARKDOWN ********************

# ### 🛡️ 4.4 Quality Gate 2 — Source Path Uniqueness
# 
# The next quality gate validates the opposite side of the mapping.
# 
# We already confirmed that every generated Bronze table name is unique.
# Now we verify that every metadata record also represents a unique physical
# RAW source path.
# 
# The validation compares:
# 
# **Total metadata records**
# 
# with:
# 
# **Distinct RAW source paths**
# 
# ### 🎯 Why This Matters
# 
# A duplicated `source_path` could cause the same RAW file to be represented
# multiple times in the ingestion metadata and potentially processed more than once.
# 
# The expected relationship is:
# 
# **1 physical RAW file → 1 metadata record → 1 Bronze target**
# 
# ### 🎯 Expected Result
# 
# ```text
# Total metadata records : 72
# Distinct source paths  : 72

# CELL ********************

# Count distinct RAW source paths
distinct_source_paths = (
    metadata_df
        .select("source_path")
        .distinct()
        .count()
)

# Display validation results
print(f"Total metadata records : {total_records}")
print(f"Distinct source paths  : {distinct_source_paths}")

# Validate RAW source path uniqueness
if total_records == distinct_source_paths:
    print("PASS: All RAW source paths are unique.")
else:
    print("FAIL: Duplicate RAW source paths found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 4.5 Quality Gate 2 Result
# 
# The RAW source-path uniqueness validation completed successfully.
# 
# **Total metadata records:** 72  
# **Distinct source paths:** 72  
# **Duplicate source paths:** 0  
# 
# **Quality Gate 2 → PASS ✅**
# 
# Every metadata record represents a unique physical RAW source file.
# 
# The ingestion mapping now satisfies:
# 
# **1 physical RAW file → 1 metadata record → 1 unique Bronze target**

# MARKDOWN ********************

# ### 🛡️ 4.6 Quality Gate 3 — CSV Extension Validation
# 
# The ingestion framework is currently designed to process CSV source files.
# 
# This quality gate verifies that every discovered `source_path` ends with
# the expected `.csv` extension.
# 
# Any path that does not satisfy this rule is isolated as an invalid source
# before Bronze ingestion begins.
# 
# ### 💡 Learning Note — Boolean Negation
# 
# The PySpark expression:
# 
# `metadata_df["source_path"].endswith(".csv")`
# 
# returns a Boolean condition identifying paths that end with `.csv`.
# 
# Adding the `~` operator:
# 
# `~metadata_df["source_path"].endswith(".csv")`
# 
# negates that condition.
# 
# Therefore, the filter keeps only paths that **do not** end with `.csv`.
# 
# ### 🎯 Expected Result
# 
# **Invalid file paths: 0**
# 
# If no invalid paths are found:
# 
# **Quality Gate 3 → PASS ✅**

# CELL ********************

# Find source paths that do not end with .csv
invalid_file_paths = (
    metadata_df
        .filter(~metadata_df["source_path"].endswith(".csv"))
)

# Count invalid source paths
invalid_file_count = invalid_file_paths.count()

# Validate file extensions
if invalid_file_count == 0:
    print("PASS: All RAW source files are CSV files.")
else:
    print("FAIL: Non-CSV source files found.")
    display(invalid_file_paths)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 4.7 Quality Gate 3 Result
# 
# The CSV extension validation completed successfully.
# 
# **Invalid file paths:** 0  
# **Validated RAW sources:** 72  
# 
# **Quality Gate 3 → PASS ✅**
# 
# All discovered RAW source paths use the expected `.csv` file extension.
# 
# > ⚠️ **Important Distinction**
# >
# > This validation confirms the expected **file extension and path naming**.
# >
# > It does not yet prove that Spark can physically open, parse, and read
# > the contents of each CSV file.
# >
# > Physical readability is validated in the next stage.

# MARKDOWN ********************

# ## 🧪 5. Physical File Validation
# 
# ### 🎯 Purpose
# 
# The previous quality gates validated the ingestion metadata.
# 
# The next stage moves from **metadata validation** to
# **physical source validation**.
# 
# A file may have:
# 
# - A valid path
# - A `.csv` extension
# - Valid metadata
# - A unique Bronze target
# 
# and still fail when Spark attempts to read its actual contents.
# 
# Therefore, the framework must verify that the physical RAW files are
# readable by the Spark engine before Bronze ingestion begins.
# 
# ### 🧠 Validation Strategy
# 
# We begin with **one representative RAW file** before generalizing the logic
# across all 72 files.
# 
# This allows us to understand and validate the Spark read pattern before
# building the reusable framework.
# 
# The first discovered source is:
# 
# `crm/customer_consent_history.csv`

# MARKDOWN ********************

# ### ⚡ What Are We Testing?
# 
# Spark will attempt to:
# 
# 1. Access the physical file in OneLake
# 2. Read the CSV header
# 3. Parse the source columns
# 4. Infer the source schema
# 5. Create a Spark DataFrame
# 6. Execute a row count against the physical data
# 
# The row count is especially important because it forces Spark to perform
# actual work against the source data.
# 
# ### 💡 Learning Note — Spark Lazy Evaluation
# 
# Spark uses **lazy evaluation**.
# 
# Creating a DataFrame describes a transformation plan, but Spark does not
# necessarily execute the complete workload immediately.
# 
# An action such as:
# 
# `test_df.count()`
# 
# forces Spark to execute the required work and return a result.
# 
# This gives us stronger evidence that the physical source can actually
# be processed.

# CELL ********************

# Select the first RAW file for a physical read test
test_path = metadata_list[0]["source_path"]
test_relative_path = metadata_list[0]["relative_path"]

# Read the CSV file with Spark
test_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(test_path)
)

# Display the inferred schema
print(f"Test file: {test_relative_path}")
test_df.printSchema()

# Force Spark to read the file and count its rows
test_row_count = test_df.count()

print(f"Test file row count: {test_row_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 5.1 Physical Read Test Result
# 
# The first physical RAW file was successfully read from Microsoft OneLake
# using Apache Spark.
# 
# **Test source:** `crm/customer_consent_history.csv`  
# **Rows successfully processed:** **1,200,051**  
# **Columns detected:** **7**  
# **Physical read status:** ✅ PASS
# 
# ### 📋 Inferred Schema
# 
# | Column | Spark Data Type |
# |---|---|
# | `consent_event_id` | `string` |
# | `customer_id` | `string` |
# | `event_timestamp` | `timestamp` |
# | `consent_type` | `string` |
# | `consent_action` | `string` |
# | `channel` | `string` |
# | `source_system` | `string` |
# 
# Spark successfully recognized `event_timestamp` as a timestamp while
# the remaining fields were interpreted as strings.

# MARKDOWN ********************

# ### 💡 Engineering Note — What Did This Test Prove?
# 
# This test moved the framework beyond metadata-only validation.
# 
# We have now demonstrated that Spark can:
# 
# **OneLake path → Open CSV → Parse header → Infer schema → Read data → Execute action**
# 
# The successful count of **1,200,051 rows** provides evidence that the
# selected RAW file is physically accessible and processable by Spark.
# 
# > ⚠️ **Important**
# >
# > This result validates only the representative test file.
# >
# > It does **not** yet prove that all 72 RAW CSV files are physically readable.
# > The next stage will generalize the validation pattern across the complete
# > RAW inventory.

# MARKDOWN ********************

# ## 🔄 6. Full RAW Physical Readability Validation
# 
# ### 🎯 Purpose
# 
# The representative physical read test proved that Spark can successfully
# process one RAW source file.
# 
# The next step is to generalize this validation across the complete
# **72-file RAW inventory**.
# 
# For each source file, the framework will attempt to:
# 
# 1. Access the physical OneLake path
# 2. Open the CSV using Spark
# 3. Parse the source structure
# 4. Execute a Spark action
# 5. Record whether the validation passed or failed
# 
# ### 🛡️ Failure Isolation
# 
# A failure in one source file should not prevent the remaining files from
# being validated.
# 
# Therefore, the validation framework will use Python `try / except`
# error handling.
# 
# Each source will produce its own validation result:
# 
# **Source → PASS ✅**
# 
# or
# 
# **Source → FAIL ❌ + Error Details**
# 
# This allows the complete RAW inventory to be assessed in a single
# validation run.

# CELL ********************

# Store physical validation results
validation_results = []

# Validate every RAW CSV file
for item in metadata_list:

    # Get source information from metadata
    file_path = item["source_path"]
    relative_path = item["relative_path"]
    domain = item["domain"]

    try:
        # Read the physical CSV file
        df = (
            spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(file_path)
        )

        # Force Spark to process the file
        row_count = df.count()

        # Record successful validation
        validation_results.append(
            {
                "relative_path": relative_path,
                "domain": domain,
                "status": "PASS",
                "row_count": row_count,
                "error": "No error"
            }
        )

    except Exception as e:

        # Record failed validation without stopping the loop
        validation_results.append(
            {
                "relative_path": relative_path,
                "domain": domain,
                "status": "FAIL",
                "row_count": None,
                "error": str(e)
            }
        )

# Confirm that every RAW file produced a validation result
print(f"Physical validation results created: {len(validation_results)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🔄 6.1 Validation Execution Result
# 
# The full physical validation loop completed across the entire RAW inventory.
# 
# **Validation records created: 72 / 72 ✅**
# 
# This confirms that every metadata record produced a physical validation result
# and that the `try / except` design allowed the framework to continue processing
# the complete source inventory.
# 
# > ⚠️ **Important**
# >
# > `72 validation results` does not mean that all 72 files passed.
# >
# > Each result may contain either `PASS` or `FAIL`.
# > The next step analyzes the validation results to determine the actual
# > physical readability status of the RAW dataset.

# MARKDOWN ********************

# ## 📊 7. Physical Validation Results Analysis
# 
# ### 🎯 Purpose
# 
# The validation results currently exist as a Python list of dictionaries.
# 
# We will convert these results into a Spark DataFrame so that we can analyze
# the physical validation status using structured Spark operations.
# 
# The analysis will answer three questions:
# 
# 1. Did all 72 sources produce a validation result?
# 2. How many sources passed or failed?
# 3. Which specific sources failed, if any?
# 
# This separates **validation execution** from **validation analysis**.

# CELL ********************

# Convert validation results into a Spark DataFrame
validation_df = spark.createDataFrame(validation_results)

# Display physical validation results
display(validation_df)

# Summarize validation results by status
status_summary = (
    validation_df
        .groupBy("status")
        .count()
)

# Display validation status summary
display(status_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 7.1 Full Physical Validation Result
# 
# The complete RAW physical readability validation finished successfully.
# 
# **RAW sources validated:** 72  
# **PASS:** 72  
# **FAIL:** 0  
# **Physical readability rate:** **100%**  
# 
# **Physical Validation Quality Gate → PASS ✅**
# 
# All 72 finalized RAW CSV files were successfully accessed and processed
# by Apache Spark in Microsoft Fabric.
# 
# This validation goes beyond file discovery. Spark executed a physical
# action against every source file, confirming that the complete RAW inventory
# is readable from OneLake.

# MARKDOWN ********************

# > 💡 **Engineering Note**
# >
# > Successful validations store `"No error"` in the `error` field.
# >
# > If a source fails validation, `row_count` remains `NULL` because no
# > reliable row count was produced, while the actual exception message
# > is stored in the `error` field.
# >
# > This preserves an important distinction:
# >
# > **`row_count = 0` → successfully read empty source**
# >
# > **`row_count = NULL` → row count unavailable because validation failed**

# MARKDOWN ********************

# ## 🥉 8. Bronze Delta Ingestion
# 
# ### 🎯 Purpose
# 
# The RAW dataset has successfully passed the pre-ingestion quality gates
# and physical readability validation.
# 
# The next stage moves from **validation** to **persistent Bronze storage**.
# 
# Each finalized RAW CSV file will be read from OneLake and written as a
# source-aligned **Delta table** in the Bronze layer.
# 
# ### 🏗️ Bronze Design
# 
# The project follows the mapping:
# 
# **1 finalized RAW CSV file → 1 Bronze Delta table**
# 
# The Bronze layer preserves the source structure and lineage as closely
# as possible.
# 
# Year-partitioned RAW files remain separate Bronze tables. Consolidation,
# standardization, deduplication, and business transformations are deferred
# to the Silver layer.
# 
# ### 🔄 Ingestion Pattern
# 
# **RAW CSV → Spark DataFrame → Delta Format → Bronze Table**
# 
# Before generalizing this pattern across all 72 sources, the ingestion logic
# will first be implemented and validated using a single source file.

# CELL ********************

# Select one source file for the first Bronze ingestion test
test_item = metadata_list[0]

# Extract source metadata
file_path = test_item["source_path"]
relative_path = test_item["relative_path"]
table_name = test_item["table_name"]

# Build the fully qualified Bronze table name
bronze_table_name = f"bronze.{table_name}"

# Read the RAW CSV file
test_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
)

# Display source-to-target mapping
print(f"Source : {relative_path}")
print(f"Target : {bronze_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ensure the Bronze schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# Display source-to-target mapping
print(f"Source : {relative_path}")
print(f"Target : {bronze_table_name}")

# Write the RAW source as a Bronze Delta table
(
    test_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(bronze_table_name)
)

print("Bronze ingestion completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the Bronze Delta table
bronze_df = spark.table(bronze_table_name)

# Count rows in RAW and Bronze
raw_row_count = test_df.count()
bronze_row_count = bronze_df.count()

# Calculate the row-count difference
difference = bronze_row_count - raw_row_count

# Display reconciliation results
print(f"RAW row count    : {raw_row_count}")
print(f"Bronze row count : {bronze_row_count}")
print(f"Difference       : {difference}")

# Validate RAW-to-Bronze row-count reconciliation
if raw_row_count == bronze_row_count:
    print("PASS: RAW and Bronze row counts match.")
else:
    print("FAIL: RAW and Bronze row counts do not match.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🧪 8.1 Schema Reconciliation — Column Names
# 
# After validating RAW-to-Bronze row-count reconciliation, the next quality gate
# checks whether the Bronze table preserves the source column structure.
# 
# This validation compares the ordered column names between:
# 
# **RAW Spark DataFrame → Bronze Delta Table**
# 
# The check ensures that:
# 
# - No source column was lost during ingestion.
# - No unexpected column was introduced.
# - Column names remain unchanged.
# - Column order is preserved.
# 
# > 💡 **Engineering Note**
# >
# > Matching column names does not guarantee full schema equivalence.
# > Data types will be validated separately in the next quality gate.

# CELL ********************

# Extract ordered column names from RAW and Bronze
raw_columns = test_df.columns
bronze_columns = bronze_df.columns

# Validate column names and order
if raw_columns == bronze_columns:
    print("PASS: Column names match.")
else:
    print("FAIL: Column names do not match.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ Column-Name Reconciliation Result
# 
# The RAW source and Bronze Delta table contain the same ordered column names.
# 
# **Result:** PASS
# 
# This confirms that the Bronze ingestion preserved the source column structure
# without dropping, adding, renaming, or reordering columns.
# 
# > 💡 **Engineering Note**
# >
# > This quality gate validates column names and order only.
# > Data types must be validated separately to confirm full schema consistency.

# MARKDOWN ********************

# ### 🧠 Understanding the Spark Schema
# 
# Apache Spark represents a DataFrame schema using a `StructType`.
# 
# A `StructType` contains multiple `StructField` objects, where each
# `StructField` describes one column.
# 
# Conceptually:
# 
# **StructType → DataFrame Schema**
# 
# **StructField → One Column Definition**
# 
# Each `StructField` contains:
# 
# 1. Column name
# 2. Data type
# 3. Nullable flag

# CELL ********************

print(test_df.schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract RAW schema field definitions
raw_schema_fields = [
    [field.name, field.dataType, field.nullable]
    for field in test_df.schema.fields
]

# Extract Bronze schema field definitions
bronze_schema_fields = [
    [field.name, field.dataType, field.nullable]
    for field in bronze_df.schema.fields
]

# Validate RAW-to-Bronze schema consistency
if raw_schema_fields == bronze_schema_fields:
    print("PASS: RAW and Bronze schema field definitions match.")
else:
    print("FAIL: RAW and Bronze schema field definitions do not match.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ Full Schema Reconciliation Result
# 
# The RAW source and Bronze Delta table contain matching schema field definitions.
# 
# The validation compared each field across:
# 
# - Column name
# - Data type
# - Nullable property
# 
# **Result:** PASS
# 
# This confirms that the Bronze Delta write preserved the Spark schema inferred
# from the RAW source for the tested ingestion.
# 
# > 💡 **Engineering Note**
# >
# > Row-count reconciliation and schema reconciliation validate different
# > dimensions of ingestion quality.
# >
# > **Row Count** verifies that the expected number of records reached Bronze.
# >
# > **Schema Reconciliation** verifies that the structural definition of those
# > records was preserved.

# CELL ********************

import time


def ingest_bronze_file(item):

    # Extract source metadata
    file_path = item["source_path"]
    relative_path = item["relative_path"]
    table_name = item["table_name"]

    # Build the fully qualified Bronze table name
    bronze_table_name = f"bronze.{table_name}"

    # Start ingestion timer
    start_time = time.perf_counter()

    try:

        # Read RAW CSV
        df = (
            spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(file_path)
        )

        # Count RAW rows
        raw_row_count = df.count()

        # Write RAW data to Bronze Delta
        (
            df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(bronze_table_name)
        )

        # Read Bronze table back
        bronze_df = spark.table(bronze_table_name)

        # Count Bronze rows
        bronze_row_count = bronze_df.count()

        # Validate row-count reconciliation
        row_count_status = (
            "PASS"
            if raw_row_count == bronze_row_count
            else "FAIL"
        )

        # Extract RAW schema field definitions
        raw_schema_fields = [
            [field.name, field.dataType, field.nullable]
            for field in df.schema.fields
        ]

        # Extract Bronze schema field definitions
        bronze_schema_fields = [
            [field.name, field.dataType, field.nullable]
            for field in bronze_df.schema.fields
        ]

        # Validate schema reconciliation
        schema_status = (
            "PASS"
            if raw_schema_fields == bronze_schema_fields
            else "FAIL"
        )

        # Determine overall ingestion status
        overall_status = (
            "PASS"
            if row_count_status == "PASS"
            and schema_status == "PASS"
            else "FAIL"
        )

        # Calculate total processing duration
        duration_seconds = round(
            time.perf_counter() - start_time,
            2
        )

        # Return successful ingestion result
        return {
            "relative_path": relative_path,
            "bronze_table": bronze_table_name,
            "raw_row_count": raw_row_count,
            "bronze_row_count": bronze_row_count,
            "row_count_status": row_count_status,
            "schema_status": schema_status,
            "overall_status": overall_status,
            "duration_seconds": duration_seconds,
            "error": "No error"
        }

    except Exception as e:

        # Calculate processing duration before failure
        duration_seconds = round(
            time.perf_counter() - start_time,
            2
        )

        # Return failed ingestion result
        return {
            "relative_path": relative_path,
            "bronze_table": bronze_table_name,
            "raw_row_count": None,
            "bronze_row_count": None,
            "row_count_status": "FAIL",
            "schema_status": "FAIL",
            "overall_status": "FAIL",
            "duration_seconds": duration_seconds,
            "error": str(e)
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test the Bronze ingestion function with one RAW source
test_result = ingest_bronze_file(metadata_list[0])

# Display the returned result
print(test_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ 8.3 Fault-Tolerant Bronze Ingestion Function
# 
# The reusable Bronze ingestion function was enhanced with structured
# exception handling using `try / except`.
# 
# The function now performs the complete workflow:
# 
# **Metadata → RAW Read → Row Count → Delta Write → Bronze Read-Back → Reconciliation → Structured Result**
# 
# For each source, the function validates:
# 
# - RAW-to-Bronze row-count reconciliation
# - Schema field-definition reconciliation
# - Overall ingestion status
# 
# If an exception occurs, the failure is captured and returned as a structured
# result instead of terminating the complete ingestion process.
# 
# ### 🧪 Single-Source Validation
# 
# **Source:** `crm/customer_consent_history.csv`  
# **Target:** `bronze.crm_customer_consent_history`
# 
# | Check | Result |
# |---|---:|
# | RAW Rows | 1,200,051 |
# | Bronze Rows | 1,200,051 |
# | Row Count | ✅ PASS |
# | Schema | ✅ PASS |
# | Overall Status | ✅ PASS |
# | Error | No error |
# 
# The enhanced function successfully passed the single-source validation.

# MARKDOWN ********************

# > 💡 **Engineering Note**
# >
# > Exception handling is implemented at the individual source level.
# > Therefore, a failure in one source can be recorded without automatically
# > preventing subsequent sources from being processed.
# >
# > This design also preserves the actual exception message for later
# > troubleshooting and audit analysis.

# CELL ********************

from tqdm.auto import tqdm

print("tqdm imported successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🚀 8.4 Full Bronze Scale-Out Execution
# 
# The single-source ingestion pattern has been successfully validated.
# 
# The framework is now ready to scale from one tested source to the complete
# RAW dataset of **72 CSV files**.
# 
# Each source will independently pass through:
# 
# **RAW Read → Row Count → Delta Write → Bronze Read-Back → Row Reconciliation → Schema Reconciliation**
# 
# Execution results will be collected for every source, including:
# 
# - RAW row count
# - Bronze row count
# - Row-count status
# - Schema status
# - Overall status
# - Processing duration
# - Error message
# 
# A progress bar will provide real-time visibility during the full Bronze load.

# CELL ********************

# Store Bronze ingestion results
bronze_results = []

# Start total pipeline timer
pipeline_start_time = time.perf_counter()

# Process every RAW source with progress tracking
for item in tqdm(
    metadata_list,
    total=len(metadata_list),
    desc="Bronze ingestion"
):

    # Ingest and validate one source
    result = ingest_bronze_file(item)

    # Store the structured result
    bronze_results.append(result)

# Calculate total pipeline runtime
pipeline_duration_seconds = round(
    time.perf_counter() - pipeline_start_time,
    2
)

print(f"Files processed : {len(bronze_results)}")
print(f"Pipeline runtime: {pipeline_duration_seconds} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📊 8.5 Full Bronze Ingestion Results
# 
# The full Bronze scale-out execution completed across all **72 RAW sources**.
# 
# **Execution Runtime:** 451.24 seconds (~7.52 minutes)
# 
# Completion of the execution loop confirms that every source was attempted.
# However, execution completion alone does not prove ingestion quality.
# 
# The collected `bronze_results` will now be analyzed to determine:
# 
# - Successful and failed source ingestions
# - RAW-to-Bronze row-count reconciliation
# - Schema reconciliation
# - Overall ingestion status
# - Processing-time distribution
# - Failed-source error details
# 
# > 💡 **Engineering Note**
# >
# > **72/72 processed** means that every source was attempted by the framework.
# > It does not automatically mean that all 72 sources passed the Bronze
# > ingestion quality gates.

# CELL ********************

# Convert Bronze ingestion results into a Spark DataFrame
bronze_results_df = spark.createDataFrame(bronze_results)

# Display all ingestion results
display(bronze_results_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summarize Bronze ingestion results by overall status
bronze_status_summary = (
    bronze_results_df
        .groupBy("overall_status")
        .count()
)

display(bronze_status_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🧪 8.6 Global RAW-to-Bronze Reconciliation
# 
# All 72 source-level ingestion results returned `PASS`.
# 
# The next validation aggregates row counts across the complete dataset to verify
# that the total number of records read from RAW matches the total number of
# records persisted in Bronze.
# 
# This provides a dataset-level reconciliation in addition to the individual
# source-level checks.

# CELL ********************

from pyspark.sql import functions as F

# Calculate global RAW-to-Bronze row-count reconciliation
global_reconciliation = (
    bronze_results_df
        .agg(
            F.count("*").alias("sources_processed"),
            F.sum("raw_row_count").alias("total_raw_rows"),
            F.sum("bronze_row_count").alias("total_bronze_rows"),
            F.sum("duration_seconds").alias("sum_source_duration_seconds")
        )
        .withColumn(
            "row_difference",
            F.col("total_bronze_rows") - F.col("total_raw_rows")
        )
)

display(global_reconciliation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ✅ Global Reconciliation Result
# 
# The complete Bronze ingestion successfully reconciled across all 72 RAW sources.
# 
# | Metric | Result |
# |---|---:|
# | Sources Processed | 72 |
# | Total RAW Rows | 60,234,021 |
# | Total Bronze Rows | 60,234,021 |
# | Row Difference | **0** |
# | Source Processing Time | 451.17 sec |
# | Pipeline Runtime | 451.24 sec |
# 
# **Result: PASS**
# 
# All **60,234,021 RAW records** were successfully persisted into the Bronze
# layer with zero aggregate row-count difference.
# 
# > 💡 **Engineering Note**
# >
# > This validation complements the source-level reconciliation.
# > Each individual source passed its own row-count and schema checks, while
# > the global reconciliation confirms consistency across the complete dataset.

# CELL ********************

# Retrieve all registered tables from the Bronze schema
bronze_tables_df = spark.sql("SHOW TABLES IN bronze")

# Keep only permanent tables
bronze_tables_df = (
    bronze_tables_df
        .filter("isTemporary = false")
)

# Count registered Bronze tables
bronze_table_count = bronze_tables_df.count()

print(f"Registered Bronze tables: {bronze_table_count}")

# Validate expected Bronze table count
if bronze_table_count == 72:
    print("PASS: All 72 Bronze Delta tables are registered.")
else:
    print(
        f"FAIL: Expected 72 Bronze tables, "
        f"but found {bronze_table_count}."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🏆 8.7 Bronze Ingestion Gate — PASS
# 
# The complete RAW-to-Bronze ingestion framework has successfully passed
# all ingestion-level validation gates.
# 
# | Validation | Result |
# |---|---:|
# | RAW CSV Sources | 72 |
# | Sources Attempted | 72 |
# | Successful Ingestions | 72 |
# | Failed Ingestions | 0 |
# | Total RAW Rows | 60,234,021 |
# | Total Bronze Rows | 60,234,021 |
# | Aggregate Row Difference | 0 |
# | Row-Count Reconciliation | ✅ PASS |
# | Schema Reconciliation | ✅ PASS |
# | Registered Bronze Tables | 72 |
# | Pipeline Runtime | 451.24 sec |
# | Bronze Ingestion Gate | 🥉 **PASS** |
# 
# ### Architecture Checkpoint
# 
# **72 RAW CSV files → 72 source-aligned Bronze Delta tables**
# 
# The Bronze layer now preserves the complete finalized RAW dataset in
# Delta format while maintaining source-level lineage and structural fidelity.
# 
# > 💡 **Engineering Note**
# >
# > Passing the Bronze Ingestion Gate confirms successful and structurally
# > consistent persistence from RAW into Bronze.
# >
# > It does **not** yet certify business-level data quality.
# > Content-level quality rules, referential integrity, duplicate analysis,
# > null analysis, domain rules, and cross-domain reconciliation are separate
# > validation responsibilities.


# MARKDOWN ********************

# # 🛡️ 9. Bronze Data Quality Gate
# 
# ## 🎯 Objective
# 
# The Bronze Ingestion Gate confirmed that all finalized RAW sources were
# successfully persisted into Microsoft Fabric as source-aligned Delta tables.
# 
# The next stage evaluates the **quality of the data stored inside Bronze**
# before any source is allowed to progress toward the Silver layer.
# 
# ### Ingestion Quality vs Data Quality
# 
# **Ingestion Quality** answers:
# 
# > Did the data arrive correctly?
# 
# This has already been validated through:
# 
# - 72 / 72 successful source ingestions
# - 60,234,021 RAW rows reconciled with Bronze
# - Zero aggregate row-count difference
# - Matching RAW and Bronze schema definitions
# - 72 registered Bronze Delta tables
# 
# **Data Quality** answers:
# 
# > Is the data itself structurally and logically trustworthy?
# 
# The Bronze Data Quality Gate will progressively evaluate:
# 
# **Completeness → Uniqueness → Key Integrity → Referential Integrity → Domain Rules → Cross-Domain Reconciliation**
# 
# The objective is not to transform or clean Bronze data.
# 
# Bronze remains the source-aligned historical layer.
# 
# Detected quality issues will be measured, documented, and handled deliberately
# during downstream Silver processing when appropriate.


# CELL ********************

# Select one Bronze table for the first DQ profiling exercise
dq_table_name = "bronze.crm_customer_consent_history"

# Read the Bronze Delta table
dq_df = spark.table(dq_table_name)

print(f"DQ table: {dq_table_name}")
print(f"Rows    : {dq_df.count()}")
print(f"Columns : {len(dq_df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
# Count NULL values in customer_id
dq_df.select(
    F.sum(
        F.when(
            F.col("customer_id").isNull(),
            1
        ).otherwise(0)
    ).alias("customer_id_null_count")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(dq_df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build NULL-count expressions dynamically for every column
null_count_expressions = [
    F.sum(
        F.when(
            F.col(column).isNull(),
            1
        ).otherwise(0)
    ).alias(f"{column}_null_count")

    for column in dq_df.columns
]

# Calculate NULL counts for all columns
null_profile = dq_df.select(*null_count_expressions)

display(null_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get total number of rows
total_rows = dq_df.count()

# Build NULL-percentage expressions for every column
null_percentage_expressions = [
    (
        F.sum(
            F.when(
                F.col(column).isNull(),
                1
            ).otherwise(0)
        )
        / total_rows
        * 100
    ).alias(f"{column}_null_percentage")

    for column in dq_df.columns
]

# Calculate NULL percentages
null_percentage_profile = dq_df.select(
    *null_percentage_expressions
)

display(null_percentage_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Store column-level DQ profiling results
dq_profile_rows = []

# Get total table row count
total_rows = dq_df.count()

# Profile every column
for column in dq_df.columns:

    # Calculate NULL count for the current column
    null_count = (
        dq_df
            .filter(F.col(column).isNull())
            .count()
    )

    # Calculate NULL percentage
    null_percentage = (
        null_count / total_rows * 100
        if total_rows > 0
        else 0.0
    )

    # Store the column-level profile
    dq_profile_rows.append(
        {
            "table_name": dq_table_name,
            "column_name": column,
            "row_count": total_rows,
            "null_count": null_count,
            "null_percentage": round(null_percentage, 4)
        }
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(dq_profile_rows[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(len(dq_profile_rows))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dq_profile_df = spark.createDataFrame(dq_profile_rows)

display(dq_profile_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time


def profile_bronze_completeness(table_name):

    # Start table profiling timer
    start_time = time.perf_counter()

    try:

        # Read the Bronze Delta table
        df = spark.table(table_name)

        # Count total table rows
        total_rows = df.count()

        # Build NULL-count expressions for all columns
        null_count_expressions = [
            F.sum(
                F.when(
                    F.col(column).isNull(),
                    1
                ).otherwise(0)
            ).alias(column)

            for column in df.columns
        ]

        # Calculate NULL counts for all columns in one aggregation
        null_counts_row = (
            df
                .select(*null_count_expressions)
                .first()
        )

        # Store column-level completeness results
        profile_rows = []

        # Build the long-format DQ profile
        for column in df.columns:

            # Get pre-calculated NULL count
            null_count = null_counts_row[column]

            # Calculate NULL percentage
            null_percentage = (
                null_count / total_rows * 100
                if total_rows > 0
                else 0.0
            )

            # Store column-level result
            profile_rows.append(
                {
                    "table_name": table_name,
                    "column_name": column,
                    "row_count": total_rows,
                    "null_count": null_count,
                    "null_percentage": round(null_percentage, 4)
                }
            )

        # Calculate profiling duration
        duration_seconds = round(
            time.perf_counter() - start_time,
            2
        )

        # Store table-level execution result
        table_result = {
            "table_name": table_name,
            "row_count": total_rows,
            "column_count": len(df.columns),
            "status": "PASS",
            "duration_seconds": duration_seconds,
            "error": "No error"
        }

        return profile_rows, table_result

    except Exception as e:

        # Calculate duration before failure
        duration_seconds = round(
            time.perf_counter() - start_time,
            2
        )

        # No valid column profile is available
        profile_rows = []

        # Store failed table-level execution result
        table_result = {
            "table_name": table_name,
            "row_count": None,
            "column_count": None,
            "status": "FAIL",
            "duration_seconds": duration_seconds,
            "error": str(e)
        }

        return profile_rows, table_result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_profile_rows, test_table_result = profile_bronze_completeness(
    "bronze.crm_customer_consent_history"
)

print(test_table_result)
print(f"Profile rows: {len(test_profile_rows)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_tables_df = spark.sql("SHOW TABLES IN bronze")

display(bronze_tables_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2 — Collect table names from Spark to Python
table_rows = (
    bronze_tables_df
        .select("tableName")
        .collect()
)

print(type(table_rows))
print(len(table_rows))
print(table_rows[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3 — Build fully-qualified Bronze table names
bronze_table_names = [
    f"bronze.{row['tableName']}"
    for row in table_rows
]

print(type(bronze_table_names))
print(len(bronze_table_names))
print(bronze_table_names[0])
print(bronze_table_names[-1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from tqdm import tqdm
# Store column-level completeness profiles
all_completeness_rows = []

# Store table-level profiling execution results
completeness_table_results = []

# Start total profiling timer
completeness_start_time = time.perf_counter()

# Profile all Bronze tables
for table_name in tqdm(
    bronze_table_names,
    total=len(bronze_table_names),
    desc="Bronze completeness profiling"
):

    # Profile one Bronze table
    profile_rows, table_result = profile_bronze_completeness(
        table_name
    )

    # Add column-level results
    all_completeness_rows.extend(profile_rows)

    # Add one table-level execution result
    completeness_table_results.append(table_result)

# Calculate total pipeline duration
completeness_duration_seconds = round(
    time.perf_counter() - completeness_start_time,
    2
)

print(f"Tables processed     : {len(completeness_table_results)}")
print(f"Column profiles      : {len(all_completeness_rows)}")
print(f"Profiling runtime    : {completeness_duration_seconds} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Completed table results : {len(completeness_table_results)}")
print(f"Column profile rows     : {len(all_completeness_rows)}")

if completeness_table_results:
    print("\nLast completed table:")
    print(completeness_table_results[-1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

completeness_results_df = spark.createDataFrame(
    completeness_table_results
)

display(completeness_results_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

status_summary_df = (
    completeness_results_df
        .groupBy("status")
        .count()
)

display(status_summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

completeness_profile_df = spark.createDataFrame(
    all_completeness_rows
)

print(f"Column profiles: {completeness_profile_df.count()}")

display(completeness_profile_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_with_nulls_df = (
    completeness_profile_df
        .filter(F.col("null_count") > 0)
)

print(f"Columns containing NULLs: {columns_with_nulls_df.count()}")

display(columns_with_nulls_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

null_analysis_df = (
    columns_with_nulls_df
        .orderBy(
            F.col("null_percentage").desc()
        )
)

display(null_analysis_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employees_df = spark.table("bronze.erp_employees")

employees_df.printSchema()

print("\nColumns:")
print(employees_df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Load the Bronze employees table
employees_df = spark.table("bronze.erp_employees")

# Detect invalid employee date sequences
invalid_employee_dates_df = (
    employees_df
        .filter(
            (F.col("hire_date") < F.col("birth_date"))
            |
            (F.col("termination_date") < F.col("hire_date"))
        )
)

# Count invalid records
invalid_employee_dates_count = invalid_employee_dates_df.count()

print(
    f"Invalid employee date records: "
    f"{invalid_employee_dates_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Count each temporal violation separately

hired_before_birth_count = (
    employees_df
        .filter(
            F.col("hire_date") < F.col("birth_date")
        )
        .count()
)

terminated_before_hire_count = (
    employees_df
        .filter(
            F.col("termination_date") < F.col("hire_date")
        )
        .count()
)

print(
    f"Hired before birth      : {hired_before_birth_count}"
)

print(
    f"Terminated before hire  : {terminated_before_hire_count}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employee_temporal_rule = {
    "rule_id": "EMP_TEMP_001",
    "table_name": "bronze.erp_employees",
    "rule_name": "Hired Before Birth",
    "rule_type": "TEMPORAL_VALIDITY",
    "violation_count": hired_before_birth_count,
    "status": "PASS" if hired_before_birth_count == 0 else "FAIL"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(employee_temporal_rule)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build Bronze schema inventory

bronze_schema_rows = []

for table_name in bronze_table_names:

    df = spark.table(table_name)

    for field in df.schema.fields:

        bronze_schema_rows.append(
            {
                "table_name": table_name,
                "column_name": field.name,
                "data_type": str(field.dataType),
                "nullable": field.nullable
            }
        )

bronze_schema_df = spark.createDataFrame(bronze_schema_rows)

print(f"Tables  : {len(bronze_table_names)}")
print(f"Columns : {bronze_schema_df.count()}")

display(bronze_schema_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
