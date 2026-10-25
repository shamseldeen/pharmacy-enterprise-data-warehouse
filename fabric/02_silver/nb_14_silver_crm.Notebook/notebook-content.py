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
    # silver name                          bronze name                            primary key
    "crm_customers":                     ("crm_customers",                      ["customer_id"]),
    "crm_loyalty_accounts":              ("crm_loyalty_accounts",               ["loyalty_account_id"]),
    "crm_customer_consent_history":      ("crm_customer_consent_history",       ["consent_event_id"]),
    "crm_customer_service_interactions": ("crm_customer_service_interactions",  ["interaction_key"]),
}
print(f"{len(TABLES)} tables configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 2 — Explore
# =========================================================
total = 0
for silver_name, (bronze_name, pk) in TABLES.items():
    n = spark.read.table(f"{BRONZE}.{bronze_name}").count()
    total += n
    print(f"{silver_name:36s} {n:>10,}")
print(f"{'TOTAL':36s} {total:>10,}")          # expected: 4,779,722

cust = spark.read.table(f"{BRONZE}.crm_customers")
cons = spark.read.table(f"{BRONZE}.crm_customer_consent_history")
inter = spark.read.table(f"{BRONZE}.crm_customer_service_interactions")

print("\nloyalty_enrolled values:")
cust.groupBy("loyalty_enrolled").count().show()

print("Consent: what types and actions exist?")
cons.groupBy("consent_type", "consent_action").count().orderBy("consent_type", "consent_action").show(30)

print("Interactions: satisfaction score range + open (not closed) cases:")
inter.agg(F.min("satisfaction_score"), F.max("satisfaction_score"),
          F.sum(F.col("satisfaction_score").isNull().cast("int")).alias("score_nulls"),
          F.sum(F.col("closed_timestamp").isNull().cast("int")).alias("not_closed")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 3 — Standardize, write, and derive CURRENT consent
# =========================================================
def standardize(df):
    for c, t in df.dtypes:
        if t == "string":
            trimmed = F.trim(F.col(c))
            df = df.withColumn(c, F.when(trimmed == "", None).otherwise(trimmed))
    if "loyalty_enrolled" in df.columns:
        df = (df.withColumn("is_loyalty_enrolled", F.col("loyalty_enrolled") == 1)
                .drop("loyalty_enrolled"))
    return df


def table_specific(silver_name, df):
    if silver_name == "crm_customer_service_interactions":
        # The source REUSES interaction_id for unrelated cases (key collision).
        # Build a unique surrogate key from columns that identify a real case.
        df = (df.withColumnRenamed("interaction_id", "source_interaction_id")
                .withColumn("interaction_key", F.sha2(F.concat_ws("|",
                    F.col("source_interaction_id"),
                    F.col("customer_id"),
                    F.col("opened_timestamp").cast("string")), 256)))
    return df


def write(df, silver_name, source):
    df = (df.withColumn("_silver_loaded_at", F.current_timestamp())
            .withColumn("_source_table", F.lit(source)))
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{silver_name}"))
    print(f"OK  {silver_name}")


# 1) The 4 tables, cleaned
for silver_name, (bronze_name, pk) in TABLES.items():
    src = f"{BRONZE}.{bronze_name}"
    df = table_specific(silver_name, standardize(spark.read.table(src)))
    write(df, silver_name, src)

# 2) Derived table: the LATEST consent event per customer and type
events = spark.read.table(f"{SILVER}.crm_customer_consent_history")
w = (Window.partitionBy("customer_id", "consent_type")
           .orderBy(F.col("event_timestamp").desc(), F.col("consent_event_id").desc()))
current = (events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("customer_id", "consent_type",
            (F.col("consent_action") == "GRANTED").alias("is_consented"),
            F.col("event_timestamp").alias("consent_updated_at"),
            "channel"))
write(current, "crm_customer_consent_current", f"{SILVER}.crm_customer_consent_history")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: rows + duplicate keys (incl. the derived table)
# =========================================================
rows = []
for silver_name, (bronze_name, pk) in TABLES.items():
    b = spark.read.table(f"{BRONZE}.{bronze_name}")
    s = spark.read.table(f"{SILVER}.{silver_name}")
    rows.append((silver_name, ", ".join(pk), b.count(), s.count(),
                 s.groupBy(*pk).count().filter("count > 1").count()))

# Derived table: expected rows = number of distinct (customer, type) pairs in the history
hist = spark.read.table(f"{SILVER}.crm_customer_consent_history")
cur  = spark.read.table(f"{SILVER}.crm_customer_consent_current")
pk   = ["customer_id", "consent_type"]
rows.append(("crm_customer_consent_current", ", ".join(pk),
             hist.select(*pk).distinct().count(), cur.count(),
             cur.groupBy(*pk).count().filter("count > 1").count()))

display(spark.createDataFrame(rows,
    "table string, key_columns string, expected_rows long, silver_rows long, duplicate_keys long"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 5 — CRM business rules
# =========================================================
cust  = spark.read.table(f"{SILVER}.crm_customers")
loy   = spark.read.table(f"{SILVER}.crm_loyalty_accounts")
inter = spark.read.table(f"{SILVER}.crm_customer_service_interactions")
pharm = spark.read.table(f"{SILVER}.erp_pharmacies")
ref   = spark.read.table(f"{SILVER}.crm_customer_service_reference")

has_account = loy.select("customer_id").distinct().withColumn("has_account", F.lit(True))
c = cust.join(has_account, "customer_id", "left").fillna({"has_account": False})

# First-ever event per customer+type: was it a WITHDRAWN (withdraw before any grant)?
w_first = Window.partitionBy("customer_id", "consent_type").orderBy("event_timestamp", "consent_event_id")
first_withdrawn = (hist.withColumn("rn", F.row_number().over(w_first))
                       .filter((F.col("rn") == 1) & (F.col("consent_action") == "WITHDRAWN")).count())

checks = {
    "loyalty: flag TRUE but no account":
        c.filter(F.col("is_loyalty_enrolled") & ~F.col("has_account")).count(),
    "loyalty: has account but flag FALSE":
        c.filter(~F.col("is_loyalty_enrolled") & F.col("has_account")).count(),
    "loyalty: more than one account per customer":
        loy.groupBy("customer_id").count().filter("count > 1").count(),
    "loyalty: enrolled before customer registered":
        loy.join(cust.select("customer_id", "registration_date"), "customer_id")
           .filter(F.col("enrollment_date") < F.col("registration_date")).count(),
    "loyalty accounts with unknown customer (orphans)":
        loy.join(cust.select("customer_id"), "customer_id", "left_anti").count(),
    "consent: first event is WITHDRAWN (no prior grant)":
        first_withdrawn,
    "consent events with unknown customer (orphans)":
        hist.join(cust.select("customer_id"), "customer_id", "left_anti").count(),
    "consent: customers with NO consent record [info]":
        cust.join(cur.select("customer_id").distinct(), "customer_id", "left_anti").count(),
    "interactions: closed before opened":
        inter.filter(F.col("closed_timestamp") < F.col("opened_timestamp")).count(),
    "interactions with unknown customer (orphans)":
        inter.join(cust.select("customer_id"), "customer_id", "left_anti").count(),
    "interactions with unknown pharmacy (orphans)":
        inter.filter(F.col("pharmacy_id").isNotNull())
             .join(pharm.select("pharmacy_id"), "pharmacy_id", "left_anti").count(),
}

for name, n in checks.items():
    status = ("INFO" if "[info]" in name else "PASS" if n == 0 else "CHECK")
    print(f"{status:5s}  {name:52s} {n:>10,}")

# Open cases (no closed time): what status do they have?
print("\nNot-closed interactions by status:")
(inter.filter(F.col("closed_timestamp").isNull())
      .join(ref.filter("reference_type = 'STATUS'")
               .select(F.col("code").alias("status_id"), F.col("name").alias("status_name")),
            "status_id", "left")
      .groupBy("status_name").count().orderBy(F.desc("count")).show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 6 — Investigate duplicate interaction_id
# =========================================================
inter = spark.read.table(f"{SILVER}.crm_customer_service_interactions")
biz_cols = [c for c in inter.columns if not c.startswith("_")]   # ignore audit columns

print("total rows             :", inter.count())
print("distinct interaction_id:", inter.select("interaction_id").distinct().count())
print("distinct FULL rows     :", inter.select(*biz_cols).distinct().count())

# How many copies does a duplicated id have?
print("\nCopies per id:")
(inter.groupBy("interaction_id").count()
      .groupBy(F.col("count").alias("copies")).count()
      .orderBy("copies").show())

# Look at 2 duplicated ids side by side
dup_ids = (inter.groupBy("interaction_id").count()
                .filter("count > 1").limit(2))
(inter.join(dup_ids.select("interaction_id"), "interaction_id")
      .select(*biz_cols).orderBy("interaction_id").show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table(f"{SILVER}.crm_customer_consent_current").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inter = spark.read.table(f"{SILVER}.crm_customer_service_interactions")
print(inter.select("source_interaction_id", "customer_id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
