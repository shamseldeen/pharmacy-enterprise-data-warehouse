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
    # silver name                        bronze name                          primary key
    "erp_employee_assignments":        ("erp_employee_assignments",         ["assignment_id"]),
    "erp_employee_shift_rosters":      ("erp_employee_shift_rosters",       ["roster_id"]),
    "erp_employee_monthly_timekeeping":("erp_employee_monthly_timekeeping", ["timekeeping_id"]),
    "erp_pharmacy_operating_hours":    ("erp_pharmacy_operating_hours",     ["pharmacy_id", "day_of_week"]),
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
    print(f"{silver_name:36s} {n:>8,}")
print(f"{'TOTAL':36s} {total:>8,}")            # expected: 559,920

asg = spark.read.table(f"{BRONZE}.erp_employee_assignments")
oh  = spark.read.table(f"{BRONZE}.erp_pharmacy_operating_hours")
tk  = spark.read.table(f"{BRONZE}.erp_employee_monthly_timekeeping")

print("\nFlags:")
asg.groupBy("is_primary_assignment").count().show()
oh.groupBy("is_24h").count().show()

print("What do open/close times really contain? (full value, as text)")
(oh.select(F.date_format("open_time",  "yyyy-MM-dd HH:mm:ss").alias("open_time"),
           F.date_format("close_time", "yyyy-MM-dd HH:mm:ss").alias("close_time"), "is_24h")
   .distinct().orderBy("open_time").show(20, truncate=False))

print("Is (pharmacy, day) alone unique, or do we need the start date too?")
print("  rows                         :", oh.count())
print("  distinct pharmacy+day        :", oh.select("pharmacy_id", "day_of_week").distinct().count())
print("  distinct pharmacy+day+start  :", oh.select("pharmacy_id", "day_of_week", "effective_start_date").distinct().count())
print("  day_of_week values           :", sorted(r[0] for r in oh.select("day_of_week").distinct().collect()))

print("\nTimekeeping: a sample to guess the hours formula")
tk.select("scheduled_hours", "worked_hours", "overtime_hours",
          "absence_hours", "sick_leave_hours", "late_minutes").show(8)

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
        # 0/1 flags already named is_* -> boolean, same name
        elif t == "int" and c.startswith("is_"):
            df = df.withColumn(c, F.col(c) == 1)
    return df


def minutes_of_day(col):
    # Keep only the time part: hours*60 + minutes (the date part is meaningless)
    return F.hour(col) * 60 + F.minute(col)


def table_specific(silver_name, df):
    if silver_name == "erp_pharmacy_operating_hours":
        open_m  = minutes_of_day(F.col("open_time"))
        close_m = minutes_of_day(F.col("close_time"))
        df = (df
            .withColumn("open_time_hhmm",  F.date_format("open_time",  "HH:mm"))
            .withColumn("close_time_hhmm", F.date_format("close_time", "HH:mm"))
            .withColumn("closes_after_midnight", ~F.col("is_24h") & (close_m <= open_m))
            .withColumn("operating_minutes",
                F.when(F.col("is_24h"), F.lit(1440))                   # 24h stores 00:00-23:59
                 .when(close_m <= open_m, close_m + 1440 - open_m)     # crosses midnight
                 .otherwise(close_m - open_m))
            .drop("open_time", "close_time"))                          # drop the fake dates
    return df


def load(silver_name):
    bronze_name, pk = TABLES[silver_name]
    src = f"{BRONZE}.{bronze_name}"
    df = table_specific(silver_name, standardize(spark.read.table(src)))
    df = (df.withColumn("_silver_loaded_at", F.current_timestamp())
            .withColumn("_source_table", F.lit(src)))
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{silver_name}"))
    print(f"OK  {silver_name}")


for name in TABLES:
    load(name)

# Check the new time columns
(spark.read.table(f"{SILVER}.erp_pharmacy_operating_hours")
      .select("open_time_hhmm", "close_time_hhmm", "is_24h",
              "closes_after_midnight", "operating_minutes")
      .distinct().orderBy("open_time_hhmm").show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 4 — Validate: rows + duplicate keys
# =========================================================
rows = []
for silver_name, (bronze_name, pk) in TABLES.items():
    b = spark.read.table(f"{BRONZE}.{bronze_name}")
    s = spark.read.table(f"{SILVER}.{silver_name}")
    rows.append((silver_name, ", ".join(pk), b.count(), s.count(),
                 s.groupBy(*pk).count().filter("count > 1").count()))

display(spark.createDataFrame(rows,
    "table string, key_columns string, bronze_rows long, silver_rows long, duplicate_keys long"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# CELL 5 — ERP business rules
# =========================================================
asg   = spark.read.table(f"{SILVER}.erp_employee_assignments")
ros   = spark.read.table(f"{SILVER}.erp_employee_shift_rosters")
tk    = spark.read.table(f"{SILVER}.erp_employee_monthly_timekeeping")
oh    = spark.read.table(f"{SILVER}.erp_pharmacy_operating_hours")
emp   = spark.read.table(f"{SILVER}.erp_employees")
pharm = spark.read.table(f"{SILVER}.erp_pharmacies")

# Overlapping assignments for the same employee (all are "primary", so overlap = conflict)
w = Window.partitionBy("employee_id").orderBy("assignment_start_date", "assignment_id")
asg_overlaps = (asg.withColumn("prev_end", F.lag("assignment_end_date").over(w))
                   .withColumn("prev_exists", F.lag("assignment_id").over(w).isNotNull())
                   .filter(F.col("prev_exists") &
                           (F.col("prev_end").isNull() |
                            (F.col("prev_end") >= F.col("assignment_start_date"))))
                   .count())

# Roster must point to the same pharmacy as its assignment
ros_vs_asg = (ros.join(asg.select("assignment_id", F.col("pharmacy_id").alias("asg_pharmacy")),
                       "assignment_id", "inner")
                 .filter(F.col("pharmacy_id") != F.col("asg_pharmacy")).count())

checks = {
    "timekeeping: worked <> scheduled + overtime - absence - sick":
        tk.filter(F.col("worked_hours") !=
                  F.col("scheduled_hours") + F.col("overtime_hours")
                  - F.col("absence_hours") - F.col("sick_leave_hours")).count(),
    "timekeeping: negative hours or minutes":
        tk.filter((F.col("worked_hours") < 0) | (F.col("overtime_hours") < 0) |
                  (F.col("absence_hours") < 0) | (F.col("sick_leave_hours") < 0) |
                  (F.col("late_minutes") < 0)).count(),
    "assignments: end before start":
        asg.filter(F.col("assignment_end_date") < F.col("assignment_start_date")).count(),
    "assignments: overlapping per employee":
        asg_overlaps,
    "rosters: pharmacy differs from its assignment":
        ros_vs_asg,
    "operating hours: pharmacies without all 7 days":
        oh.groupBy("pharmacy_id").count().filter("count <> 7").count(),
    "assignments with unknown employee (orphans)":
        asg.join(emp.select("employee_id"), "employee_id", "left_anti").count(),
    "assignments with unknown pharmacy (orphans)":
        asg.join(pharm.select("pharmacy_id"), "pharmacy_id", "left_anti").count(),
    "rosters with unknown assignment (orphans)":
        ros.join(asg.select("assignment_id"), "assignment_id", "left_anti").count(),
    "timekeeping with unknown assignment (orphans)":
        tk.join(asg.select("assignment_id"), "assignment_id", "left_anti").count(),
    "operating hours with unknown pharmacy (orphans)":
        oh.join(pharm.select("pharmacy_id"), "pharmacy_id", "left_anti").count(),
    "pharmacies with no operating hours":
        pharm.join(oh.select("pharmacy_id").distinct(), "pharmacy_id", "left_anti").count(),
}

for name, n in checks.items():
    status = ("INFO" if "[info]" in name else "PASS" if n == 0 else "CHECK")
    print(f"{status:5s}  {name:58s} {n:>8,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
