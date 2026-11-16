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
# CELL 1 — Install the Event Hubs client (session only)
# =========================================================
%pip install azure-eventhub --quiet


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# =========================================================
# CELL 2 — Settings
# Default lakehouse: lh_silver. The connection string is NOT in this notebook:
# it lives in lh_silver -> Files/secrets/es_pharmacy_sales.txt (Files are not in Git).
# =========================================================
import json, random, time, uuid
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

SECRET_FILE = "/lakehouse/default/Files/secrets/es_pharmacy_sales.txt"
with open(SECRET_FILE) as f:
    CONN_STR = f.read().strip()          # Event hub-compatible connection string (includes EntityPath)

RUN_MINUTES       = 10                   # how long the simulator runs
BATCH_EVERY_SEC   = 5                    # one batch of receipts every 5 seconds
SILENT_BRANCH     = "PH0412"             # this branch stops selling during the run (simulated outage)
SILENT_AFTER_MIN  = 3                    # ... after 3 minutes

# 18 real branches from the warehouse (dim_pharmacy), at least one per region
BRANCHES = [
    ("PH0073", "Riyadh"), ("PH0169", "Riyadh"), ("PH0172", "Riyadh"),
    ("PH0261", "Makkah"), ("PH0378", "Makkah"), ("PH0412", "Makkah"),
    ("PH0475", "Madinah"), ("PH0567", "Qassim"),
    ("PH0680", "Eastern Province"), ("PH0681", "Eastern Province"),
    ("PH0770", "Asir"), ("PH0822", "Tabuk"), ("PH0852", "Hail"),
    ("PH0878", "Northern Borders"), ("PH0910", "Jazan"), ("PH0954", "Najran"),
    ("PH0962", "Al Bahah"), ("PH0979", "Al Jawf"),
]

# Must match the KQL ingestion mapping live_sales_json exactly
EXPECTED_FIELDS = {"event_time", "receipt_id", "pharmacy_id", "region_name",
                   "items", "amount_sar", "payment_type"}
print("Secret loaded:", len(CONN_STR) > 0, "| branches:", len(BRANCHES))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# =========================================================
# CELL 3 — Build one receipt and check its shape before sending
# =========================================================
def make_receipt(pharmacy_id, region):
    items = random.choice([1, 1, 2, 2, 2, 3, 3, 4, 5])
    amount = round(random.lognormvariate(5.6, 0.55), 2)          # median about 270 SAR, long right tail
    payment = random.choices(["cash", "card", "insurance"], weights=[25, 50, 25])[0]
    return {
        "event_time":   datetime.now(timezone.utc).isoformat(),
        "receipt_id":   f"LV-{uuid.uuid4().hex[:12].upper()}",
        "pharmacy_id":  pharmacy_id,
        "region_name":  region,
        "items":        items,
        "amount_sar":   amount,
        "payment_type": payment,
    }

sample = make_receipt(*BRANCHES[0])
missing, extra = EXPECTED_FIELDS - sample.keys(), sample.keys() - EXPECTED_FIELDS
if missing or extra:
    raise RuntimeError(f"Event shape does not match live_sales_json: missing={missing}, extra={extra}")
print(json.dumps(sample, indent=2))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# =========================================================
# CELL 4 — Run the simulator
# =========================================================
producer = EventHubProducerClient.from_connection_string(CONN_STR)
start, sent, batches = time.time(), 0, 0
try:
    while time.time() - start < RUN_MINUTES * 60:
        minutes = (time.time() - start) / 60
        active = [b for b in BRANCHES if not (b[0] == SILENT_BRANCH and minutes >= SILENT_AFTER_MIN)]
        batch = producer.create_batch()
        n = 0
        for pharmacy_id, region in active:
            if random.random() < 0.6:                    # not every branch sells every 5 seconds
                batch.add(EventData(json.dumps(make_receipt(pharmacy_id, region))))
                n += 1
        if n:
            producer.send_batch(batch)
            sent += n
        batches += 1
        if batches % 12 == 0:                            # about once a minute
            print(f"{minutes:5.1f} min | sent {sent:,} receipts | active branches {len(active)}")
        time.sleep(BATCH_EVERY_SEC)
finally:
    producer.close()
print(f"DONE: {sent:,} receipts in {batches} batches; {SILENT_BRANCH} silent after minute {SILENT_AFTER_MIN}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
