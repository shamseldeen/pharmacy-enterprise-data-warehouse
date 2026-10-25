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

# ============================================================
# CELL 01 — EXECUTION PARAMETERS
# ============================================================
# Mark this cell as the notebook parameter cell.
# Pipeline parameters can override these interactive defaults.

workspace_name = "Pharmacy-Enterprise-DW-DEV"
metadata_lakehouse = "lh_silver"
entity_id = "reference_regions"
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 02 — LOAD THE DECLARED ENTITY CONTRACT
# ============================================================
# Read configuration only; no Bronze or Silver business-data scan.

from pyspark.sql import functions as F


def qualified_name(*parts):
    return ".".join(
        "`" + part.replace("`", "``") + "`"
        for part in parts
    )


def load_entity_contract(workspace, metadata_lakehouse, selected_entity):
    root = qualified_name(workspace, metadata_lakehouse, "metadata")

    entity_rows = (
        spark.table(f"{root}.`silver_entity_config`")
            .filter(F.col("entity_id") == selected_entity)
            .limit(2)
            .collect()
    )

    if len(entity_rows) != 1:
        raise ValueError("Expected exactly one entity configuration row.")

    entity = entity_rows[0].asDict()

    # Existing verified targets follow a protected reuse path.
    if entity["baseline_status"] == "EXISTING_VERIFIED":
        if entity["load_policy"] != "REUSE_EXISTING":
            raise ValueError("Existing baseline must use REUSE_EXISTING.")

        return {
            "entity": entity,
            "sources": [],
            "columns": [],
            "rules": [],
            "action": "SKIP_EXISTING"
        }

    if entity["config_status"] != "APPROVED" or not entity["is_enabled"]:
        raise ValueError("Entity contract is not approved and enabled.")

    if entity["load_policy"] != "CREATE_ONLY":
        raise ValueError("This executor version requires CREATE_ONLY.")

    if (
        not entity["grain_description"]
        or not entity["transformation_pattern"]
        or entity["dependency_group"] < 1
    ):
        raise ValueError(
            "Grain, transformation or dependency configuration is incomplete."
        )

    version = entity["contract_version"]

    def read_enabled(table_name, order_column):
        return [
            row.asDict()
            for row in (
                spark.table(f"{root}.`{table_name}`")
                    .filter(
                        (F.col("entity_id") == selected_entity)
                        & (F.col("contract_version") == version)
                        & F.col("is_enabled")
                    )
                    .orderBy(order_column)
                    .collect()
            )
        ]

    sources = read_enabled("silver_source_config", "source_order")
    columns = read_enabled("silver_column_config", "column_order")
    rules = read_enabled("silver_rule_config", "rule_id")

    for rows, key in (
        (sources, "source_id"),
        (columns, "column_config_id"),
        (rules, "rule_id")
    ):
        if not rows:
            raise ValueError(f"Missing enabled configuration: {key}")

        if len(rows) != len({row[key] for row in rows}):
            raise ValueError(f"Duplicate configuration IDs: {key}")

    source_ids = {row["source_id"] for row in sources}

    if any(row["source_id"] not in source_ids for row in columns):
        raise ValueError("Column mapping references an unavailable source.")

    return {
        "entity": entity,
        "sources": sources,
        "columns": columns,
        "rules": rules,
        "action": "PROCESS"
    }


# Preview the selected contract using the parameter-cell defaults.
contract = load_entity_contract(
    workspace_name,
    metadata_lakehouse,
    entity_id
)

print("Entity:", contract["entity"]["entity_id"])
print("Pattern:", contract["entity"]["transformation_pattern"])
print("Load policy:", contract["entity"]["load_policy"])
print("Sources:", len(contract["sources"]))
print("Column mappings:", len(contract["columns"]))
print("Rules:", len(contract["rules"]))
print(
    "STATUS:",
    "CONTRACT_LOADED" if contract["action"] == "PROCESS" else "SKIP_EXISTING"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
