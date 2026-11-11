# Naming Conventions

These are the naming rules this project follows, from Fabric items down to column names. New objects should follow the same rules.

**General rules**
- `snake_case`, all lowercase, English only.
- Names say what the thing *is*, not how it was built.
- Numeric prefixes (`01_`, `nb_04_`) show run or reading order.

---

## 1. Fabric items

| Item | Pattern | Example |
|---|---|---|
| Layer folder | `NN_<layer>` | `01_bronze`, `02_silver`, `03_gold`, `04_reporting`, `05_orchestration`, `99_archive` |
| Lakehouse | `lh_<layer>` | `lh_bronze`, `lh_silver` |
| Warehouse | `wh_<layer>` | `wh_gold` |
| Notebook | `nb_NN_<layer>_<domain>[_transform]` | `nb_02_bronze_ingestion_framework`, `nb_11_silver_pos` |
| Semantic model | `sm_<subject>` | `sm_pharmacy_sales` |
| Report | `rpt_<subject>` | `rpt_pharmacy_dashboard` |
| Data pipeline | `pl_<subject>_<frequency>_<action>` | `pl_pharmacy_daily_build` |

A letter suffix on a notebook number (`nb_05b`, `nb_07b`) marks a follow-up step in the same domain. Retired notebooks move to `99_archive`.

## 2. Bronze and Silver tables (Lakehouse)

Pattern: `<source_system>_<entity>`. The source prefix tells you where the data came from.

| Prefix | Source system | Example |
|---|---|---|
| `erp_` | ERP (branches, staff) | `erp_pharmacies`, `erp_employees` |
| `crm_` | CRM (customers, consent) | `crm_customer_consent_history` |
| `rx_` | Prescription system | `rx_prescriptions`, `rx_dispenses`, `rx_doctors` |
| `reference_` | Reference data | `reference_regions` |

Entity names are usually plural (`erp_pharmacies`, `rx_prescriptions`).

## 3. Gold tables (Warehouse schema `gold`)

Pattern: `<type>_<entity>`. Gold entity names are singular, because a row describes one thing.

| Prefix | Meaning | Example |
|---|---|---|
| `dim_` | Dimension: who, what, where, when | `dim_pharmacy`, `dim_product`, `dim_date` |
| `fact_` | Fact: an event or a measurement | `fact_sales_line`, `fact_claim` |
| `agg_` | Pre-aggregated table built from facts | `agg_product_loss_summary` |
| `ref_` | Reference or lookup data | `ref_calendar_events`, `ref_data_checks` |

Suffixes on fact and aggregate names state the grain or timing:
- `_monthly`: one row per month (`agg_branch_scorecard_monthly`)
- `_asof`: a snapshot at one date (`agg_stock_aging_asof`)
- `_snapshot`: a periodic snapshot (`fact_inventory_snapshot`)

## 4. Stored procedures

Pattern: `usp_build_<table_name>`, one procedure per Gold table (`usp_build_dim_pharmacy`). `usp_build_all` runs them in dependency order: calendar, dimensions, facts, aggregates.

## 5. Columns

| Pattern | Meaning | Example |
|---|---|---|
| `<entity>_key` | Surrogate key (INT) in Gold, used for joins | `pharmacy_key`, `product_key` |
| `<entity>_id` | Natural / business id from the source system | `pharmacy_id`, `transaction_id` |
| `<role>_<entity>_key` | A second key to the same dimension, named by its role | `dispensing_employee_key`, `buyer_employee_key` |
| `date_key`, `<event>_date_key` | Integer key to `dim_date` | `order_date_key`, `return_date_key` |
| `_date` | Calendar date | `opening_date`, `receipt_date` |
| `_ts` | Timestamp (date and time) | `transaction_ts`, `dispense_ts` |
| `_sar` | Money amount in Saudi riyals | `net_amount_sar`, `loss_sar` |
| `_qty`, `quantity` | Unit counts | `closing_stock`, `loss_qty` |
| `_pct` | Percentage | `margin_pct`, `loss_lines_pct` |
| `_days`, `_hours`, `_minutes` | Duration, unit in the name | `lead_time_days`, `minutes_to_dispense` |
| `is_`, `has_`, `can_` | Boolean flag (BIT) | `is_sold_below_cost`, `has_return`, `can_dispense` |
| `pr_` | Percentile rank within a peer group | `pr_sales_per_hour` |
| `est_` | Estimated value, not recorded in the source | `est_cost_sar`, `est_margin_sar` |
| `_ly` | Same period last year | `revenue_ly_sar` |
| `_asof` | Value at the snapshot date | `age_at_asof`, `is_active_at_asof` |

Flags that record a data-quality finding say so in their name: `is_cost_imputed`, `is_cost_ratio_suspicious`, `is_opening_date_conflict`.
