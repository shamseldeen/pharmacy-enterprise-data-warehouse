# Data Catalog: Gold Layer (`wh_gold`)

Every table in the Gold layer of the Fabric Warehouse, built by the stored procedures in `fabric/03_gold/wh_gold.Warehouse/gold/StoredProcedures/`. `usp_build_all` rebuilds them in this order: calendar, dimensions, facts, aggregates. `ref_data_checks` has its own procedure and is not part of `usp_build_all`. Each table is rebuilt with DROP + CTAS.

All money columns are in Saudi riyals (`_sar`). Dimension keys (`_key`) are stable integers (ROW_NUMBER over the natural id). All data is synthetic.

## Overview

| Table | Type | Grain | Purpose |
|---|---|---|---|
| [`dim_date`](#dim_date) | Dimension | One row per calendar day | Calendar with Saudi events: Ramadan, both Eids, school and national holidays. |
| [`dim_pharmacy`](#dim_pharmacy) | Dimension | One row per pharmacy (branch) | Branch master: type, region, city, opening date, hours, and opening-date quality flags. |
| [`dim_product`](#dim_product) | Dimension | One row per product | Product master with list price, estimated unit cost and cost-quality flags. |
| [`dim_customer`](#dim_customer) | Dimension | One row per customer | Customer profile, loyalty, insurance status and marketing consent (adult and consented = marketable). |
| [`dim_employee`](#dim_employee) | Dimension | One row per employee | Staff master: role, pharmacist and dispensing rights, tenure. |
| [`dim_doctor`](#dim_doctor) | Dimension | One row per doctor | Prescribing doctors and their specialty. |
| [`dim_insurance_plan`](#dim_insurance_plan) | Dimension | One row per insurance plan | Payer, plan tier, coverage and copay defaults. |
| [`dim_supplier`](#dim_supplier) | Dimension | One row per supplier | Supplier master: type, lead time, cold-chain capability, minimum order. |
| [`fact_sales_line`](#fact_sales_line) | Fact | One row per sales line (item on a receipt) | Revenue, discount, VAT, estimated cost and margin; flags lines sold below cost. |
| [`fact_sales_transaction`](#fact_sales_transaction) | Fact | One row per sales transaction (receipt) | Receipt totals, status, payment method and returns. |
| [`fact_return`](#fact_return) | Fact | One row per return | Returned quantity and refund, linked to the original sale. |
| [`fact_rx_dispense`](#fact_rx_dispense) | Fact | One row per dispensed prescription item | Prescription dispensing: time to dispense, partial fills and who dispensed. |
| [`fact_claim`](#fact_claim) | Fact | One row per insurance claim | Claimed, approved and rejected amounts, adjudication time and rejection reason. |
| [`fact_inventory_snapshot`](#fact_inventory_snapshot) | Fact (periodic snapshot) | One row per month × pharmacy × product | Opening and closing stock, movements, stockouts and estimated lost sales. |
| [`fact_purchase_line`](#fact_purchase_line) | Fact | One row per purchase-order line | Ordered against received quantities, cost, and on-time / in-full delivery. |
| [`fact_branch_expense`](#fact_branch_expense) | Fact | One row per branch expense record | Branch operating expenses by category and month. |
| [`fact_timekeeping`](#fact_timekeeping) | Fact | One row per timekeeping record | Scheduled, worked, overtime and absence hours per employee and branch. |
| [`agg_branch_scorecard_monthly`](#agg_branch_scorecard_monthly) | Aggregate | One row per month × pharmacy | Branch KPIs ranked as percentiles within a peer group (pr_*) and combined into overall_score. |
| [`agg_product_loss_summary`](#agg_product_loss_summary) | Aggregate | One row per product | Below-cost sales: loss lines, loss SAR, time span, branches affected and stock still on hand. |
| [`agg_stock_aging_asof`](#agg_stock_aging_asof) | Aggregate | One row per pharmacy × product at the snapshot date | Stock age, dead-stock and rotation flags, and a recommended action. |
| [`ref_calendar_events`](#ref_calendar_events) | Reference | One row per calendar event | Event dates (Ramadan, Eids and so on) used to build dim_date. |
| [`ref_data_checks`](#ref_data_checks) | Reference | One row per check | Integrity signals tested, data gaps and data fingerprints, shown on the "What we checked" page. |

## Relationships

Facts join to dimensions on the integer keys: `pharmacy_key` → `dim_pharmacy`, `product_key` → `dim_product`, `customer_key` → `dim_customer`, `employee_key` (and `dispensing_employee_key`, `buyer_employee_key`, `responsible_employee_key`) → `dim_employee`, `doctor_key` → `dim_doctor`, `plan_key` → `dim_insurance_plan`, `supplier_key` → `dim_supplier`, and `date_key` (and `submitted_date_key`, `order_date_key`, `return_date_key`, `dispense_date_key`) → `dim_date`.

## dim_date

**Type:** Dimension · **Grain:** One row per calendar day

Calendar with Saudi events: Ramadan, both Eids, school and national holidays.

| Column | Data type |
|---|---|
| `date_key` | INT |
| `full_date` | DATE |
| `year_number` | INT |
| `quarter_number` | INT |
| `month_number` | INT |
| `month_name` | VARCHAR (3) |
| `month_start` | DATE |
| `day_of_month` | INT |
| `day_of_week` | INT |
| `day_name` | VARCHAR (3) |
| `is_weekend` | BIT |
| `is_ramadan` | BIT |
| `is_eid_al_fitr` | BIT |
| `is_eid_al_adha` | BIT |
| `is_school_holiday` | BIT |
| `is_national_holiday` | BIT |

## dim_pharmacy

**Type:** Dimension · **Grain:** One row per pharmacy (branch)

Branch master: type, region, city, opening date, hours, and opening-date quality flags.

| Column | Data type |
|---|---|
| `pharmacy_key` | INT |
| `pharmacy_id` | VARCHAR (8000) |
| `pharmacy_name` | VARCHAR (8000) |
| `branch_type` | VARCHAR (8000) |
| `region_id` | VARCHAR (8000) |
| `region_name` | VARCHAR (8000) |
| `city_name` | VARCHAR (8000) |
| `district_name` | VARCHAR (8000) |
| `map_location` | VARCHAR (8000) |
| `opening_date` | DATE |
| `opening_year` | INT |
| `weekly_operating_hours` | DECIMAL (6, 1) |
| `is_24h` | BIT |
| `first_activity_date` | DATE |
| `effective_opening_date` | DATE |
| `is_opening_date_conflict` | BIT |
| `is_age_lower_bound` | BIT |

## dim_product

**Type:** Dimension · **Grain:** One row per product

Product master with list price, estimated unit cost and cost-quality flags.

| Column | Data type |
|---|---|
| `product_key` | INT |
| `product_id` | VARCHAR (8000) |
| `trade_name` | VARCHAR (8000) |
| `generic_name` | VARCHAR (8000) |
| `strength` | VARCHAR (8000) |
| `strength_unit` | VARCHAR (8000) |
| `dosage_form` | VARCHAR (8000) |
| `category` | VARCHAR (8000) |
| `list_price_sar` | DECIMAL (18, 2) |
| `est_unit_cost_sar` | DECIMAL (18, 2) |
| `est_cost_ratio` | DECIMAL (9, 4) |
| `est_margin_pct` | DECIMAL (9, 4) |
| `batch_count` | INT |
| `has_recalled_batch` | BIT |
| `needs_cold_chain` | BIT |
| `is_cost_imputed` | BIT |
| `is_cost_ratio_suspicious` | BIT |

## dim_customer

**Type:** Dimension · **Grain:** One row per customer

Customer profile, loyalty, insurance status and marketing consent (adult and consented = marketable).

| Column | Data type |
|---|---|
| `customer_key` | INT |
| `customer_id` | VARCHAR (8000) |
| `full_name` | VARCHAR (8000) |
| `gender` | VARCHAR (8000) |
| `nationality_group` | VARCHAR (8000) |
| `region_name` | VARCHAR (8000) |
| `city_name` | VARCHAR (8000) |
| `birth_date` | DATE |
| `age_at_asof` | INT |
| `age_band` | VARCHAR (7) |
| `registration_date` | DATE |
| `tenure_years` | INT |
| `preferred_contact_channel` | VARCHAR (8000) |
| `is_loyalty_enrolled` | BIT |
| `loyalty_tier` | VARCHAR (8000) |
| `loyalty_status` | VARCHAR (8000) |
| `has_insurance_ever` | BIT |
| `is_insured_at_asof` | BIT |
| `can_contact_sms` | BIT |
| `can_contact_email` | BIT |
| `can_contact_app` | BIT |
| `is_marketable` | BIT |

## dim_employee

**Type:** Dimension · **Grain:** One row per employee

Staff master: role, pharmacist and dispensing rights, tenure.

| Column | Data type |
|---|---|
| `employee_key` | INT |
| `employee_id` | VARCHAR (8000) |
| `full_name` | VARCHAR (8000) |
| `gender` | VARCHAR (8000) |
| `nationality_group` | VARCHAR (8000) |
| `job_role` | VARCHAR (8000) |
| `is_pharmacist` | BIT |
| `can_dispense` | BIT |
| `hire_date` | DATE |
| `termination_date` | DATE |
| `is_active_at_asof` | BIT |
| `tenure_years` | INT |

## dim_doctor

**Type:** Dimension · **Grain:** One row per doctor

Prescribing doctors and their specialty.

| Column | Data type |
|---|---|
| `doctor_key` | INT |
| `doctor_id` | VARCHAR (8000) |
| `full_name` | VARCHAR (8000) |
| `gender` | VARCHAR (8000) |
| `specialty_name` | VARCHAR (8000) |
| `region_name` | VARCHAR (8000) |
| `city_name` | VARCHAR (8000) |
| `active_from` | DATE |
| `active_to` | DATE |
| `is_active_at_asof` | BIT |

## dim_insurance_plan

**Type:** Dimension · **Grain:** One row per insurance plan

Payer, plan tier, coverage and copay defaults.

| Column | Data type |
|---|---|
| `plan_key` | INT |
| `insurance_plan_id` | VARCHAR (8000) |
| `payer_name` | VARCHAR (8000) |
| `plan_name` | VARCHAR (8000) |
| `plan_tier` | VARCHAR (8000) |
| `network_type` | VARCHAR (8000) |
| `default_coverage_pct` | DECIMAL (5, 4) |
| `default_copay_sar` | DECIMAL (18, 2) |
| `annual_benefit_limit_sar` | DECIMAL (18, 2) |
| `covers_prescriptions` | BIT |

## dim_supplier

**Type:** Dimension · **Grain:** One row per supplier

Supplier master: type, lead time, cold-chain capability, minimum order.

| Column | Data type |
|---|---|
| `supplier_key` | INT |
| `supplier_id` | VARCHAR (8000) |
| `supplier_name` | VARCHAR (8000) |
| `supplier_type_name` | VARCHAR (8000) |
| `region_name` | VARCHAR (8000) |
| `city_name` | VARCHAR (8000) |
| `standard_lead_time_days` | INT |
| `is_cold_chain_capable` | BIT |
| `minimum_order_value_sar` | DECIMAL (18, 2) |
| `supplier_status` | VARCHAR (8000) |

## fact_sales_line

**Type:** Fact · **Grain:** One row per sales line (item on a receipt)

Revenue, discount, VAT, estimated cost and margin; flags lines sold below cost.

| Column | Data type |
|---|---|
| `sales_line_id` | VARCHAR (8000) |
| `transaction_id` | VARCHAR (8000) |
| `line_number` | INT |
| `date_key` | INT |
| `transaction_ts` | DATETIME2 (6) |
| `pharmacy_key` | INT |
| `product_key` | INT |
| `customer_key` | INT |
| `employee_key` | INT |
| `channel_id` | VARCHAR (8000) |
| `quantity` | INT |
| `unit_price_sar` | DECIMAL (18, 2) |
| `gross_amount_sar` | DECIMAL (18, 2) |
| `discount_amount_sar` | DECIMAL (18, 2) |
| `net_before_vat_sar` | DECIMAL (18, 2) |
| `vat_rate` | DECIMAL (5, 4) |
| `vat_amount_sar` | DECIMAL (18, 2) |
| `net_amount_sar` | DECIMAL (18, 2) |
| `est_cost_sar` | DECIMAL (18, 2) |
| `est_margin_sar` | DECIMAL (18, 2) |
| `is_sold_below_cost` | BIT |

## fact_sales_transaction

**Type:** Fact · **Grain:** One row per sales transaction (receipt)

Receipt totals, status, payment method and returns.

| Column | Data type |
|---|---|
| `transaction_id` | VARCHAR (8000) |
| `receipt_number` | VARCHAR (8000) |
| `date_key` | INT |
| `transaction_ts` | DATETIME2 (6) |
| `hour_of_day` | INT |
| `pharmacy_key` | INT |
| `customer_key` | INT |
| `employee_key` | INT |
| `channel_id` | VARCHAR (8000) |
| `transaction_status` | VARCHAR (8000) |
| `is_completed` | BIT |
| `is_cancelled` | BIT |
| `is_voided` | BIT |
| `line_count` | INT |
| `items_qty` | INT |
| `gross_amount_sar` | DECIMAL (38, 2) |
| `discount_amount_sar` | DECIMAL (38, 2) |
| `net_before_vat_sar` | DECIMAL (38, 2) |
| `vat_amount_sar` | DECIMAL (38, 2) |
| `net_amount_sar` | DECIMAL (38, 2) |
| `est_margin_sar` | DECIMAL (38, 2) |
| `payment_method_id` | VARCHAR (8000) |
| `paid_amount_sar` | DECIMAL (38, 2) |
| `return_lines` | INT |
| `refund_amount_sar` | DECIMAL (38, 2) |
| `has_return` | BIT |

## fact_return

**Type:** Fact · **Grain:** One row per return

Returned quantity and refund, linked to the original sale.

| Column | Data type |
|---|---|
| `return_id` | VARCHAR (8000) |
| `original_transaction_id` | VARCHAR (8000) |
| `original_sales_line_id` | VARCHAR (8000) |
| `return_date_key` | INT |
| `return_ts` | DATETIME2 (6) |
| `sale_date_key` | INT |
| `days_to_return` | INT |
| `pharmacy_key` | INT |
| `customer_key` | INT |
| `employee_key` | INT |
| `product_key` | INT |
| `return_reason` | VARCHAR (8000) |
| `return_quantity` | INT |
| `refund_amount_sar` | DECIMAL (18, 2) |
| `sold_quantity` | INT |
| `sold_net_amount_sar` | DECIMAL (18, 2) |
| `is_refund_above_sale` | BIT |
| `is_qty_above_sale` | BIT |

## fact_rx_dispense

**Type:** Fact · **Grain:** One row per dispensed prescription item

Prescription dispensing: time to dispense, partial fills and who dispensed.

| Column | Data type |
|---|---|
| `dispense_id` | VARCHAR (8000) |
| `prescription_id` | VARCHAR (8000) |
| `prescription_item_id` | VARCHAR (8000) |
| `dispense_date_key` | INT |
| `prescribed_ts` | DATETIME2 (6) |
| `dispense_ts` | DATETIME2 (6) |
| `minutes_to_dispense` | INT |
| `pharmacy_key` | INT |
| `customer_key` | INT |
| `doctor_key` | INT |
| `product_key` | INT |
| `dispensing_employee_key` | INT |
| `prescription_source` | VARCHAR (8000) |
| `prescribed_quantity` | INT |
| `dispensed_quantity` | INT |
| `is_partial_fill` | BIT |
| `dosage_frequency` | VARCHAR (8000) |
| `duration_days` | INT |
| `authorized_refills` | INT |
| `route` | VARCHAR (8000) |
| `is_dispensed_by_pharmacist` | BIT |
| `is_authorized_dispenser` | BIT |
| `dispenser_role` | VARCHAR (8000) |
| `dispense_linked_transaction_id` | VARCHAR (8000) |

## fact_claim

**Type:** Fact · **Grain:** One row per insurance claim

Claimed, approved and rejected amounts, adjudication time and rejection reason.

| Column | Data type |
|---|---|
| `claim_id` | VARCHAR (8000) |
| `submitted_date_key` | INT |
| `submitted_ts` | DATETIME2 (6) |
| `adjudicated_ts` | DATETIME2 (6) |
| `hours_to_adjudicate` | INT |
| `pharmacy_key` | INT |
| `customer_key` | INT |
| `plan_key` | INT |
| `prescription_id` | VARCHAR (8000) |
| `linked_transaction_id` | VARCHAR (8000) |
| `claim_status_name` | VARCHAR (8000) |
| `rejection_reason_name` | VARCHAR (8000) |
| `claimed_amount_sar` | DECIMAL (18, 2) |
| `approved_amount_sar` | DECIMAL (18, 2) |
| `rejected_amount_sar` | DECIMAL (18, 2) |
| `customer_copay_sar` | DECIMAL (18, 2) |
| `is_fully_rejected` | BIT |
| `is_partially_rejected` | BIT |

## fact_inventory_snapshot

**Type:** Fact (periodic snapshot) · **Grain:** One row per month × pharmacy × product

Opening and closing stock, movements, stockouts and estimated lost sales.

| Column | Data type |
|---|---|
| `date_key` | INT |
| `month_start` | DATE |
| `pharmacy_key` | INT |
| `product_key` | INT |
| `opening_stock` | INT |
| `net_movement_qty` | INT |
| `closing_stock` | INT |
| `unfulfilled_qty` | INT |
| `is_stockout` | BIT |
| `has_lost_demand` | BIT |
| `stock_value_at_cost_sar` | DECIMAL (18, 2) |
| `lost_sales_value_est_sar` | DECIMAL (18, 2) |

## fact_purchase_line

**Type:** Fact · **Grain:** One row per purchase-order line

Ordered against received quantities, cost, and on-time / in-full delivery.

| Column | Data type |
|---|---|
| `po_line_id` | VARCHAR (8000) |
| `purchase_order_id` | VARCHAR (8000) |
| `order_date_key` | INT |
| `order_date` | DATE |
| `expected_delivery_date` | DATE |
| `receipt_date` | DATE |
| `order_status` | VARCHAR (8000) |
| `supplier_key` | INT |
| `product_key` | INT |
| `buyer_employee_key` | INT |
| `warehouse_id` | VARCHAR (8000) |
| `warehouse_name` | VARCHAR (8000) |
| `warehouse_region` | VARCHAR (8000) |
| `ordered_qty` | INT |
| `unit_cost_sar` | DECIMAL (18, 2) |
| `gross_cost_sar` | DECIMAL (18, 2) |
| `discount_sar` | DECIMAL (18, 2) |
| `tax_sar` | DECIMAL (18, 2) |
| `net_cost_sar` | DECIMAL (18, 2) |
| `received_qty` | INT |
| `rejected_qty` | INT |
| `accepted_qty` | INT |
| `is_received` | BIT |
| `is_on_time` | BIT |
| `is_in_full` | BIT |
| `is_otif` | BIT |
| `lead_time_days` | INT |
| `delay_days` | INT |

## fact_branch_expense

**Type:** Fact · **Grain:** One row per branch expense record

Branch operating expenses by category and month.

| Column | Data type |
|---|---|
| `branch_expense_id` | VARCHAR (8000) |
| `date_key` | INT |
| `accounting_month` | DATE |
| `pharmacy_key` | INT |
| `expense_category_id` | VARCHAR (8000) |
| `expense_category_code` | VARCHAR (8000) |
| `expense_group` | VARCHAR (8000) |
| `cost_basis` | VARCHAR (8000) |
| `expense_amount_sar` | DECIMAL (18, 2) |

## fact_timekeeping

**Type:** Fact · **Grain:** One row per timekeeping record

Scheduled, worked, overtime and absence hours per employee and branch.

| Column | Data type |
|---|---|
| `timekeeping_id` | VARCHAR (8000) |
| `date_key` | INT |
| `accounting_month` | DATE |
| `employee_key` | INT |
| `pharmacy_key` | INT |
| `assignment_id` | VARCHAR (8000) |
| `scheduled_hours` | INT |
| `worked_hours` | INT |
| `overtime_hours` | INT |
| `absence_hours` | INT |
| `sick_leave_hours` | INT |
| `late_minutes` | INT |

## agg_branch_scorecard_monthly

**Type:** Aggregate · **Grain:** One row per month × pharmacy

Branch KPIs ranked as percentiles within a peer group (pr_*) and combined into overall_score.

| Column | Data type |
|---|---|
| `date_key` | INT |
| `month_start` | DATE |
| `pharmacy_key` | INT |
| `branch_type` | VARCHAR (8000) |
| `region_name` | VARCHAR (8000) |
| `city_name` | VARCHAR (8000) |
| `region_tier` | VARCHAR (5) |
| `effective_opening_date` | DATE |
| `is_age_lower_bound` | BIT |
| `age_months` | INT |
| `size_band` | VARCHAR (1) |
| `is_band_from_same_year` | BIT |
| `peer_group` | VARCHAR (7) |
| `is_ranked` | BIT |
| `is_growth_ranked` | BIT |
| `revenue_sar` | DECIMAL (18, 2) |
| `margin_sar` | DECIMAL (18, 2) |
| `transactions` | INT |
| `refund_sar` | DECIMAL (18, 2) |
| `worked_hours` | BIGINT |
| `expense_sar` | DECIMAL (18, 2) |
| `revenue_ly_sar` | DECIMAL (18, 2) |
| `sales_per_hour` | DECIMAL (18, 2) |
| `margin_pct` | DECIMAL (9, 2) |
| `expense_pct` | DECIMAL (9, 2) |
| `avg_basket_sar` | DECIMAL (18, 2) |
| `return_pct` | DECIMAL (9, 2) |
| `yoy_growth_pct` | DECIMAL (9, 2) |
| `pr_sales_per_hour` | DECIMAL (5, 4) |
| `pr_margin_pct` | DECIMAL (5, 4) |
| `pr_expense_pct` | DECIMAL (5, 4) |
| `pr_avg_basket` | DECIMAL (5, 4) |
| `pr_return_pct` | DECIMAL (5, 4) |
| `pr_yoy_growth` | DECIMAL (5, 4) |
| `overall_score` | DECIMAL (5, 4) |
| `responsible_employee_key` | INT |
| `is_responsible_inferred` | BIT |
| `is_responsible_changed` | BIT |
| `months_with_current_responsible` | BIGINT |

## agg_product_loss_summary

**Type:** Aggregate · **Grain:** One row per product

Below-cost sales: loss lines, loss SAR, time span, branches affected and stock still on hand.

| Column | Data type |
|---|---|
| `product_key` | INT |
| `product_id` | VARCHAR (8000) |
| `trade_name` | VARCHAR (8000) |
| `category` | VARCHAR (8000) |
| `is_cost_imputed` | BIT |
| `is_cost_ratio_suspicious` | BIT |
| `total_lines` | BIGINT |
| `loss_lines` | INT |
| `loss_lines_pct` | DECIMAL (5, 2) |
| `total_qty` | BIGINT |
| `loss_qty` | BIGINT |
| `revenue_sar` | DECIMAL (18, 2) |
| `margin_sar` | DECIMAL (18, 2) |
| `loss_sar` | DECIMAL (18, 2) |
| `first_loss_date` | DATE |
| `last_loss_date` | DATE |
| `loss_span_days` | INT |
| `loss_months` | INT |
| `loss_pharmacies` | INT |
| `closing_stock_asof` | INT |
| `stock_value_asof_sar` | DECIMAL (18, 2) |

## agg_stock_aging_asof

**Type:** Aggregate · **Grain:** One row per pharmacy × product at the snapshot date

Stock age, dead-stock and rotation flags, and a recommended action.

| Column | Data type |
|---|---|
| `date_key` | INT |
| `snap_month` | DATE |
| `snap_date` | DATE |
| `pharmacy_key` | INT |
| `product_key` | INT |
| `supplier_key` | INT |
| `closing_stock` | INT |
| `stock_value_sar` | DECIMAL (18, 2) |
| `last_sale_date` | DATE |
| `days_since_last_sale` | INT |
| `chain_qty_180d` | BIGINT |
| `other_pharmacies_lost_demand` | INT |
| `other_lost_qty` | BIGINT |
| `age_bucket` | VARCHAR (17) |
| `is_dead_stock` | BIT |
| `is_rotation_candidate` | BIT |
| `is_no_demand_anywhere` | BIT |
| `recommended_action` | VARCHAR (15) |

## ref_calendar_events

**Type:** Reference · **Grain:** One row per calendar event

Event dates (Ramadan, Eids and so on) used to build dim_date.

| Column | Data type |
|---|---|
| `event_type` | VARCHAR (30) |
| `event_name` | VARCHAR (80) |
| `start_date` | DATE |
| `end_date` | DATE |
| `is_approximate` | BIT |
| `source_note` | VARCHAR (200) |

## ref_data_checks

**Type:** Reference · **Grain:** One row per check

Integrity signals tested, data gaps and data fingerprints, shown on the "What we checked" page.

| Column | Data type |
|---|---|
| `check_id` | VARCHAR (10) |
| `check_group` | VARCHAR (40) |
| `check_name` | VARCHAR (200) |
| `what_tested` | VARCHAR (300) |
| `result` | VARCHAR (300) |
| `verdict` | VARCHAR (40) |

