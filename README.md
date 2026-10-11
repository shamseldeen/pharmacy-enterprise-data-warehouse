# 💊 Pharmacy Enterprise Data Warehouse

## 📍 Current Project Checkpoint — 2026-10-11

> Status is based on saved Fabric notebook outputs and the current Lakehouse workspace inspection. The user runs and changes Fabric items; this repository records verified results and the next controlled step.

### The project journey

1. **Business design and source data.** The project began as a multi-domain pharmacy enterprise warehouse, with a finalized synthetic operational dataset, source catalog, business relationships, and cross-domain validation. The existing repository records 72 source files, 25/25 RAW checks, and a SQL Server Bronze load of 72 tables and 60,234,021 rows. Those remain historical repository claims, not a live SQL Server revalidation in this checkpoint.
2. **Fabric foundation.** The project continued in Microsoft Fabric using the finalized RAW files in OneLake and the `Pharmacy-Enterprise-DW-DEV` workspace with `lh_bronze`. The governing Bronze rule remains one physical RAW CSV to one source-aligned Bronze Delta table. Bronze preserves source fidelity; cleaning, deduplication, conformance, and business corrections belong in Silver.
   - **Earlier reference exercise (`nb_01_bronze_reference_regions`):** the saved run read `Files/raw/reference/regions.csv`, wrote `bronze.reference_regions`, and reconciled 13 RAW rows to 13 Bronze rows. It verified 13 distinct `region_id` values, zero invalid/empty IDs, and zero format violations. Its cell 13 inventory check failed at 67/72 files; cell 14 re-ran recursive discovery and returned 72/72 PASS. The saved evidence does not establish why the first listing was incomplete. This was a single-table/reference exercise, not the full ingestion.

3. **Fabric Bronze ingestion.** Saved `nb_02_bronze_ingestion_framework` outputs record 72/72 files processed, 72 registered Bronze tables, 60,234,021 RAW rows reconciled to 60,234,021 Bronze rows (difference 0), schema reconciliation PASS, and a 451.24 second pipeline runtime. The saved notebook records the Fabric Bronze Ingestion Gate as PASS.
4. **Bronze Data Quality.** Saved outputs record a completeness profile for `bronze.crm_customer_consent_history` (1,200,051 rows, 7 columns, 0 NULLs in each column), a 72-table completeness run covering 612 column profiles, and 19 columns with at least one NULL. The employee temporal rule returned 0 violations and PASS. These are useful controls, but they do not complete uniqueness, key, referential, domain, business, or cross-domain DQ gates.
5. **Historical `nb_02` issue — not the current project stop.** A saved catalog-inventory cell failed with `NameError: name 'bronze_table_names' is not defined`; a later cell in `nb_03_silver_discovery_contract` independently discovers the Bronze catalog. The old `nb_02` error is not a reason to rerun the successful 72-table ingestion, and its trailing cell should not be run without restoring its inputs.

6. **Silver implementation is in progress.** The current Fabric `lh_silver` inspection showed eight Delta tables in schema `silver`: `erp_branch_lifecycle_events` (1,081 rows), `erp_employees` (10,000), `erp_job_roles` (10), `erp_pharmacies` (1,000), `rx_dispenses` (173,913), `rx_doctors` (20,000), `rx_prescription_items` (203,899), and `rx_prescriptions` (100,000). The `dbo` schema was present but empty at that inspection; the active objects were in `silver`. Do not create duplicate `dbo` tables or rerun completed transforms without a specific reason. User-shared RX checks passed for unique dispense grain, all 173,913 prescription-item links, prescription-item/header links, and dispense chronology. User-shared branch-event evidence reports 1,081 source rows, 1,081 distinct nonblank event IDs, and PASS. These checks cover selected entities; they do not mean all Silver contracts or the full project are complete.

### Current status and exact next action

- **Completed:** Fabric Bronze ingestion (72/72 tables; 60,234,021 rows reconciled); initial Bronze profiling; eight Silver Delta tables; selected RX grain, link, and chronology checks; branch-event identifier check.
- **In progress:** Silver source coverage, contract-to-implementation traceability, and entity-level quality gates.
- **Not started:** Gold facts and dimensions, semantic model, Power BI reports for this warehouse, production orchestration, CI/CD deployment, and production readiness.

**Latest user-shared execution evidence:** `bronze.erp_branch_lifecycle_events` had 1,081 source rows, 1,081 distinct event IDs, and zero NULL/blank IDs (PASS). The exact cell number was not included in the shared output. RX checks already reported PASS and must not be repeated without a changed source or a concrete failure.

**Next laptop action:** open the ERP Silver notebook containing the branch-event check (`nb_05_silver_erp_transform`; confirm its title in Fabric), and add a Markdown checkpoint after the existing output only if it is not already documented. Record the source and target names, the already-observed PASS result, the matching Silver row count if it is already visible, and the decision/rule applied. Do not rerun the key or RX checks. Then begin the next Silver vertical slice for the product and core-sales sources, using the existing Bronze catalog and `nb_03` mapping as references; validate only new transformations and their distinct business grain. The next produced output should be a documented, reconciled Silver sales foundation for Gold.

### Engineering notes to carry forward

- `Candidate Silver entities: 57` is a discovery count, not 57 implemented Silver tables.
- Do not claim the yearly schemas all pass until the unreadable comparison outputs are inspected.
- Review multiword domain parsing (`supply_chain`) and numeric identifier classification before treating inferred metadata as contract truth.
- Keep NULL findings as profiling evidence; define field-specific rules before deciding whether NULL is acceptable or an exception.
- The status table and roadmap reflect the Fabric checkpoint dated 2026-10-11. Historical SQL Server work remains labeled as its own earlier milestone.
- Execution status is based on saved notebook outputs and workspace inspection. The project notebook history includes the `nb_01_bronze_reference_regions` notebook, its BuiltIn package, and `nb_02_bronze_ingestion_framework`; the `nb_03_silver_discovery_contract` notebook is the current discovery reference. These notebook artifacts are not yet committed to this repository.

### Consolidated route to completion

`Reuse saved Bronze profiling and reconciliation evidence` → `document approved decisions and map Silver contracts` → `extend Silver with product and core-sales entities` → `complete remaining Silver entities by business value` → `apply one new grain, referential, and reconciliation gate per entity without rerunning completed checks` → `design Gold facts and conformed dimensions at explicit grains` → `publish the semantic model and governed measures` → `build and validate Power BI reports` → `optimize refresh, model performance, security, and monitoring` → `orchestrate stable notebooks with Fabric pipelines, retries, and alerts` → `implement Git integration and CI/CD` → `publish portfolio documentation; evaluate AI only against a validated use case` .

---



![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Data%20Platform-742774) &nbsp; ![SQL Server](https://img.shields.io/badge/SQL%20Server-Data%20Warehouse-CC2927?logo=microsoftsqlserver&logoColor=white)
![T-SQL](https://img.shields.io/badge/T--SQL-ETL%20%26%20Analytics-0078D4)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-orange)
![Power BI](https://img.shields.io/badge/Power%20BI-Not%20Started-F2C811?logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-Analytics%20%26%20Automation-3776AB?logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Status-Silver%20In%20Progress-8a2be2)
![License](https://img.shields.io/badge/License-MIT-green)

> **An enterprise-scale retail pharmacy data warehouse evolving from SQL Server design into a Microsoft Fabric Medallion platform. Fabric Bronze ingestion is verified, Silver is in progress, and Gold, the semantic model, and Power BI reports remain ahead.**

This project is designed as a realistic portfolio implementation rather than a single-dashboard demo. It integrates pharmacy retail operations across **sales, customers, products, prescriptions, insurance, procurement, suppliers, inventory, workforce, branch operations, and delivery** into one analytical platform.

![Pharmacy Enterprise Data Warehouse — Project Overview](docs/architecture/01_project_overview.png)

---

## 📌 Project Status

| Area | Status |
|---|---|
| Business scope & source-system design | ✅ Complete |
| Synthetic RAW business dataset | ✅ Complete / locked |
| Cross-domain RAW validation | ✅ 25 / 25 checks passed |
| Metadata & business data catalog | ✅ Complete |
| Architecture, ERD & lineage | ✅ Complete |
| Bronze DDL generation and table creation | ✅ 72 tables created |
| Bronze loading procedure and batch audit | ✅ Implemented |
| RAW → Bronze SQL Server ingestion | ✅ 72 / 72 tables loaded |
| Fabric Bronze rows reconciled | ✅ 60,234,021 rows; 72/72 tables |
| Bronze failed tables | ✅ 0 |
| Bronze DQ and integrity coverage | 🔄 Partial; selected checks only |
| Silver layer | 🔄 In progress — 8 Delta tables verified in `silver` |
| Gold dimensional layer | ⏳ Not started |
| Semantic model & Power BI reports | ⏳ Not started |
| Orchestration, monitoring, CI/CD, advanced analytics | ⏳ Future phases |

**Current milestone:** `Silver implementation — 8 verified tables; expand sales foundation`

**Next milestone:** `Silver product and core-sales foundation with source-to-target reconciliation`

---

## 🎯 Project Objective

The goal is to build a reproducible **enterprise analytical platform for a multi-branch retail pharmacy business**.

The warehouse is intended to support questions such as:

- Which branches, regions, categories, and products drive revenue and profitability?
- How do discounts, returns, and product mix affect margin?
- Which suppliers perform best on fill rate, lead time, and OTIF?
- Where are stockouts, overstock, dead stock, and near-expiry risks concentrated?
- How do prescriptions and insurance claims behave across products, doctors, plans, and branches?
- What customer, workforce, and operational patterns explain branch performance?
- How can trusted warehouse data feed executive Power BI reporting and advanced Python analytics?

---

## 📊 Dataset Scale

The project uses a large synthetic operational dataset designed for realistic warehousing and analytics workloads.

| Dataset | Scale |
|---|---:|
| Product master | **11,274 SKUs** |
| Pharmacy branches | **1,000** |
| Employees | **10,000** |
| Customers | **1,000,000** |
| Doctors | **20,000** |
| Suppliers | **2,000** |
| Warehouses | **30** |
| POS transaction headers | **10,000,000** |
| Completed POS transactions | **9,890,375** |
| Canonical sales lines | **30,365,507** |
| Purchase orders | **25,000** |
| Purchase-order lines | **82,140** |
| Purchase receipts | **80,478** |
| Product batches | **40,000** |
| Inventory movements | **410,576** |
| Prescriptions | **100,000** |
| Prescription items | **203,899** |
| Insurance claims | **75,000** |
| Insurance claim lines | **153,098** |

> **Note:** the 30.3M canonical sales-line fact has been validated and financially reconciled, but the large standalone materialized file is not committed directly to the portable repository. Its materialization rule and validation evidence are documented separately.

---

## 🥉 Bronze Ingestion Milestone

The finalized RAW dataset has been successfully loaded into SQL Server as a source-aligned Bronze layer.

| Metric | Result |
|---|---:|
| Batch ID | `841372319990` |
| Bronze tables | **72** |
| Successful tables | **72** |
| Failed tables | **0** |
| Rows loaded | **60,234,021** |
| Load status | **SUCCESS** |

The ingestion process was executed through a generated SQL Server loading procedure with table-level batch auditing.

The audit records capture:

```text
_batch_id
bronze_table_name
source_domain
relative_file_path
_ingestion_ts
rows_loaded
load_status
error_message
```

The loader completed 71 tables during its main execution. One small CSV containing a quoted comma required an isolated reload using the file's Windows-style `CRLF` row ending:

```sql
ROWTERMINATOR = '0x0D0A'
```

After the isolated reload and audit correction, the final batch reached:

```text
72 / 72 tables
60,234,021 rows
0 failures
```

This milestone confirms successful ingestion. It does **not** yet represent completion of Bronze final validation, key-integrity testing, duplicate analysis, null profiling, or cross-table reconciliation.

---

## 🏗️ Architecture

![End-to-End Architecture](docs/architecture/03_end_to_end_architecture.png)

The project follows a **Medallion Architecture**:

```text
Operational / Synthetic Source Systems
                │
                ▼
              RAW
        source evidence
                │
                ▼
             BRONZE
   source-aligned ingestion + audit
                │
                ▼
             SILVER
 cleaning + standardization + conformance
                │
                ▼
              GOLD
 dimensions + facts + analytical marts
                │
        ┌───────┼────────┐
        ▼       ▼        ▼
     Power BI   SQL     Python
```

### Layer Responsibilities

| Layer | Responsibility |
|---|---|
| **RAW** | Preserve source-system evidence and original business records |
| **Bronze** | Load source-aligned tables with technical metadata and auditability |
| **Silver** | Clean, standardize, validate, deduplicate, and conform business entities |
| **Gold** | Build dimensional models, KPI-ready facts, dimensions, and marts |
| **Consumption** | Power BI, SQL analysis, Python analytics, forecasting |

---

## 🏢 Source Domains

![Source Systems Overview](docs/architecture/02_source_systems_overview.png)

The warehouse is modeled around multiple operational domains instead of one flat sales file.

| Domain | Main Business Data |
|---|---|
| **Reference / Master** | Regions, cities, products, reference codes |
| **ERP / Branch Operations** | Pharmacies, employees, job roles, shifts, operating hours, branch expenses |
| **POS / Finance** | Transactions, sales lines, payments, returns |
| **CRM & Loyalty** | Customers, loyalty accounts, consent history, customer-service interactions |
| **Prescription / RX** | Prescriptions, prescription items, dispensing events, doctors |
| **Insurance** | Insurance plans, memberships, eligibility, claims, claim lines |
| **Supply Chain** | Suppliers, contracts, warehouses, purchase orders, receipts, batches |
| **Inventory** | Inventory movements, snapshots, cycle counts, transfers |
| **Delivery** | Delivery orders and delivery methods |

---

## 🧱 RAW → Bronze Implementation

![RAW to Bronze Mapping](docs/architecture/04_raw_to_bronze_mapping.png)

The finalized RAW structure was materialized into **72 source-aligned Bronze tables** in SQL Server.

The implementation deliberately keeps Bronze close to the source files:

- One source-aligned Bronze table for each finalized RAW file
- Source-domain prefixes in Bronze table names
- Conservative SQL Server datatypes
- No business cleansing or dimensional transformations
- Generated DDL for reproducible table creation
- Generated loading procedure for repeatable ingestion
- Compact table-level batch auditing
- Failure isolation without reloading successful large tables

### Implementation Flow

```text
Finalized RAW CSV files
        ↓
Python Bronze DDL Generator
        ↓
72 SQL Server Bronze tables
        ↓
Python Procedure Generator
        ↓
Generated T-SQL loading procedure
        ↓
BULK INSERT execution
        ↓
Compact batch/file audit
```

### Example Source-to-Bronze Mappings

```text
pos/sales_transactions/sales_transactions_2020.csv
    → bronze.pos_sales_transactions_2020

pos/canonical_sales_lines/canonical_sales_lines_2025_FINAL.csv
    → bronze.pos_canonical_sales_lines_2025_final

pos/payments/payments_2025.csv
    → bronze.pos_payments_2025

reference/products_FINAL_11274.csv
    → bronze.reference_products_final_11274

rx/prescription_items_FINAL.csv
    → bronze.rx_prescription_items_final

supply_chain/purchase_order_lines_FINAL.csv
    → bronze.supply_chain_purchase_order_lines_final
```

### Loader Behavior

The generated loader:

- Loads CSV files into their corresponding Bronze tables
- Uses UTF-8 CSV handling
- Records the batch, table, source file, row count, status, and error message
- Continues loading independent tables when one table fails
- Supports isolated troubleshooting and reload of only the failed table
- Avoids repeating a successful 60-million-row ingestion because of one small failure

For this Windows-generated dataset, the final loader configuration should use:

```sql
ROWTERMINATOR = '0x0D0A'
```

If one table fails, its `BULK INSERT` should be executed separately without `TRY/CATCH` to expose the exact row and column error. The full batch should not be rerun when the other tables have already succeeded.

Bronze remains a source-aligned ingestion layer. Cleansing, standardization, conformance, business-rule enforcement, and dimensional transformations are deferred to Silver.

---

## 🧪 Data Quality & Reconciliation

![Data Quality and Reconciliation](docs/architecture/06_data_quality_reconciliation.png)

Data quality is treated as part of the architecture, not as a final dashboard check.

The RAW business model passed **25 / 25 cross-domain validation checks**.

Key controls include:

- Unique product identifiers and barcodes
- Positive product prices
- Referential integrity across key business entities
- Exclusion of cancelled/voided POS sales from canonical revenue
- Completed-sales to captured-payment reconciliation
- Prescription-to-customer / doctor / product validation
- Insurance claim and plan validation
- `approved_amount <= claimed_amount`
- Purchase-order to supplier / warehouse integrity
- Receipt to purchase-order-line integrity
- Product-batch to product / supplier integrity
- Balanced inventory transfer OUT / IN pairs
- Valid inventory snapshot equation
- Non-negative canonical closing inventory

### Financial Reconciliation

```text
Completed sales     = 1,871,609,856.56 SAR
Captured payments   = 1,871,609,856.56 SAR
Difference          =             0.00 SAR
```

This reconciliation is one of the main controls protecting downstream profitability and KPI analysis.

---

## 🔗 Core Business Flow

![Business ERD and Cross-Domain Relationships](docs/architecture/05_business_erd_cross_domain.png)

```text
CUSTOMERS / PHARMACIES / EMPLOYEES
                │
                ▼
        SALES TRANSACTIONS
                │
        ┌───────┼────────┐
        ▼       ▼        ▼
   SALES LINES PAYMENTS RETURNS
        │
        ▼
     PRODUCTS


CUSTOMERS + DOCTORS + PHARMACIES
                │
                ▼
         PRESCRIPTIONS
                │
                ▼
      PRESCRIPTION ITEMS
                │
                ▼
        INSURANCE CLAIMS
                │
                ▼
       INSURANCE CLAIM LINES


SUPPLIERS + WAREHOUSES
                │
                ▼
        PURCHASE ORDERS
                │
                ▼
      PURCHASE ORDER LINES
                │
                ▼
       PURCHASE RECEIPTS
                │
                ▼
      INVENTORY MOVEMENTS
                │
                ▼
      INVENTORY SNAPSHOTS
```

---

## 🥈 Silver Layer — implementation in progress

![Planned Silver and Gold Architecture](docs/architecture/07_planned_silver_gold.png)

> **Status: IN PROGRESS — eight Delta tables are present in schema `silver`; the deployed inventory is listed in the checkpoint above. This proposed object list is not the current inventory.**

Silver converts source-aligned Bronze tables into trusted, conformed entities. The current deployed inventory is limited to the eight tables listed in the checkpoint above; the proposed names below are targets, not deployed objects.

### Candidate Silver Entities — proposed target names

```text
silver.product
silver.customer
silver.pharmacy
silver.employee
silver.doctor

silver.sales_transaction
silver.sales_line

silver.prescription
silver.insurance_member
silver.insurance_claim

silver.supplier
silver.procurement
silver.batch

silver.inventory_movement
silver.inventory_snapshot
```

Silver will own:

- Standardized data types and naming
- Canonical business statuses
- Explicit deduplication rules
- Null and invalid-value treatment
- Conformed entity keys
- Referential validation
- Monetary and date normalization
- Data-quality exception handling

---

## 🥇 Gold Layer — not started

> **Status: NOT STARTED — define dimensional grains, keys, and slowly changing dimension rules after the required Silver entities are validated.**

The Gold layer will expose business-ready dimensional models.

### Planned Conformed Dimensions

```text
gold.dim_date
gold.dim_pharmacy
gold.dim_product
gold.dim_customer
gold.dim_employee
gold.dim_doctor
gold.dim_supplier
gold.dim_insurance_plan
```

### Planned Analytical Facts

```text
gold.fact_sales
gold.fact_return
gold.fact_prescription
gold.fact_insurance_claim
gold.fact_purchase
gold.fact_inventory_movement
gold.fact_inventory_snapshot
gold.fact_delivery
gold.fact_customer_service
gold.fact_branch_expense
```

The final grain, surrogate-key strategy, SCD handling, and analytical relationships will be fixed during Silver/Gold implementation rather than prematurely enforced in Bronze.

---

## 📈 Business Analytics Coverage

The completed warehouse is designed to support:

| Area | Example Analytics |
|---|---|
| **Executive** | Revenue, profit, growth, margin, regional performance |
| **Sales** | ATV, UPT, product/category mix, discount impact, returns |
| **Branches** | Branch ranking, same-store growth, labor productivity |
| **Products** | Revenue, units, margin, brand/generic mix |
| **Customers** | Retention, frequency, RFM, CLV, loyalty behavior |
| **Inventory** | Stock turnover, DOI, OOS, dead stock, expiry risk |
| **Procurement** | Supplier spend, fill rate, OTIF, lead time |
| **RX** | Prescription volume, dispensing, doctor/product analysis |
| **Insurance** | Approval rate, rejection reasons, payer performance |
| **Workforce** | Sales per employee, labor hours, staffing efficiency |
| **Delivery** | SLA, delivery time, cost per order |
| **Customer Experience** | Complaints, resolution time, service quality |

---

## 🛠️ Technology Stack

![Technology Stack, Repository and Roadmap](docs/architecture/08_technology_repository_roadmap.png)

| Technology | Purpose |
|---|---|
| **Microsoft Fabric** | Current Lakehouse platform: OneLake, Delta and PySpark | 
| **T-SQL** | DDL, ETL/ELT, validation, analytics |
| **SSMS** | SQL Server development |
| **Python** | Data generation, validation, analytics |
| **Pandas** | Data analysis and validation |
| **Power BI** | Not started; build reports after Gold and the semantic model |
| **DAX** | Not started; measures follow the semantic model |
| **Git / GitHub** | Version control and portfolio delivery |
| **Draw.io / Architecture Diagrams** | ERD, lineage and data-flow design |
| **VS Code** | Development environment |

---

## 📂 Repository Structure

The current GitHub repository contains:

```text
pharmacy-enterprise-data-warehouse/
│
├── datasets/
│   └── placeholder
│
├── docs/
│   ├── architecture/
│   └── placeholder
│
├── scripts/
│
├── sql/
│   ├── setup/
│   ├── bronze/
│   │   ├── ddl_bronze.sql
│   │   └── placeholder
│   ├── silver/
│   └── gold/
│
├── tests/
│
├── LICENSE
└── README.md
```

The repository will expand as the completed Bronze implementation artifacts are reviewed and published, followed by Silver transformations, Gold models, tests, BI assets, and supporting documentation.

Large generated datasets are intentionally stored outside the GitHub repository.

At the current repository state, `datasets/` contains only a placeholder; the warehouse dataset itself is maintained separately.

GitHub is used for SQL, architecture, documentation, tests, scripts, and reproducibility artifacts.

> **Repository note:** `sql/bronze/ddl_bronze.sql` is present in this repository. Fabric notebooks and current Silver execution evidence remain in the Fabric workspace and are not represented as committed notebook files here. The project checkpoint above reflects observed Fabric outputs, while this repository page describes committed artifacts.

---

## 🗺️ Project Roadmap

```text
✅ Business requirements & scope
        ↓
✅ Enterprise source-system design
        ↓
✅ Synthetic RAW dataset
        ↓
✅ RAW quality & reconciliation
        ↓
✅ Metadata & data catalog
        ↓
✅ Architecture / ERD / lineage
        ↓
✅ RAW → Bronze source mapping
        ↓
✅ Bronze DDL generation
        ↓
✅ 72 Bronze tables created
        ↓
✅ Bronze loader + batch audit
        ↓
✅ RAW → Bronze ingestion
   72 / 72 tables
   60,234,021 rows
   0 failures
        ↓
▶ Silver: map and build the priority product and POS sales foundation
        ↓
⏳ Complete remaining Silver entities with approved rules and entity-level validation
        ↓
⏳ Gold star schema with explicit fact grain and conformed dimensions
        ↓
⏳ Governed semantic model and DAX measures
        ↓
⏳ Power BI report suite, usability and refresh validation
        ↓
⏳ Fabric orchestration, monitoring, optimization, Git/CI-CD and portfolio release
        ↓
⏳ Optional AI use case after core data products pass validation
```

---

## 📚 Documentation

### Visual Architecture Set

| # | Visual | Purpose |
|---:|---|---|
| **01** | Project Overview | Executive introduction, project scale and status |
| **02** | Source Systems Overview | Operational domains feeding the warehouse |
| **03** | End-to-End Architecture | RAW → Bronze → Silver → Gold → Consumption |
| **04** | RAW → Bronze Mapping | Source-aligned ingestion and technical metadata |
| **05** | Business ERD / Cross-Domain | Core business relationships across domains |
| **06** | Data Quality & Reconciliation | Validation controls and financial/operational gates |
| **07** | Planned Silver & Gold | Future conformed and dimensional architecture |
| **08** | Technology / Repository / Roadmap | Stack, delivery structure and implementation path |

Architecture visuals are stored under:

```text
docs/architecture/
```

The repository will continue to integrate engineering documentation including:

- Business ERD
- Source-to-target mapping
- Metadata and data dictionary
- Business rules
- Data-quality rules
- Reconciliation logic
- Data lineage
- Implementation documentation

---

## ⚠️ Data & Provenance Disclaimer

This is a **portfolio and learning project built on synthetic operational data**.

Some product records may use real-world brand names or standard medicine concepts to improve business realism.

Generated combinations of SKU, barcode, pack, price, transaction, prescription, insurance, customer, employee, supplier, and operational data are synthetic unless explicitly documented otherwise.

The dataset must not be interpreted as an official manufacturer, retailer, payer, or regulatory registry.

---

## 👤 Author

### Shamseldeen Ismaiil

Retail pharmacy professional with more than 10 years of operational experience, building practical expertise in:

**Data Analytics • Business Intelligence • SQL • Python • Power BI • Data Warehousing**

The project combines technical data skills with practical domain understanding across:

**Pharmacy Operations • Retail • Sales • Insurance • Inventory • Customer Service**

---

## 📄 License

This project is released under the [MIT License](LICENSE).

---

### ⭐ Project Vision

> Build a realistic, auditable, and scalable pharmacy analytics platform that demonstrates the complete journey from complex operational data to trusted business intelligence.
