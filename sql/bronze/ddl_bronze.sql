/*
==================================================================================================
DDL Script: Create Bronze Tables
==================================================================================================
Script Purpose:
    This script creates the Bronze Layer tables for the Pharmacy Enterprise
    Data Warehouse, dropping existing Bronze tables if they already exist.

    The Bronze Layer preserves source-system data in its original or near-original
    structure and adds only technical ingestion metadata for audit and lineage.

Source Systems:
    ERP  = Enterprise Resource Planning / HR
    POS  = Point of Sale
    CRM  = Customer Relationship Management / Loyalty / CX
    RX   = Prescription System
    INS  = Insurance / Claims
    SCM  = Supply Chain / Procurement / Inventory
    DEL  = Delivery Operations
    REF  = Reference / Master Data

Bronze Layer Principles:
    - Preserve source column names and structure.
    - No business transformations.
    - No dimensional modeling.
    - No business PK/FK constraints in Bronze.
    - Add ingestion metadata only.
    - Cleaning, standardization, deduplication and conformance happen in Silver.

WARNING:
    Running this script will drop and recreate the Bronze tables defined below.
    Any data already loaded into those tables will be permanently deleted.
==================================================================================================
*/

USE PharmacyEnterpriseDW;
GO


/* ================================================================================================
   ERP / HR SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.erp_employees', 'U') IS NOT NULL
    DROP TABLE bronze.erp_employees;
CREATE TABLE bronze.erp_employees (
    employee_id             NVARCHAR(50),
    employee_name           NVARCHAR(150),
    job_title               NVARCHAR(100),
    pharmacy_id             NVARCHAR(50),
    region_id               NVARCHAR(50),
    hire_date               DATE,
    termination_date        DATE,
    employment_status       NVARCHAR(50),
    manager_employee_id     NVARCHAR(50),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.erp_finance_account_types', 'U') IS NOT NULL
    DROP TABLE bronze.erp_finance_account_types;
CREATE TABLE bronze.erp_finance_account_types (
    account_type_id         NVARCHAR(50),
    account_type            NVARCHAR(100),
    statement_group         NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.erp_job_roles', 'U') IS NOT NULL
    DROP TABLE bronze.erp_job_roles;
CREATE TABLE bronze.erp_job_roles (
    job_role_id             NVARCHAR(50),
    job_title               NVARCHAR(100),
    department              NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.erp_shift_types', 'U') IS NOT NULL
    DROP TABLE bronze.erp_shift_types;
CREATE TABLE bronze.erp_shift_types (
    shift_id                NVARCHAR(50),
    shift_name              NVARCHAR(100),
    start_hour              INT,
    end_hour                INT,
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* ================================================================================================
   POS SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.pos_campaigns', 'U') IS NOT NULL
    DROP TABLE bronze.pos_campaigns;
CREATE TABLE bronze.pos_campaigns (
    campaign_id             NVARCHAR(50),
    campaign_name           NVARCHAR(150),
    campaign_type           NVARCHAR(100),
    start_date              DATE,
    end_date                DATE,
    budget_sar              DECIMAL(18,2),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_channels', 'U') IS NOT NULL
    DROP TABLE bronze.pos_channels;
CREATE TABLE bronze.pos_channels (
    channel_id              NVARCHAR(50),
    channel                 NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_payment_methods', 'U') IS NOT NULL
    DROP TABLE bronze.pos_payment_methods;
CREATE TABLE bronze.pos_payment_methods (
    payment_method_id       NVARCHAR(50),
    payment_method          NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_promotion_types', 'U') IS NOT NULL
    DROP TABLE bronze.pos_promotion_types;
CREATE TABLE bronze.pos_promotion_types (
    promotion_type_id       NVARCHAR(50),
    promotion_type          NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_promotions', 'U') IS NOT NULL
    DROP TABLE bronze.pos_promotions;
CREATE TABLE bronze.pos_promotions (
    promotion_id            NVARCHAR(50),
    promotion_name          NVARCHAR(150),
    promotion_type          NVARCHAR(100),
    target_scope            NVARCHAR(100),
    start_date              DATE,
    end_date                DATE,
    discount_pct            DECIMAL(9,2),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_return_reasons', 'U') IS NOT NULL
    DROP TABLE bronze.pos_return_reasons;
CREATE TABLE bronze.pos_return_reasons (
    reason_id               NVARCHAR(50),
    reason                  NVARCHAR(150),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* High-volume POS transactional feeds generated/loaded locally */

IF OBJECT_ID ('bronze.pos_sales_transaction', 'U') IS NOT NULL
    DROP TABLE bronze.pos_sales_transaction;
CREATE TABLE bronze.pos_sales_transaction (
    transaction_id          NVARCHAR(50),
    transaction_ts          DATETIME2,
    pharmacy_id             NVARCHAR(50),
    customer_id             NVARCHAR(50),
    employee_id             NVARCHAR(50),
    payment_method_id       NVARCHAR(50),
    channel_id              NVARCHAR(50),
    currency_id             NVARCHAR(50),
    line_count              INT,
    item_count              INT,
    gross_amount_sar        DECIMAL(18,2),
    discount_amount_sar     DECIMAL(18,2),
    tax_amount_sar          DECIMAL(18,2),
    net_amount_sar          DECIMAL(18,2),
    cost_amount_sar         DECIMAL(18,2),
    gross_profit_sar        DECIMAL(18,2),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_sales_line', 'U') IS NOT NULL
    DROP TABLE bronze.pos_sales_line;
CREATE TABLE bronze.pos_sales_line (
    sales_line_id           BIGINT,
    transaction_id          NVARCHAR(50),
    product_id              NVARCHAR(50),
    batch_id                NVARCHAR(50),
    promotion_id            NVARCHAR(50),
    quantity                INT,
    unit_price_sar          DECIMAL(18,2),
    gross_sales_sar         DECIMAL(18,2),
    discount_sar            DECIMAL(18,2),
    net_sales_sar           DECIMAL(18,2),
    unit_cost_sar           DECIMAL(18,2),
    cost_sar                DECIMAL(18,2),
    gross_profit_sar        DECIMAL(18,2),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_returns', 'U') IS NOT NULL
    DROP TABLE bronze.pos_returns;
CREATE TABLE bronze.pos_returns (
    return_id               NVARCHAR(50),
    original_sales_line_id  BIGINT,
    return_ts               DATETIME2,
    quantity                INT,
    refund_amount_sar       DECIMAL(18,2),
    return_reason_id        NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.pos_promotion_redemption', 'U') IS NOT NULL
    DROP TABLE bronze.pos_promotion_redemption;
CREATE TABLE bronze.pos_promotion_redemption (
    redemption_id           NVARCHAR(50),
    promotion_id            NVARCHAR(50),
    sales_line_id           BIGINT,
    transaction_id          NVARCHAR(50),
    discount_sar            DECIMAL(18,2),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* ================================================================================================
   CRM / LOYALTY / CUSTOMER EXPERIENCE SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.crm_consent_types', 'U') IS NOT NULL
    DROP TABLE bronze.crm_consent_types;
CREATE TABLE bronze.crm_consent_types (
    consent_type_id         NVARCHAR(50),
    consent_type            NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.crm_customer_service_types', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_service_types;
CREATE TABLE bronze.crm_customer_service_types (
    interaction_type_id     NVARCHAR(50),
    interaction_type        NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.crm_customers', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customers;
CREATE TABLE bronze.crm_customers (
    customer_id             NVARCHAR(50),
    customer_name           NVARCHAR(150),
    gender                  NVARCHAR(20),
    birth_year              INT,
    preferred_pharmacy_id   NVARCHAR(50),
    registration_date       DATE,
    segment                 NVARCHAR(50),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.crm_loyalty_types', 'U') IS NOT NULL
    DROP TABLE bronze.crm_loyalty_types;
CREATE TABLE bronze.crm_loyalty_types (
    loyalty_type_id         NVARCHAR(50),
    loyalty_type            NVARCHAR(100),
    description             NVARCHAR(250),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* High-volume CRM event feeds generated/loaded locally */

IF OBJECT_ID ('bronze.crm_loyalty_transactions', 'U') IS NOT NULL
    DROP TABLE bronze.crm_loyalty_transactions;
CREATE TABLE bronze.crm_loyalty_transactions (
    loyalty_txn_id          NVARCHAR(50),
    customer_id             NVARCHAR(50),
    transaction_id          NVARCHAR(50),
    txn_date                DATE,
    loyalty_type_id         NVARCHAR(50),
    points                  INT,
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.crm_customer_service', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_service;
CREATE TABLE bronze.crm_customer_service (
    interaction_id          NVARCHAR(50),
    customer_id             NVARCHAR(50),
    pharmacy_id             NVARCHAR(50),
    employee_id             NVARCHAR(50),
    interaction_date        DATETIME2,
    issue_type_id           NVARCHAR(50),
    resolution_minutes      INT,
    resolved_first_contact  BIT,
    satisfaction_score      INT,
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* ================================================================================================
   PRESCRIPTION / RX SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.rx_doctors', 'U') IS NOT NULL
    DROP TABLE bronze.rx_doctors;
CREATE TABLE bronze.rx_doctors (
    doctor_id               NVARCHAR(50),
    doctor_name             NVARCHAR(150),
    specialty               NVARCHAR(100),
    city                    NVARCHAR(100),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* High-volume RX feeds generated/loaded locally */

IF OBJECT_ID ('bronze.rx_prescriptions', 'U') IS NOT NULL
    DROP TABLE bronze.rx_prescriptions;
CREATE TABLE bronze.rx_prescriptions (
    prescription_id         NVARCHAR(50),
    prescription_date       DATE,
    customer_id             NVARCHAR(50),
    doctor_id               NVARCHAR(50),
    pharmacy_id             NVARCHAR(50),
    employee_id             NVARCHAR(50),
    insurance_id            NVARCHAR(50),
    prescription_status     NVARCHAR(50),
    items_count             INT,
    prescription_value_sar  DECIMAL(18,2),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.rx_prescription_items', 'U') IS NOT NULL
    DROP TABLE bronze.rx_prescription_items;
CREATE TABLE bronze.rx_prescription_items (
    prescription_item_id    NVARCHAR(50),
    prescription_id         NVARCHAR(50),
    product_id              NVARCHAR(50),
    quantity                INT,
    dosage_text             NVARCHAR(250),
    frequency_text          NVARCHAR(100),
    duration_days           INT,
    refill_number           INT,
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* ================================================================================================
   INSURANCE / CLAIMS SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.ins_insurance_plans', 'U') IS NOT NULL
    DROP TABLE bronze.ins_insurance_plans;
CREATE TABLE bronze.ins_insurance_plans (
    insurance_id            NVARCHAR(50),
    insurance_company       NVARCHAR(150),
    plan_name               NVARCHAR(150),
    coverage_percent        DECIMAL(9,2),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ins_claim_status', 'U') IS NOT NULL
    DROP TABLE bronze.ins_claim_status;
CREATE TABLE bronze.ins_claim_status (
    claim_status_id         NVARCHAR(50),
    claim_status            NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* High-volume claim feeds generated/loaded locally */

IF OBJECT_ID ('bronze.ins_claims', 'U') IS NOT NULL
    DROP TABLE bronze.ins_claims;
CREATE TABLE bronze.ins_claims (
    claim_id                NVARCHAR(50),
    prescription_id         NVARCHAR(50),
    insurance_id            NVARCHAR(50),
    claim_date              DATE,
    claimed_amount_sar      DECIMAL(18,2),
    approved_amount_sar     DECIMAL(18,2),
    rejected_amount_sar     DECIMAL(18,2),
    customer_copay_sar      DECIMAL(18,2),
    claim_status_id         NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ins_claim_lines', 'U') IS NOT NULL
    DROP TABLE bronze.ins_claim_lines;
CREATE TABLE bronze.ins_claim_lines (
    claim_line_id           NVARCHAR(50),
    claim_id                NVARCHAR(50),
    prescription_item_id    NVARCHAR(50),
    product_id              NVARCHAR(50),
    claimed_amount_sar      DECIMAL(18,2),
    approved_amount_sar     DECIMAL(18,2),
    rejected_amount_sar     DECIMAL(18,2),
    rejection_reason        NVARCHAR(250),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* ================================================================================================
   SUPPLY CHAIN / PROCUREMENT / INVENTORY SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.scm_inventory_movement_types', 'U') IS NOT NULL
    DROP TABLE bronze.scm_inventory_movement_types;
CREATE TABLE bronze.scm_inventory_movement_types (
    movement_type_id        NVARCHAR(50),
    movement_type           NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_supplier_contracts', 'U') IS NOT NULL
    DROP TABLE bronze.scm_supplier_contracts;
CREATE TABLE bronze.scm_supplier_contracts (
    contract_id             NVARCHAR(50),
    supplier_id             NVARCHAR(50),
    start_date              DATE,
    end_date                DATE,
    payment_terms_days      INT,
    rebate_pct              DECIMAL(9,2),
    service_level_pct       DECIMAL(9,2),
    status                  NVARCHAR(50),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_suppliers', 'U') IS NOT NULL
    DROP TABLE bronze.scm_suppliers;
CREATE TABLE bronze.scm_suppliers (
    supplier_id             NVARCHAR(50),
    supplier_name           NVARCHAR(150),
    supplier_type           NVARCHAR(100),
    payment_terms_days      INT,
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_warehouses', 'U') IS NOT NULL
    DROP TABLE bronze.scm_warehouses;
CREATE TABLE bronze.scm_warehouses (
    warehouse_id            NVARCHAR(50),
    warehouse_name          NVARCHAR(150),
    region_id               NVARCHAR(50),
    warehouse_type          NVARCHAR(100),
    status                  NVARCHAR(50),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* High-volume supply-chain feeds generated/loaded locally */

IF OBJECT_ID ('bronze.scm_product_batches', 'U') IS NOT NULL
    DROP TABLE bronze.scm_product_batches;
CREATE TABLE bronze.scm_product_batches (
    batch_id                NVARCHAR(50),
    product_id              NVARCHAR(50),
    supplier_id             NVARCHAR(50),
    manufacture_date        DATE,
    expiry_date             DATE,
    unit_cost_sar           DECIMAL(18,2),
    temperature_class       NVARCHAR(50),
    recall_flag             BIT,
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_purchase_orders', 'U') IS NOT NULL
    DROP TABLE bronze.scm_purchase_orders;
CREATE TABLE bronze.scm_purchase_orders (
    purchase_order_id       NVARCHAR(50),
    order_date              DATE,
    supplier_id             NVARCHAR(50),
    warehouse_id            NVARCHAR(50),
    employee_id             NVARCHAR(50),
    status                  NVARCHAR(50),
    expected_delivery_date  DATE,
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_purchase_order_lines', 'U') IS NOT NULL
    DROP TABLE bronze.scm_purchase_order_lines;
CREATE TABLE bronze.scm_purchase_order_lines (
    po_line_id              NVARCHAR(50),
    purchase_order_id       NVARCHAR(50),
    product_id              NVARCHAR(50),
    ordered_qty             INT,
    unit_cost_sar           DECIMAL(18,2),
    gross_cost_sar          DECIMAL(18,2),
    discount_sar            DECIMAL(18,2),
    tax_sar                 DECIMAL(18,2),
    net_cost_sar            DECIMAL(18,2),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_purchase_receipts', 'U') IS NOT NULL
    DROP TABLE bronze.scm_purchase_receipts;
CREATE TABLE bronze.scm_purchase_receipts (
    receipt_id              NVARCHAR(50),
    po_line_id              NVARCHAR(50),
    batch_id                NVARCHAR(50),
    receipt_date            DATE,
    received_qty            INT,
    unit_cost_sar           DECIMAL(18,2),
    on_time_flag            BIT,
    in_full_flag            BIT,
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_inventory_movements', 'U') IS NOT NULL
    DROP TABLE bronze.scm_inventory_movements;
CREATE TABLE bronze.scm_inventory_movements (
    movement_id             NVARCHAR(50),
    movement_ts             DATETIME2,
    pharmacy_id             NVARCHAR(50),
    warehouse_id            NVARCHAR(50),
    product_id              NVARCHAR(50),
    batch_id                NVARCHAR(50),
    supplier_id             NVARCHAR(50),
    movement_type_id        NVARCHAR(50),
    reference_number        NVARCHAR(100),
    reference_line_id       NVARCHAR(100),
    quantity                INT,
    unit_cost_sar           DECIMAL(18,2),
    total_cost_sar          DECIMAL(18,2),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_inventory_snapshot', 'U') IS NOT NULL
    DROP TABLE bronze.scm_inventory_snapshot;
CREATE TABLE bronze.scm_inventory_snapshot (
    snapshot_id             NVARCHAR(50),
    snapshot_date           DATE,
    pharmacy_id             NVARCHAR(50),
    product_id              NVARCHAR(50),
    batch_id                NVARCHAR(50),
    opening_qty             INT,
    received_qty            INT,
    sold_qty                INT,
    returned_qty            INT,
    adjustment_qty          INT,
    damaged_qty             INT,
    expired_qty             INT,
    closing_qty             INT,
    reserved_qty            INT,
    unit_cost_sar           DECIMAL(18,2),
    inventory_value_sar     DECIMAL(18,2),
    oos_flag                BIT,
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.scm_stock_transfers', 'U') IS NOT NULL
    DROP TABLE bronze.scm_stock_transfers;
CREATE TABLE bronze.scm_stock_transfers (
    transfer_id             NVARCHAR(50),
    transfer_date           DATE,
    source_pharmacy_id      NVARCHAR(50),
    destination_pharmacy_id NVARCHAR(50),
    product_id              NVARCHAR(50),
    batch_id                NVARCHAR(50),
    quantity                INT,
    unit_cost_sar           DECIMAL(18,2),
    transfer_value_sar      DECIMAL(18,2),
    status                  NVARCHAR(50),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* ================================================================================================
   DELIVERY SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.del_delivery_methods', 'U') IS NOT NULL
    DROP TABLE bronze.del_delivery_methods;
CREATE TABLE bronze.del_delivery_methods (
    delivery_method_id      NVARCHAR(50),
    delivery_method         NVARCHAR(100),
    sla_minutes             INT,
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* High-volume delivery feed generated/loaded locally */

IF OBJECT_ID ('bronze.del_delivery_orders', 'U') IS NOT NULL
    DROP TABLE bronze.del_delivery_orders;
CREATE TABLE bronze.del_delivery_orders (
    delivery_order_id       NVARCHAR(50),
    transaction_id          NVARCHAR(50),
    pharmacy_id             NVARCHAR(50),
    customer_id             NVARCHAR(50),
    delivery_method_id      NVARCHAR(50),
    order_ts                DATETIME2,
    promised_delivery_ts    DATETIME2,
    dispatch_ts             DATETIME2,
    delivered_ts            DATETIME2,
    delivery_status         NVARCHAR(50),
    delivery_cost_sar       DECIMAL(18,2),
    data_status             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


/* ================================================================================================
   REFERENCE / MASTER DATA SOURCE
   ================================================================================================ */

IF OBJECT_ID ('bronze.ref_branch_clusters', 'U') IS NOT NULL
    DROP TABLE bronze.ref_branch_clusters;
CREATE TABLE bronze.ref_branch_clusters (
    cluster_id              NVARCHAR(50),
    cluster_name            NVARCHAR(100),
    cluster_type            NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ref_currencies', 'U') IS NOT NULL
    DROP TABLE bronze.ref_currencies;
CREATE TABLE bronze.ref_currencies (
    currency_id             NVARCHAR(50),
    currency_code           NVARCHAR(10),
    currency_name           NVARCHAR(100),
    is_base_currency        BIT,
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ref_pharmacies', 'U') IS NOT NULL
    DROP TABLE bronze.ref_pharmacies;
CREATE TABLE bronze.ref_pharmacies (
    pharmacy_id             NVARCHAR(50),
    pharmacy_name           NVARCHAR(150),
    region_id               NVARCHAR(50),
    region                  NVARCHAR(100),
    city                    NVARCHAR(100),
    district                NVARCHAR(150),
    cluster_id              NVARCHAR(50),
    pharmacy_type           NVARCHAR(50),
    selling_area_sqm        INT,
    latitude                DECIMAL(10,6),
    longitude               DECIMAL(10,6),
    opening_date            DATE,
    closing_date            DATE,
    status                  NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ref_price_types', 'U') IS NOT NULL
    DROP TABLE bronze.ref_price_types;
CREATE TABLE bronze.ref_price_types (
    price_type_id           NVARCHAR(50),
    price_type              NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ref_products', 'U') IS NOT NULL
    DROP TABLE bronze.ref_products;
CREATE TABLE bronze.ref_products (
    product_id              NVARCHAR(50),
    trade_name              NVARCHAR(250),
    generic_name            NVARCHAR(250),
    strength                NVARCHAR(100),
    strength_unit           NVARCHAR(50),
    dosage_form             NVARCHAR(100),
    category                NVARCHAR(100),
    list_price_sar          DECIMAL(18,2),
    source_type             NVARCHAR(50),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ref_regions', 'U') IS NOT NULL
    DROP TABLE bronze.ref_regions;
CREATE TABLE bronze.ref_regions (
    region_id               NVARCHAR(50),
    region_name             NVARCHAR(100),
    city                    NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);


IF OBJECT_ID ('bronze.ref_sfda_products', 'U') IS NOT NULL
    DROP TABLE bronze.ref_sfda_products;
CREATE TABLE bronze.ref_sfda_products (
    scientific_name         NVARCHAR(250),
    trade_name              NVARCHAR(250),
    strength                NVARCHAR(100),
    dosage_form             NVARCHAR(100),
    price_sar               DECIMAL(18,2),
    source_url              NVARCHAR(500),
    source_status           NVARCHAR(100),
    _source_file            NVARCHAR(260),
    _ingestion_ts           DATETIME2 DEFAULT SYSDATETIME(),
    _batch_id               NVARCHAR(100),
    _row_hash               VARBINARY(32)
);

GO
