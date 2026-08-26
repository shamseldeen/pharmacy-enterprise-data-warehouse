/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing
    tables if they already exist.

    Each Bronze table matches its source CSV structure 1:1 so the files can
    be loaded directly with BULK INSERT in the same style as the reference
    Data Warehouse project.

    No cleansing or business transformations are performed in Bronze.
    Transformations will be handled in the Silver Layer.

WARNING:
    Running this script drops and recreates the Bronze tables listed below.
    Existing data in these tables will be permanently deleted.
===============================================================================
*/

USE PharmacyEnterpriseDW;
GO

/* ====================================================
   ERP SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.erp_employees', 'U') IS NOT NULL
	DROP TABLE bronze.erp_employees;
CREATE TABLE bronze.erp_employees (
	employee_id NVARCHAR(50),
	employee_name NVARCHAR(150),
	job_title NVARCHAR(100),
	pharmacy_id NVARCHAR(50),
	region_id NVARCHAR(50),
	hire_date DATE,
	termination_date DATE,
	employment_status NVARCHAR(50),
	manager_employee_id NVARCHAR(50),
	data_status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_finance_account_types', 'U') IS NOT NULL
	DROP TABLE bronze.erp_finance_account_types;
CREATE TABLE bronze.erp_finance_account_types (
	account_type_id NVARCHAR(50),
	account_type NVARCHAR(100),
	statement_group NVARCHAR(100)
);

IF OBJECT_ID ('bronze.erp_job_roles', 'U') IS NOT NULL
	DROP TABLE bronze.erp_job_roles;
CREATE TABLE bronze.erp_job_roles (
	job_role_id NVARCHAR(50),
	job_title NVARCHAR(100),
	department NVARCHAR(100)
);

IF OBJECT_ID ('bronze.erp_shift_types', 'U') IS NOT NULL
	DROP TABLE bronze.erp_shift_types;
CREATE TABLE bronze.erp_shift_types (
	shift_id NVARCHAR(50),
	shift_name NVARCHAR(100),
	start_hour INT,
	end_hour INT
);

/* ====================================================
   POS SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.pos_campaigns', 'U') IS NOT NULL
	DROP TABLE bronze.pos_campaigns;
CREATE TABLE bronze.pos_campaigns (
	campaign_id NVARCHAR(50),
	campaign_name NVARCHAR(150),
	campaign_type NVARCHAR(100),
	start_date DATE,
	end_date DATE,
	budget_sar DECIMAL(18,2),
	data_status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.pos_channels', 'U') IS NOT NULL
	DROP TABLE bronze.pos_channels;
CREATE TABLE bronze.pos_channels (
	channel_id NVARCHAR(50),
	channel NVARCHAR(100)
);

IF OBJECT_ID ('bronze.pos_payment_methods', 'U') IS NOT NULL
	DROP TABLE bronze.pos_payment_methods;
CREATE TABLE bronze.pos_payment_methods (
	payment_method_id NVARCHAR(50),
	payment_method NVARCHAR(100)
);

IF OBJECT_ID ('bronze.pos_promotion_types', 'U') IS NOT NULL
	DROP TABLE bronze.pos_promotion_types;
CREATE TABLE bronze.pos_promotion_types (
	promotion_type_id NVARCHAR(50),
	promotion_type NVARCHAR(100)
);

IF OBJECT_ID ('bronze.pos_promotions', 'U') IS NOT NULL
	DROP TABLE bronze.pos_promotions;
CREATE TABLE bronze.pos_promotions (
	promotion_id NVARCHAR(50),
	promotion_name NVARCHAR(150),
	promotion_type NVARCHAR(100),
	target_scope NVARCHAR(100),
	start_date DATE,
	end_date DATE,
	discount_pct DECIMAL(9,2),
	data_status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.pos_return_reasons', 'U') IS NOT NULL
	DROP TABLE bronze.pos_return_reasons;
CREATE TABLE bronze.pos_return_reasons (
	reason_id NVARCHAR(50),
	reason NVARCHAR(150)
);

/* ====================================================
   CRM SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.crm_consent_types', 'U') IS NOT NULL
	DROP TABLE bronze.crm_consent_types;
CREATE TABLE bronze.crm_consent_types (
	consent_type_id NVARCHAR(50),
	consent_type NVARCHAR(100)
);

IF OBJECT_ID ('bronze.crm_customer_service_types', 'U') IS NOT NULL
	DROP TABLE bronze.crm_customer_service_types;
CREATE TABLE bronze.crm_customer_service_types (
	interaction_type_id NVARCHAR(50),
	interaction_type NVARCHAR(100)
);

IF OBJECT_ID ('bronze.crm_customers', 'U') IS NOT NULL
	DROP TABLE bronze.crm_customers;
CREATE TABLE bronze.crm_customers (
	customer_id NVARCHAR(50),
	customer_name NVARCHAR(150),
	gender NVARCHAR(20),
	birth_year INT,
	preferred_pharmacy_id NVARCHAR(50),
	registration_date DATE,
	segment NVARCHAR(50),
	data_status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.crm_loyalty_types', 'U') IS NOT NULL
	DROP TABLE bronze.crm_loyalty_types;
CREATE TABLE bronze.crm_loyalty_types (
	loyalty_type_id NVARCHAR(50),
	loyalty_type NVARCHAR(100),
	description NVARCHAR(250)
);

/* ====================================================
   PRESCRIPTION SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.rx_doctors', 'U') IS NOT NULL
	DROP TABLE bronze.rx_doctors;
CREATE TABLE bronze.rx_doctors (
	doctor_id NVARCHAR(50),
	doctor_name NVARCHAR(150),
	specialty NVARCHAR(100),
	city NVARCHAR(100),
	data_status NVARCHAR(50)
);

/* ====================================================
   INSURANCE SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.ins_insurance_plans', 'U') IS NOT NULL
	DROP TABLE bronze.ins_insurance_plans;
CREATE TABLE bronze.ins_insurance_plans (
	insurance_id NVARCHAR(50),
	insurance_company NVARCHAR(150),
	plan_name NVARCHAR(150),
	coverage_percent DECIMAL(9,2),
	data_status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.ins_claim_status', 'U') IS NOT NULL
	DROP TABLE bronze.ins_claim_status;
CREATE TABLE bronze.ins_claim_status (
	claim_status_id NVARCHAR(50),
	claim_status NVARCHAR(100)
);

/* ====================================================
   SUPPLY_CHAIN SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.scm_inventory_movement_types', 'U') IS NOT NULL
	DROP TABLE bronze.scm_inventory_movement_types;
CREATE TABLE bronze.scm_inventory_movement_types (
	movement_type_id NVARCHAR(50),
	movement_type NVARCHAR(100)
);

IF OBJECT_ID ('bronze.scm_supplier_contracts', 'U') IS NOT NULL
	DROP TABLE bronze.scm_supplier_contracts;
CREATE TABLE bronze.scm_supplier_contracts (
	contract_id NVARCHAR(50),
	supplier_id NVARCHAR(50),
	start_date DATE,
	end_date DATE,
	payment_terms_days INT,
	rebate_pct DECIMAL(9,2),
	service_level_pct DECIMAL(9,2),
	status NVARCHAR(50),
	data_status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.scm_suppliers', 'U') IS NOT NULL
	DROP TABLE bronze.scm_suppliers;
CREATE TABLE bronze.scm_suppliers (
	supplier_id NVARCHAR(50),
	supplier_name NVARCHAR(150),
	supplier_type NVARCHAR(100),
	payment_terms_days INT,
	data_status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.scm_warehouses', 'U') IS NOT NULL
	DROP TABLE bronze.scm_warehouses;
CREATE TABLE bronze.scm_warehouses (
	warehouse_id NVARCHAR(50),
	warehouse_name NVARCHAR(150),
	region_id NVARCHAR(50),
	warehouse_type NVARCHAR(100),
	status NVARCHAR(50),
	data_status NVARCHAR(50)
);

/* ====================================================
   DELIVERY SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.del_delivery_methods', 'U') IS NOT NULL
	DROP TABLE bronze.del_delivery_methods;
CREATE TABLE bronze.del_delivery_methods (
	delivery_method_id NVARCHAR(50),
	delivery_method NVARCHAR(100),
	sla_minutes INT
);

/* ====================================================
   REFERENCE SOURCE TABLES
   ==================================================== */

IF OBJECT_ID ('bronze.ref_branch_clusters', 'U') IS NOT NULL
	DROP TABLE bronze.ref_branch_clusters;
CREATE TABLE bronze.ref_branch_clusters (
	cluster_id NVARCHAR(50),
	cluster_name NVARCHAR(100),
	cluster_type NVARCHAR(50)
);

IF OBJECT_ID ('bronze.ref_currencies', 'U') IS NOT NULL
	DROP TABLE bronze.ref_currencies;
CREATE TABLE bronze.ref_currencies (
	currency_id NVARCHAR(50),
	currency_code NVARCHAR(10),
	currency_name NVARCHAR(100),
	is_base_currency BIT
);

IF OBJECT_ID ('bronze.ref_pharmacies', 'U') IS NOT NULL
	DROP TABLE bronze.ref_pharmacies;
CREATE TABLE bronze.ref_pharmacies (
	pharmacy_id NVARCHAR(50),
	pharmacy_name NVARCHAR(150),
	region_id NVARCHAR(50),
	region NVARCHAR(100),
	city NVARCHAR(100),
	district NVARCHAR(150),
	cluster_id NVARCHAR(50),
	pharmacy_type NVARCHAR(50),
	selling_area_sqm INT,
	latitude DECIMAL(10,6),
	longitude DECIMAL(10,6),
	opening_date DATE,
	closing_date DATE,
	status NVARCHAR(50)
);

IF OBJECT_ID ('bronze.ref_price_types', 'U') IS NOT NULL
	DROP TABLE bronze.ref_price_types;
CREATE TABLE bronze.ref_price_types (
	price_type_id NVARCHAR(50),
	price_type NVARCHAR(100)
);

IF OBJECT_ID ('bronze.ref_products', 'U') IS NOT NULL
	DROP TABLE bronze.ref_products;
CREATE TABLE bronze.ref_products (
	product_id NVARCHAR(50),
	trade_name NVARCHAR(250),
	generic_name NVARCHAR(250),
	strength NVARCHAR(100),
	strength_unit NVARCHAR(50),
	dosage_form NVARCHAR(100),
	category NVARCHAR(100),
	list_price_sar DECIMAL(18,2),
	source_type NVARCHAR(50)
);

IF OBJECT_ID ('bronze.ref_regions', 'U') IS NOT NULL
	DROP TABLE bronze.ref_regions;
CREATE TABLE bronze.ref_regions (
	region_id NVARCHAR(50),
	region_name NVARCHAR(100),
	city NVARCHAR(100)
);

IF OBJECT_ID ('bronze.ref_sfda_products', 'U') IS NOT NULL
	DROP TABLE bronze.ref_sfda_products;
CREATE TABLE bronze.ref_sfda_products (
	scientific_name NVARCHAR(250),
	trade_name NVARCHAR(250),
	strength NVARCHAR(100),
	dosage_form NVARCHAR(100),
	price_sar DECIMAL(18,2),
	source_url NVARCHAR(500),
	source_status NVARCHAR(100)
);

GO
