CREATE TABLE [gold].[fact_purchase_line] (
    [po_line_id]             VARCHAR (8000)  NULL,
    [purchase_order_id]      VARCHAR (8000)  NULL,
    [order_date_key]         INT             NULL,
    [order_date]             DATE            NULL,
    [expected_delivery_date] DATE            NULL,
    [receipt_date]           DATE            NULL,
    [order_status]           VARCHAR (8000)  NULL,
    [supplier_key]           INT             NULL,
    [product_key]            INT             NULL,
    [buyer_employee_key]     INT             NULL,
    [warehouse_id]           VARCHAR (8000)  NULL,
    [warehouse_name]         VARCHAR (8000)  NULL,
    [warehouse_region]       VARCHAR (8000)  NULL,
    [ordered_qty]            INT             NULL,
    [unit_cost_sar]          DECIMAL (18, 2) NULL,
    [gross_cost_sar]         DECIMAL (18, 2) NULL,
    [discount_sar]           DECIMAL (18, 2) NULL,
    [tax_sar]                DECIMAL (18, 2) NULL,
    [net_cost_sar]           DECIMAL (18, 2) NULL,
    [received_qty]           INT             NULL,
    [rejected_qty]           INT             NULL,
    [accepted_qty]           INT             NULL,
    [is_received]            BIT             NULL,
    [is_on_time]             BIT             NULL,
    [is_in_full]             BIT             NULL,
    [is_otif]                BIT             NULL,
    [lead_time_days]         INT             NULL,
    [delay_days]             INT             NULL
);


GO