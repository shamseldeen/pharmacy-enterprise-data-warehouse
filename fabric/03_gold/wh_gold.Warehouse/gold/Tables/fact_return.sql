CREATE TABLE [gold].[fact_return] (
    [return_id]               VARCHAR (8000)  NULL,
    [original_transaction_id] VARCHAR (8000)  NULL,
    [original_sales_line_id]  VARCHAR (8000)  NULL,
    [return_date_key]         INT             NULL,
    [return_ts]               DATETIME2 (6)   NULL,
    [sale_date_key]           INT             NULL,
    [days_to_return]          INT             NULL,
    [pharmacy_key]            INT             NULL,
    [customer_key]            INT             NULL,
    [employee_key]            INT             NULL,
    [product_key]             INT             NULL,
    [return_reason]           VARCHAR (8000)  NULL,
    [return_quantity]         INT             NULL,
    [refund_amount_sar]       DECIMAL (18, 2) NULL,
    [sold_quantity]           INT             NULL,
    [sold_net_amount_sar]     DECIMAL (18, 2) NULL,
    [is_refund_above_sale]    BIT             NULL,
    [is_qty_above_sale]       BIT             NULL
);


GO