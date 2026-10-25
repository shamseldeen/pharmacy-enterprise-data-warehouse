CREATE TABLE [gold].[fact_sales_line] (
    [sales_line_id]       VARCHAR (8000)  NULL,
    [transaction_id]      VARCHAR (8000)  NULL,
    [line_number]         INT             NULL,
    [date_key]            INT             NULL,
    [transaction_ts]      DATETIME2 (6)   NULL,
    [pharmacy_key]        INT             NULL,
    [product_key]         INT             NULL,
    [customer_key]        INT             NULL,
    [employee_key]        INT             NULL,
    [channel_id]          VARCHAR (8000)  NULL,
    [quantity]            INT             NULL,
    [unit_price_sar]      DECIMAL (18, 2) NULL,
    [gross_amount_sar]    DECIMAL (18, 2) NULL,
    [discount_amount_sar] DECIMAL (18, 2) NULL,
    [net_before_vat_sar]  DECIMAL (18, 2) NULL,
    [vat_rate]            DECIMAL (5, 4)  NULL,
    [vat_amount_sar]      DECIMAL (18, 2) NULL,
    [net_amount_sar]      DECIMAL (18, 2) NULL,
    [est_cost_sar]        DECIMAL (18, 2) NULL,
    [est_margin_sar]      DECIMAL (18, 2) NULL,
    [is_sold_below_cost]  BIT             NULL
);


GO