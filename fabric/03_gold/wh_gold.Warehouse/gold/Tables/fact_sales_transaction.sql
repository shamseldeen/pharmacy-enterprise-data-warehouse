CREATE TABLE [gold].[fact_sales_transaction] (
    [transaction_id]      VARCHAR (8000)  NULL,
    [receipt_number]      VARCHAR (8000)  NULL,
    [date_key]            INT             NULL,
    [transaction_ts]      DATETIME2 (6)   NULL,
    [hour_of_day]         INT             NULL,
    [pharmacy_key]        INT             NULL,
    [customer_key]        INT             NULL,
    [employee_key]        INT             NULL,
    [channel_id]          VARCHAR (8000)  NULL,
    [transaction_status]  VARCHAR (8000)  NULL,
    [is_completed]        BIT             NULL,
    [is_cancelled]        BIT             NULL,
    [is_voided]           BIT             NULL,
    [line_count]          INT             NULL,
    [items_qty]           INT             NULL,
    [gross_amount_sar]    DECIMAL (38, 2) NULL,
    [discount_amount_sar] DECIMAL (38, 2) NULL,
    [net_before_vat_sar]  DECIMAL (38, 2) NULL,
    [vat_amount_sar]      DECIMAL (38, 2) NULL,
    [net_amount_sar]      DECIMAL (38, 2) NULL,
    [est_margin_sar]      DECIMAL (38, 2) NULL,
    [payment_method_id]   VARCHAR (8000)  NULL,
    [paid_amount_sar]     DECIMAL (38, 2) NULL,
    [return_lines]        INT             NULL,
    [refund_amount_sar]   DECIMAL (38, 2) NULL,
    [has_return]          BIT             NULL
);


GO