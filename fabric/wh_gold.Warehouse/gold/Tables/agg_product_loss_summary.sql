CREATE TABLE [gold].[agg_product_loss_summary] (
    [product_key]              INT             NULL,
    [product_id]               VARCHAR (8000)  NULL,
    [trade_name]               VARCHAR (8000)  NULL,
    [category]                 VARCHAR (8000)  NULL,
    [is_cost_imputed]          BIT             NULL,
    [is_cost_ratio_suspicious] BIT             NULL,
    [total_lines]              BIGINT          NULL,
    [loss_lines]               INT             NULL,
    [loss_lines_pct]           DECIMAL (5, 2)  NULL,
    [total_qty]                BIGINT          NULL,
    [loss_qty]                 BIGINT          NULL,
    [revenue_sar]              DECIMAL (18, 2) NULL,
    [margin_sar]               DECIMAL (18, 2) NULL,
    [loss_sar]                 DECIMAL (18, 2) NULL,
    [first_loss_date]          DATE            NULL,
    [last_loss_date]           DATE            NULL,
    [loss_span_days]           INT             NULL,
    [loss_months]              INT             NULL,
    [loss_pharmacies]          INT             NULL,
    [closing_stock_asof]       INT             NULL,
    [stock_value_asof_sar]     DECIMAL (18, 2) NULL
);


GO