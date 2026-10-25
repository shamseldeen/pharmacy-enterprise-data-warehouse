CREATE TABLE [gold].[dim_product] (
    [product_key]              INT             NULL,
    [product_id]               VARCHAR (8000)  NULL,
    [trade_name]               VARCHAR (8000)  NULL,
    [generic_name]             VARCHAR (8000)  NULL,
    [strength]                 VARCHAR (8000)  NULL,
    [strength_unit]            VARCHAR (8000)  NULL,
    [dosage_form]              VARCHAR (8000)  NULL,
    [category]                 VARCHAR (8000)  NULL,
    [list_price_sar]           DECIMAL (18, 2) NULL,
    [est_unit_cost_sar]        DECIMAL (18, 2) NULL,
    [est_cost_ratio]           DECIMAL (9, 4)  NULL,
    [est_margin_pct]           DECIMAL (9, 4)  NULL,
    [batch_count]              INT             NULL,
    [has_recalled_batch]       BIT             NULL,
    [needs_cold_chain]         BIT             NULL,
    [is_cost_imputed]          BIT             NULL,
    [is_cost_ratio_suspicious] BIT             NULL
);


GO