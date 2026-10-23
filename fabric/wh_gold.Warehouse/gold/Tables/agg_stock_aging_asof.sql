CREATE TABLE [gold].[agg_stock_aging_asof] (
    [date_key]                     INT             NULL,
    [snap_month]                   DATE            NULL,
    [snap_date]                    DATE            NULL,
    [pharmacy_key]                 INT             NULL,
    [product_key]                  INT             NULL,
    [supplier_key]                 INT             NULL,
    [closing_stock]                INT             NULL,
    [stock_value_sar]              DECIMAL (18, 2) NULL,
    [last_sale_date]               DATE            NULL,
    [days_since_last_sale]         INT             NULL,
    [chain_qty_180d]               BIGINT          NULL,
    [other_pharmacies_lost_demand] INT             NULL,
    [other_lost_qty]               BIGINT          NULL,
    [age_bucket]                   VARCHAR (17)    NOT NULL,
    [is_dead_stock]                BIT             NULL,
    [is_rotation_candidate]        BIT             NULL,
    [is_no_demand_anywhere]        BIT             NULL,
    [recommended_action]           VARCHAR (15)    NOT NULL
);


GO