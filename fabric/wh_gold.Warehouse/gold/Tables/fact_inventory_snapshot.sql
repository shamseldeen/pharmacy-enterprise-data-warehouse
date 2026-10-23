CREATE TABLE [gold].[fact_inventory_snapshot] (
    [date_key]                 INT             NULL,
    [month_start]              DATE            NULL,
    [pharmacy_key]             INT             NULL,
    [product_key]              INT             NULL,
    [opening_stock]            INT             NULL,
    [net_movement_qty]         INT             NULL,
    [closing_stock]            INT             NULL,
    [unfulfilled_qty]          INT             NULL,
    [is_stockout]              BIT             NULL,
    [has_lost_demand]          BIT             NULL,
    [stock_value_at_cost_sar]  DECIMAL (18, 2) NULL,
    [lost_sales_value_est_sar] DECIMAL (18, 2) NULL
);


GO