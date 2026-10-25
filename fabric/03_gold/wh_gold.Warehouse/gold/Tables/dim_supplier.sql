CREATE TABLE [gold].[dim_supplier] (
    [supplier_key]            INT             NULL,
    [supplier_id]             VARCHAR (8000)  NULL,
    [supplier_name]           VARCHAR (8000)  NULL,
    [supplier_type_name]      VARCHAR (8000)  NULL,
    [region_name]             VARCHAR (8000)  NULL,
    [city_name]               VARCHAR (8000)  NULL,
    [standard_lead_time_days] INT             NULL,
    [is_cold_chain_capable]   BIT             NULL,
    [minimum_order_value_sar] DECIMAL (18, 2) NULL,
    [supplier_status]         VARCHAR (8000)  NULL
);


GO