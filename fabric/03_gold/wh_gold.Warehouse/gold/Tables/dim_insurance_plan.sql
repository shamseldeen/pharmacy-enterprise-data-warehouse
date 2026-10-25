CREATE TABLE [gold].[dim_insurance_plan] (
    [plan_key]                 INT             NULL,
    [insurance_plan_id]        VARCHAR (8000)  NULL,
    [payer_name]               VARCHAR (8000)  NULL,
    [plan_name]                VARCHAR (8000)  NULL,
    [plan_tier]                VARCHAR (8000)  NULL,
    [network_type]             VARCHAR (8000)  NULL,
    [default_coverage_pct]     DECIMAL (5, 4)  NULL,
    [default_copay_sar]        DECIMAL (18, 2) NULL,
    [annual_benefit_limit_sar] DECIMAL (18, 2) NULL,
    [covers_prescriptions]     BIT             NULL
);


GO