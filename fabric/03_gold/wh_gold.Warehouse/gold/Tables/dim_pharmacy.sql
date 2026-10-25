CREATE TABLE [gold].[dim_pharmacy] (
    [pharmacy_key]             INT            NULL,
    [pharmacy_id]              VARCHAR (8000) NULL,
    [pharmacy_name]            VARCHAR (8000) NULL,
    [branch_type]              VARCHAR (8000) NULL,
    [region_id]                VARCHAR (8000) NULL,
    [region_name]              VARCHAR (8000) NULL,
    [city_name]                VARCHAR (8000) NULL,
    [district_name]            VARCHAR (8000) NULL,
    [map_location]             VARCHAR (8000) NULL,
    [opening_date]             DATE           NULL,
    [opening_year]             INT            NULL,
    [weekly_operating_hours]   DECIMAL (6, 1) NULL,
    [is_24h]                   BIT            NULL,
    [first_activity_date]      DATE           NULL,
    [effective_opening_date]   DATE           NULL,
    [is_opening_date_conflict] BIT            NULL,
    [is_age_lower_bound]       BIT            NULL
);


GO