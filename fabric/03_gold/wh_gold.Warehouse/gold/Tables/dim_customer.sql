CREATE TABLE [gold].[dim_customer] (
    [customer_key]              INT            NULL,
    [customer_id]               VARCHAR (8000) NULL,
    [full_name]                 VARCHAR (8000) NULL,
    [gender]                    VARCHAR (8000) NULL,
    [nationality_group]         VARCHAR (8000) NULL,
    [region_name]               VARCHAR (8000) NULL,
    [city_name]                 VARCHAR (8000) NULL,
    [birth_date]                DATE           NULL,
    [age_at_asof]               INT            NULL,
    [age_band]                  VARCHAR (7)    NOT NULL,
    [registration_date]         DATE           NULL,
    [tenure_years]              INT            NULL,
    [preferred_contact_channel] VARCHAR (8000) NULL,
    [is_loyalty_enrolled]       BIT            NULL,
    [loyalty_tier]              VARCHAR (8000) NULL,
    [loyalty_status]            VARCHAR (8000) NULL,
    [has_insurance_ever]        BIT            NULL,
    [is_insured_at_asof]        BIT            NULL,
    [can_contact_sms]           BIT            NULL,
    [can_contact_email]         BIT            NULL,
    [can_contact_app]           BIT            NULL,
    [is_marketable]             BIT            NULL
);


GO