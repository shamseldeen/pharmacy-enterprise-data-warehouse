CREATE TABLE [gold].[dim_doctor] (
    [doctor_key]        INT            NULL,
    [doctor_id]         VARCHAR (8000) NULL,
    [full_name]         VARCHAR (8000) NULL,
    [gender]            VARCHAR (8000) NULL,
    [specialty_name]    VARCHAR (8000) NULL,
    [region_name]       VARCHAR (8000) NULL,
    [city_name]         VARCHAR (8000) NULL,
    [active_from]       DATE           NULL,
    [active_to]         DATE           NULL,
    [is_active_at_asof] BIT            NULL
);


GO