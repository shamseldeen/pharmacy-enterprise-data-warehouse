CREATE TABLE [gold].[dim_employee] (
    [employee_key]      INT            NULL,
    [employee_id]       VARCHAR (8000) NULL,
    [full_name]         VARCHAR (8000) NULL,
    [gender]            VARCHAR (8000) NULL,
    [nationality_group] VARCHAR (8000) NULL,
    [job_role]          VARCHAR (8000) NULL,
    [is_pharmacist]     BIT            NULL,
    [can_dispense]      BIT            NULL,
    [hire_date]         DATE           NULL,
    [termination_date]  DATE           NULL,
    [is_active_at_asof] BIT            NULL,
    [tenure_years]      INT            NULL
);


GO