CREATE TABLE [gold].[dim_date] (
    [date_key]            INT         NULL,
    [full_date]           DATE        NULL,
    [year_number]         INT         NULL,
    [quarter_number]      INT         NULL,
    [month_number]        INT         NULL,
    [month_name]          VARCHAR (3) NOT NULL,
    [month_start]         DATE        NULL,
    [day_of_month]        INT         NULL,
    [day_of_week]         INT         NULL,
    [day_name]            VARCHAR (3) NOT NULL,
    [is_weekend]          BIT         NULL,
    [is_ramadan]          BIT         NULL,
    [is_eid_al_fitr]      BIT         NULL,
    [is_eid_al_adha]      BIT         NULL,
    [is_school_holiday]   BIT         NULL,
    [is_national_holiday] BIT         NULL
);


GO