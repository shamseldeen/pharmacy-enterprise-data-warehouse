CREATE TABLE [gold].[fact_timekeeping] (
    [timekeeping_id]   VARCHAR (8000) NULL,
    [date_key]         INT            NULL,
    [accounting_month] DATE           NULL,
    [employee_key]     INT            NULL,
    [pharmacy_key]     INT            NULL,
    [assignment_id]    VARCHAR (8000) NULL,
    [scheduled_hours]  INT            NULL,
    [worked_hours]     INT            NULL,
    [overtime_hours]   INT            NULL,
    [absence_hours]    INT            NULL,
    [sick_leave_hours] INT            NULL,
    [late_minutes]     INT            NULL
);


GO