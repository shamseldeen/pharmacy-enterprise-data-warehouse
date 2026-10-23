CREATE TABLE [gold].[fact_branch_expense] (
    [branch_expense_id]     VARCHAR (8000)  NULL,
    [date_key]              INT             NULL,
    [accounting_month]      DATE            NULL,
    [pharmacy_key]          INT             NULL,
    [expense_category_id]   VARCHAR (8000)  NULL,
    [expense_category_code] VARCHAR (8000)  NULL,
    [expense_group]         VARCHAR (8000)  NULL,
    [cost_basis]            VARCHAR (8000)  NULL,
    [expense_amount_sar]    DECIMAL (18, 2) NULL
);


GO