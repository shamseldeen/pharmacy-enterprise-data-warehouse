CREATE TABLE [gold].[fact_claim] (
    [claim_id]              VARCHAR (8000)  NULL,
    [submitted_date_key]    INT             NULL,
    [submitted_ts]          DATETIME2 (6)   NULL,
    [adjudicated_ts]        DATETIME2 (6)   NULL,
    [hours_to_adjudicate]   INT             NULL,
    [pharmacy_key]          INT             NULL,
    [customer_key]          INT             NULL,
    [plan_key]              INT             NULL,
    [prescription_id]       VARCHAR (8000)  NULL,
    [linked_transaction_id] VARCHAR (8000)  NULL,
    [claim_status_name]     VARCHAR (8000)  NULL,
    [rejection_reason_name] VARCHAR (8000)  NULL,
    [claimed_amount_sar]    DECIMAL (18, 2) NULL,
    [approved_amount_sar]   DECIMAL (18, 2) NULL,
    [rejected_amount_sar]   DECIMAL (18, 2) NULL,
    [customer_copay_sar]    DECIMAL (18, 2) NULL,
    [is_fully_rejected]     BIT             NULL,
    [is_partially_rejected] BIT             NULL
);


GO