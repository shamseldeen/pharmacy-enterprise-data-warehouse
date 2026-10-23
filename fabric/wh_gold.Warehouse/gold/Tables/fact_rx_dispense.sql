CREATE TABLE [gold].[fact_rx_dispense] (
    [dispense_id]                    VARCHAR (8000) NULL,
    [prescription_id]                VARCHAR (8000) NULL,
    [prescription_item_id]           VARCHAR (8000) NULL,
    [dispense_date_key]              INT            NULL,
    [prescribed_ts]                  DATETIME2 (6)  NULL,
    [dispense_ts]                    DATETIME2 (6)  NULL,
    [minutes_to_dispense]            INT            NULL,
    [pharmacy_key]                   INT            NULL,
    [customer_key]                   INT            NULL,
    [doctor_key]                     INT            NULL,
    [product_key]                    INT            NULL,
    [dispensing_employee_key]        INT            NULL,
    [prescription_source]            VARCHAR (8000) NULL,
    [prescribed_quantity]            INT            NULL,
    [dispensed_quantity]             INT            NULL,
    [is_partial_fill]                BIT            NULL,
    [dosage_frequency]               VARCHAR (8000) NULL,
    [duration_days]                  INT            NULL,
    [authorized_refills]             INT            NULL,
    [route]                          VARCHAR (8000) NULL,
    [is_dispensed_by_pharmacist]     BIT            NULL,
    [is_authorized_dispenser]        BIT            NULL,
    [dispenser_role]                 VARCHAR (8000) NULL,
    [dispense_linked_transaction_id] VARCHAR (8000) NULL
);


GO