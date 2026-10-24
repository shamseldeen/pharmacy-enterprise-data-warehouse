CREATE TABLE [dbo].[gold_script_backup] (
    [target_table]             VARCHAR (MAX) NULL,
    [captured_from_run_at_utc] DATETIME2 (6) NULL,
    [create_statement]         VARCHAR (MAX) NULL
);


GO