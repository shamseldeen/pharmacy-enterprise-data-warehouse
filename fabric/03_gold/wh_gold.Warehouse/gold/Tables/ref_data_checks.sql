CREATE TABLE [gold].[ref_data_checks] (
    [check_id]    VARCHAR (10)  NOT NULL,
    [check_group] VARCHAR (40)  NOT NULL,
    [check_name]  VARCHAR (200) NOT NULL,
    [what_tested] VARCHAR (300) NOT NULL,
    [result]      VARCHAR (300) NOT NULL,
    [verdict]     VARCHAR (40)  NOT NULL
);


GO