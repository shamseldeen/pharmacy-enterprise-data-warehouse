CREATE TABLE [gold].[ref_calendar_events] (
    [event_type]     VARCHAR (30)  NOT NULL,
    [event_name]     VARCHAR (80)  NOT NULL,
    [start_date]     DATE          NOT NULL,
    [end_date]       DATE          NOT NULL,
    [is_approximate] BIT           NOT NULL,
    [source_note]    VARCHAR (200) NULL
);


GO