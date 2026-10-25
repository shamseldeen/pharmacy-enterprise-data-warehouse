CREATE   PROCEDURE gold.usp_build_ref_calendar_events
AS
-- =========================================================
-- Rebuilds gold.ref_calendar_events (DROP + CTAS)
-- Generated 2026-10-24 from queryinsights (last successful build)
-- Backup of the original statement: dbo.gold_script_backup
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.ref_calendar_events;

CREATE TABLE gold.ref_calendar_events (
    event_type      varchar(30)  NOT NULL,   -- RAMADAN, EID_AL_FITR, EID_AL_ADHA, SCHOOL_HOLIDAY ...
    event_name      varchar(80)  NOT NULL,
    start_date      date         NOT NULL,
    end_date        date         NOT NULL,
    is_approximate  bit          NOT NULL,   -- 1 = may shift by a day (moon sighting)
    source_note     varchar(200) NULL
)

INSERT INTO gold.ref_calendar_events VALUES
-- Ramadan (Umm al-Qura)
('RAMADAN','Ramadan 1441','2020-04-24','2020-05-23',1,'Umm al-Qura'),
('RAMADAN','Ramadan 1442','2021-04-13','2021-05-12',1,'Umm al-Qura'),
('RAMADAN','Ramadan 1443','2022-04-02','2022-05-01',1,'Umm al-Qura'),
('RAMADAN','Ramadan 1444','2023-03-23','2023-04-20',1,'Umm al-Qura'),
('RAMADAN','Ramadan 1445','2024-03-11','2024-04-09',1,'Umm al-Qura'),
('RAMADAN','Ramadan 1446','2025-03-01','2025-03-29',1,'Umm al-Qura'),
('RAMADAN','Ramadan 1447','2026-02-18','2026-03-19',1,'Umm al-Qura'),
-- Eid al-Fitr: 5 days starting the day after Ramadan
('EID_AL_FITR','Eid al-Fitr 2020','2020-05-24','2020-05-28',1,'5 days'),
('EID_AL_FITR','Eid al-Fitr 2021','2021-05-13','2021-05-17',1,'5 days'),
('EID_AL_FITR','Eid al-Fitr 2022','2022-05-02','2022-05-06',1,'5 days'),
('EID_AL_FITR','Eid al-Fitr 2023','2023-04-21','2023-04-25',1,'5 days'),
('EID_AL_FITR','Eid al-Fitr 2024','2024-04-10','2024-04-14',1,'5 days'),
('EID_AL_FITR','Eid al-Fitr 2025','2025-03-30','2025-04-03',1,'5 days'),
('EID_AL_FITR','Eid al-Fitr 2026','2026-03-20','2026-03-24',1,'5 days'),
-- Eid al-Adha: 5 days
('EID_AL_ADHA','Eid al-Adha 2020','2020-07-31','2020-08-04',1,'5 days'),
('EID_AL_ADHA','Eid al-Adha 2021','2021-07-20','2021-07-24',1,'5 days'),
('EID_AL_ADHA','Eid al-Adha 2022','2022-07-09','2022-07-13',1,'5 days'),
('EID_AL_ADHA','Eid al-Adha 2023','2023-06-28','2023-07-02',1,'5 days'),
('EID_AL_ADHA','Eid al-Adha 2024','2024-06-16','2024-06-20',1,'5 days'),
('EID_AL_ADHA','Eid al-Adha 2025','2025-06-06','2025-06-10',1,'5 days'),
('EID_AL_ADHA','Eid al-Adha 2026','2026-05-27','2026-05-31',1,'5 days')
END

GO