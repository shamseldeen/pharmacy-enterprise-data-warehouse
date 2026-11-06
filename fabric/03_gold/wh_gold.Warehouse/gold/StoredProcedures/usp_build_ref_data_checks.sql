CREATE   PROCEDURE gold.usp_build_ref_data_checks
AS
-- =========================================================
-- Rebuilds gold.ref_data_checks: integrity signals tested, data gaps
-- and data-generation fingerprints, shown on the "What we checked" page.
-- 2026-11-05: 7 integrity signals, 2 data gaps, 29 fingerprints.
-- =========================================================
BEGIN
    DROP TABLE IF EXISTS gold.ref_data_checks;

    CREATE TABLE gold.ref_data_checks (
        check_id     varchar(10)  NOT NULL,
        check_group  varchar(40)  NOT NULL,
        check_name   varchar(200) NOT NULL,
        what_tested  varchar(300) NOT NULL,
        result       varchar(300) NOT NULL,
        verdict      varchar(40)  NOT NULL
    );

    INSERT INTO gold.ref_data_checks (check_id, check_group, check_name, what_tested, result, verdict)
    VALUES
        ('I1', 'Integrity signal', 'Void abuse', 'Void rate per employee, 2025 (8,484 employees, 100+ receipts)', 'Average 0.42%; spread equals chance (about 1 void expected per employee per year)', 'No signal beyond chance'),
        ('I2', 'Integrity signal', 'Sweethearting (discount)', 'Discount % per employee, 2025', '5.8% average, max 7.7%; no employee above 2x average', 'No signal'),
        ('I3', 'Integrity signal', 'Return after 14-day policy', 'Days from sale to return', '69% after 14 days, but delay is evenly spread 0-60 days', 'Data artifact'),
        ('I4', 'Integrity signal', 'Refund above sale amount', 'Refund vs original line amount', '0 cases', 'No signal'),
        ('I5', 'Integrity signal', 'Self-refund', 'Return processed by the employee who sold', '100% of returns: generated rule, not behaviour', 'Data artifact'),
        ('I6', 'Integrity signal', 'Off-hours sales', 'Receipts between midnight and 6 am', '0%, even in 24H branches', 'Data artifact'),
        ('I7', 'Integrity signal', 'Target gaming', 'Sold in last 3 days of month, returned in first week of next month', '9.7% of returns sold in last 3 days (10% expected); no receipt above 762 SAR', 'No signal'),
        ('G1', 'Data gap', 'Expiry and damage write-offs', 'Movement types and stock adjustments vs batch expiry', 'No expiry/damage movement type; adjustments are count differences far from expiry', 'Not measurable'),
        ('G2', 'Data gap', 'Procurement ownership', 'Supplier and buyer per product', 'Every purchase line has a different supplier and buyer', 'Not measurable'),
        ('F01', 'Data fingerprint', 'Selling price unrelated to list price', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F02', 'Data fingerprint', 'About 4.6 receipts per branch per day with a 363 SAR basket', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F03', 'Data fingerprint', 'Uniform return reasons', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F04', 'Data fingerprint', 'Near-uniform claim rejection reasons and no partial approvals', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F05', 'Data fingerprint', 'Random dispenser roles and 36 hours from prescription to dispense', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F06', 'Data fingerprint', 'Expenses about 2x revenue; about 31 SAR of sales per worked hour', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F07', 'Data fingerprint', 'Loss below cost is a fixed product property, not an event', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F08', 'Data fingerprint', 'About 103 SKUs per branch and 76% dead stock everywhere', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F09', 'Data fingerprint', 'No product without demand anywhere; weak Pareto', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F10', 'Data fingerprint', 'Branches selling before their registered opening date', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F11', 'Data fingerprint', 'Steady 10% yearly growth with no COVID effect in 2020', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F12', 'Data fingerprint', 'Flat margin: 29.4% overall, 33-34% trusted margin in every category', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F13', 'Data fingerprint', 'Flat average basket around 316 SAR', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F14', 'Data fingerprint', 'No seasonality (no Ramadan, Eid or summer effect)', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F15', 'Data fingerprint', 'Monthly receipt counts repeat exactly (171,346 and 175,000)', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F16', 'Data fingerprint', 'January 2025 spike: 3.8 lines per receipt instead of 3', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F17', 'Data fingerprint', 'Revenue share by region equals branch share; flat revenue per branch type', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F18', 'Data fingerprint', 'Every receipt has exactly 3 lines and every line exactly 2 units', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F19', 'Data fingerprint', 'Point-of-sale returns only from January to March 2025', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F20', 'Data fingerprint', 'September 2025: the whole network''s sales recorded in 203 branches', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F21', 'Data fingerprint', 'Inventory shows customer returns all 2025, point of sale only in Q1', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F22', 'Data fingerprint', 'No non-drug categories (beauty, supplements, mother and baby)', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F23', 'Data fingerprint', 'Every purchase line from a different supplier and buyer', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F24', 'Data fingerprint', 'No night sales, even in 24H branches', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F25', 'Data fingerprint', 'Every return processed by the employee who made the sale', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F26', 'Data fingerprint', 'Refund never above the sale amount', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F27', 'Data fingerprint', 'Return delay evenly spread from 0 to 60 days', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F28', 'Data fingerprint', 'Employee discount within 5-7.7%; voids random', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact'),
        ('F29', 'Data fingerprint', 'No receipt above about 762 SAR (no large or insurance receipts)', 'Data quality check', 'Unrealistic pattern for a real chain', 'Data artifact');
END

GO