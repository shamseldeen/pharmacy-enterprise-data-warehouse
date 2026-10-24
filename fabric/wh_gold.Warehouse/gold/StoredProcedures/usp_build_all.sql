CREATE   PROCEDURE gold.usp_build_all
AS
-- =========================================================
-- Rebuilds the whole Gold layer in dependency order.
-- 1) calendar  2) dimensions  3) facts  4) aggregates
-- Each step = DROP + CTAS. Dimension keys are stable (ROW_NUMBER over natural id).
-- v1 (2026-10-24)
-- =========================================================
BEGIN
    -- 1) Calendar
    EXEC gold.usp_build_ref_calendar_events;
    EXEC gold.usp_build_dim_date;

    -- 2) Dimensions (read Silver only)
    EXEC gold.usp_build_dim_pharmacy;
    EXEC gold.usp_build_dim_product;
    EXEC gold.usp_build_dim_customer;
    EXEC gold.usp_build_dim_employee;
    EXEC gold.usp_build_dim_doctor;
    EXEC gold.usp_build_dim_insurance_plan;
    EXEC gold.usp_build_dim_supplier;

    -- 3) Facts (need dimension keys)
    EXEC gold.usp_build_fact_sales_line;
    EXEC gold.usp_build_fact_sales_transaction;
    EXEC gold.usp_build_fact_return;
    EXEC gold.usp_build_fact_rx_dispense;
    EXEC gold.usp_build_fact_claim;
    EXEC gold.usp_build_fact_inventory_snapshot;
    EXEC gold.usp_build_fact_purchase_line;
    EXEC gold.usp_build_fact_branch_expense;
    EXEC gold.usp_build_fact_timekeeping;

    -- 4) Aggregates (need facts)
    EXEC gold.usp_build_agg_product_loss_summary;
    EXEC gold.usp_build_agg_stock_aging_asof;
    EXEC gold.usp_build_agg_branch_scorecard_monthly;
END;

GO