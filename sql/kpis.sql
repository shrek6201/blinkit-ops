-- ===============================
-- Daily on-time performance KPI
-- ===============================

CREATE TABLE analytics.kpi_daily_ontime AS
SELECT
    order_date,
    COUNT(*) AS deliveries,
    SUM(is_late) AS late_deliveries,
    ROUND((1 - AVG(is_late))::numeric, 4) AS on_time_rate,
    ROUND(AVG(delay_minutes)::numeric, 2) AS avg_delay_minutes
FROM analytics.fact_delivery_enriched
GROUP BY order_date;

-- ===============================
-- Store performance scorecard
-- ===============================

CREATE TABLE analytics.kpi_store_scorecard AS
SELECT
    store_id,
    COUNT(*) AS deliveries,
    ROUND(AVG(is_late)::numeric, 3) AS late_rate,
    ROUND(AVG(delay_minutes)::numeric, 2) AS avg_delay_minutes
FROM analytics.fact_delivery_enriched
GROUP BY store_id;

-- ===============================
-- Delivery partner scorecard
-- ===============================

CREATE TABLE analytics.kpi_partner_scorecard AS
SELECT
    delivery_partner_id,
    COUNT(*) AS deliveries,
    ROUND(AVG(is_late)::numeric, 3) AS late_rate,
    ROUND(AVG(delay_minutes)::numeric, 2) AS avg_delay_minutes
FROM analytics.fact_delivery_enriched
GROUP BY delivery_partner_id;

-- ===============================
-- Delay reason distribution
-- ===============================

CREATE TABLE analytics.kpi_delay_reasons AS
SELECT
    COALESCE(NULLIF(TRIM(reasons_if_delayed), ''), 'Unknown') AS reason,
    COUNT(*) AS occurrences
FROM analytics.fact_delivery_enriched
WHERE is_late = 1
GROUP BY reason
ORDER BY occurrences DESC;
