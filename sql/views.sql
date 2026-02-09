-- ===============================
-- Risk-ranked delivery queue
-- ===============================

CREATE VIEW analytics.v_today_risk_queue AS
SELECT
    s.order_id,
    f.order_date,
    f.store_id,
    f.delivery_partner_id,
    ROUND(s.delay_risk_score::numeric, 4) AS risk_score,
    f.distance_km
FROM analytics.delivery_risk_scores s
JOIN analytics.fact_delivery_enriched f
    ON s.order_id = f.order_id
ORDER BY risk_score DESC;

-- ===============================
-- Risk banding for ops usage
-- ===============================

CREATE VIEW analytics.v_today_risk_queue_banded AS
SELECT *,
    CASE
        WHEN risk_score >= 0.85 THEN 'High'
        WHEN risk_score >= 0.60 THEN 'Medium'
        ELSE 'Low'
    END AS risk_band
FROM analytics.v_today_risk_queue;
