-- ===============================
-- Core schemas
-- ===============================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ===============================
-- Core fact table
-- ===============================

-- Enriched delivery-level fact table
-- Combines orders, delivery performance, and operational metrics

CREATE TABLE analytics.fact_delivery_enriched (
    order_id TEXT PRIMARY KEY,
    order_date DATE,
    store_id TEXT,
    delivery_partner_id TEXT,
    distance_km DOUBLE PRECISION,
    delivery_time_minutes DOUBLE PRECISION,
    delay_minutes DOUBLE PRECISION,
    is_late INTEGER
);

-- ===============================
-- Machine learning outputs
-- ===============================

-- Predicted delay risk scores per order
CREATE TABLE analytics.delivery_risk_scores (
    order_id TEXT,
    order_date DATE,
    store_id TEXT,
    delivery_partner_id TEXT,
    delay_risk_score DOUBLE PRECISION
);

-- SHAP explainability outputs
CREATE TABLE analytics.delivery_risk_explanations (
    order_id TEXT,
    rank INTEGER,
    feature TEXT,
    feature_value DOUBLE PRECISION,
    shap_value DOUBLE PRECISION,
    direction TEXT
);

-- ===============================
-- Case management
-- ===============================

CREATE TABLE analytics.delay_cases (
    case_id UUID PRIMARY KEY,
    order_id TEXT,
    risk_score_at_creation DOUBLE PRECISION,
    priority TEXT,
    status TEXT,
    assigned_to TEXT,
    notes TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    resolved_at TIMESTAMP
);
