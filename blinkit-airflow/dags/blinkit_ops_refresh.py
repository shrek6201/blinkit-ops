from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

SQL_REFRESH = """
-- 1) Stable mappings
DROP TABLE IF EXISTS analytics.map_order_entities;
CREATE TABLE analytics.map_order_entities AS
SELECT
  order_id,
  'store_' || LPAD(((ABS(HASHTEXT(order_id)) % 50) + 1)::text, 3, '0') AS store_key,
  'partner_' || LPAD(((ABS(HASHTEXT(order_id)) % 200) + 1)::text, 3, '0') AS partner_key
FROM analytics.stg_orders;

-- 2) Enriched fact
DROP TABLE IF EXISTS analytics.fact_delivery_enriched;
CREATE TABLE analytics.fact_delivery_enriched AS
SELECT
  o.order_id,
  o.customer_id,
  m.store_key AS store_id,
  m.partner_key AS delivery_partner_id,
  o.order_date,
  d.promised_time,
  d.actual_time,
  d.delivery_status,
  d.reasons_if_delayed,
  d.distance_km,
  d.delivery_time_minutes,
  EXTRACT(EPOCH FROM (d.actual_time - d.promised_time)) / 60.0 AS delay_minutes,
  CASE
    WHEN d.actual_time IS NULL OR d.promised_time IS NULL THEN NULL
    WHEN d.actual_time > d.promised_time THEN 1 ELSE 0
  END AS is_late
FROM analytics.stg_orders o
JOIN analytics.stg_deliveries d USING (order_id)
JOIN analytics.map_order_entities m USING (order_id);

-- 3) KPI tables
DROP TABLE IF EXISTS analytics.kpi_daily_ontime;
CREATE TABLE analytics.kpi_daily_ontime AS
SELECT
  order_date,
  COUNT(*) AS deliveries,
  SUM(is_late) AS late_deliveries,
  ROUND((1 - AVG(is_late))::numeric, 4) AS on_time_rate,
  ROUND(AVG(delay_minutes)::numeric, 2) AS avg_delay_minutes
FROM analytics.fact_delivery_enriched
GROUP BY 1
ORDER BY 1;

DROP TABLE IF EXISTS analytics.kpi_partner_scorecard;
CREATE TABLE analytics.kpi_partner_scorecard AS
SELECT
  delivery_partner_id,
  COUNT(*) AS deliveries,
  ROUND(AVG(is_late)::numeric, 4) AS late_rate,
  ROUND(AVG(delay_minutes)::numeric, 2) AS avg_delay_minutes,
  ROUND(AVG(distance_km)::numeric, 2) AS avg_distance_km
FROM analytics.fact_delivery_enriched
GROUP BY 1;

DROP TABLE IF EXISTS analytics.kpi_store_scorecard;
CREATE TABLE analytics.kpi_store_scorecard AS
SELECT
  store_id,
  COUNT(*) AS deliveries,
  ROUND(AVG(is_late)::numeric, 4) AS late_rate,
  ROUND(AVG(delay_minutes)::numeric, 2) AS avg_delay_minutes
FROM analytics.fact_delivery_enriched
GROUP BY 1;

DROP TABLE IF EXISTS analytics.kpi_delay_reasons;
CREATE TABLE analytics.kpi_delay_reasons AS
SELECT
  COALESCE(NULLIF(TRIM(reasons_if_delayed), ''), 'Unknown') AS reason,
  COUNT(*) AS occurrences
FROM analytics.fact_delivery_enriched
WHERE is_late = 1
GROUP BY 1
ORDER BY occurrences DESC;
"""

with DAG(
    dag_id="blinkit_ops_refresh",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["blinkit", "ops", "warehouse"],
) as dag:
    refresh = PostgresOperator(
        task_id="refresh_fact_and_kpis",
        postgres_conn_id="blinkit_ops_pg",
        sql=SQL_REFRESH,
    )
