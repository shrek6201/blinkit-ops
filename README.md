# Blinkit Ops – Delivery Risk & Operations Intelligence

## Live Demo:
https://blinkit-ops.onrender.com

Note: Free hosting may sleep when inactive. First load can take ~10–20 seconds.

Blinkit Ops is an end-to-end operations analytics platform that helps identify, explain, and act on delivery delays.
It combines SQL-based analytics, machine learning risk scoring, SHAP explainability, and an interactive operations dashboard.

This project is designed to mirror how real internal ops tools are built and used by data teams.

# What this solves

### Delivery operations teams need to answer three questions quickly:

1. What’s going wrong right now?
2. Why is it happening?
3. What action should we take?

## This project addresses all three:

* Real-time performance KPIs
* Risk-ranked deliveries
* Model explanations for delays
* Intervention case management

## Key Features
### Operations KPIs
  * Daily on-time rate
  * Late delivery trends
  * Store-level and partner-level performance
  * Delay reason distribution

### Delivery Risk Scoring
  * ML model predicts likelihood of late delivery
  * Orders ranked by risk score
  * Supports proactive intervention

### SHAP Explainability
  * Global feature importance
  * Per-order explanations
  * Clear visibility into what drives delay risk

### Case Management
  * Create intervention cases for risky orders
  * Assign priority and ownership
  * Update status (Open → In Progress → Resolved)
  * Dashboard updates instantly

# Dashboard Screenshots

 * Overview
![Screenshot](screenshots/overview.png)

 * Risk score distribution
![Screenshot](screenshots/riskscoredistribution.png)

 * SHAP Feature Impact
![Screenshot](screenshots/shapfeatureimpact.png)

# Tech Stack
### Data & Analytics
 * PostgreSQL
 * SQL (CTEs, window functions, views)
 * Pandas

### Machine Learning
 * XGBoost
 * SHAP
 * scikit-learn

### Orchestration
 * Apache airflow (local)

### Visualization & App
 * Streamlit
 * Plotly

### Deployment
 * Neon (managed Postgres)
 * Render (Streamlit app)

# Data Pipeline Overview
 1. Raw operational data loaded into Postgres
 2. SQL transformations build analytics-ready fact tables
 3. KPIs and views generated for dashboard consumption
 4. ML model trained to predict late deliveries
 5. SHAP explanations written back to the database
 6. Streamlit app reads analytics + ML outputs live

## Repository Structure

```text
blinkit-ops/
├─ app/
│  └─ streamlit_app.py
├─ ml/
│  ├─ train_delay_model.py
│  └─ train_xgb_shap.py
├─ sql/
│  ├─ schema.sql
│  ├─ kpis.sql
│  └─ views.sql
├─ screenshots/
│  ├─ overview.png
│  ├─ order_explainability.png
│  └─ case_management.png
├─ requirements.txt
└─ README.md
```

## Why this project matters
### My project goes beyond basic dashboards
 * combines SQL, Python, and ML in one workflow
 * shows how predictions are operationalized
 * emphasizes explainability and decision-making
 * mirrors real internal analytics tools

## What I would improve next
 * Alerting on high-risk orders
 * SLA breach forecasting
 * Partner performance scorecards
 * Automated retraining
