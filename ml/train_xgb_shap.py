import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from sklearn.metrics import roc_auc_score
from xgboost import XGBClassifier
import shap
import joblib
import os

# -----------------------
# DB connection (edit if needed)
# -----------------------
DB_USER = "blinkit_user"
DB_PASS = "blinkit_pass"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "blinkit_ops"
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    engine = create_engine(DATABASE_URL)
else:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

# -----------------------
# Load leak-free features
# -----------------------
df = pd.read_sql("SELECT * FROM analytics.ml_delay_features_noleak", engine)

features = [
    "total_items",
    "items_value",
    "distance_km",
    "store_daily_deliveries",
    "store_daily_late_rate",
    "partner_daily_deliveries",
    "partner_daily_late_rate",
]

df[features] = df[features].fillna(0)
df["is_late"] = df["is_late"].astype(int)

# -----------------------
# Train/test split by time (more realistic than random)
# -----------------------
df = df.sort_values("order_date").reset_index(drop=True)
cut = int(len(df) * 0.8)
train_df = df.iloc[:cut].copy()
test_df = df.iloc[cut:].copy()

X_train, y_train = train_df[features], train_df["is_late"]
X_test, y_test = test_df[features], test_df["is_late"]

# -----------------------
# Train XGBoost
# -----------------------
model = XGBClassifier(
    n_estimators=300,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.9,
    colsample_bytree=0.9,
    reg_lambda=1.0,
    random_state=42,
    eval_metric="logloss",
)
model.fit(X_train, y_train)

proba_test = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, proba_test)
print("Time-split ROC AUC:", auc)

# Save model (optional)
joblib.dump(model, "ml/xgb_delay_model.joblib")
print("Saved model: ml/xgb_delay_model.joblib")

# -----------------------
# Score ALL rows
# -----------------------
X_all = df[features]
df_scores = df[["order_id", "order_date", "store_id", "delivery_partner_id"]].copy()
df_scores["delay_risk_score"] = model.predict_proba(X_all)[:, 1]

# Write scores to Postgres
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE analytics.delivery_risk_scores;"))

df_scores.to_sql(
    "delivery_risk_scores",
    engine,
    schema="analytics",
    if_exists="append",
    index=False
)
print("Saved: analytics.delivery_risk_scores")

# -----------------------
# SHAP explanations (per-order)
# -----------------------
# TreeExplainer is fast for XGBoost
explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_all)  # shape: (n_rows, n_features)

# For each order: pick top-k features by absolute shap contribution
TOP_K = 5
rows = []
for i in range(len(df)):
    order_id = df.loc[i, "order_id"]
    vals = shap_values[i]
    abs_idx = np.argsort(np.abs(vals))[::-1][:TOP_K]
    for rank, j in enumerate(abs_idx, start=1):
        feature = features[j]
        contribution = float(vals[j])
        feature_value = float(X_all.iloc[i, j])
        direction = "increases_risk" if contribution > 0 else "decreases_risk"
        rows.append({
            "order_id": order_id,
            "rank": rank,
            "feature": feature,
            "feature_value": feature_value,
            "shap_value": contribution,
            "direction": direction,
        })

explain_df = pd.DataFrame(rows)

# Save explanations
explain_df.to_sql(
    "delivery_risk_explanations",
    engine,
    schema="analytics",
    if_exists="replace",
    index=False
)
print("Saved: analytics.delivery_risk_explanations")

# Optional: global importance table (nice for dashboard + README)
imp = (
    explain_df.assign(abs_shap=lambda x: x["shap_value"].abs())
    .groupby("feature", as_index=False)["abs_shap"].mean()
    .sort_values("abs_shap", ascending=False)
)
imp.to_sql(
    "shap_global_importance",
    engine,
    schema="analytics",
    if_exists="replace",
    index=False
)
print("Saved: analytics.shap_global_importance")
