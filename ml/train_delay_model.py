import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, roc_auc_score

# Update if your DB user/pass differ
DB_USER = "blinkit_user"
DB_PASS = "blinkit_pass"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "blinkit_ops"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

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

X = df[features].fillna(0)
y = df["is_late"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=y
)

pipe = Pipeline(
    steps=[
        ("scaler", StandardScaler()),
        ("model", LogisticRegression(max_iter=2000))
    ]
)

pipe.fit(X_train, y_train)

y_pred = pipe.predict(X_test)
y_proba = pipe.predict_proba(X_test)[:, 1]

print(classification_report(y_test, y_pred))
print("ROC AUC:", roc_auc_score(y_test, y_proba))

# Score all rows
df["delay_risk_score"] = pipe.predict_proba(X)[:, 1]

# Save scores back to Postgres
out = df[["order_id", "order_date", "store_id", "delivery_partner_id", "delay_risk_score"]].copy()

out.to_sql(
    "delivery_risk_scores",
    engine,
    schema="analytics",
    if_exists="replace",
    index=False
)

print("Saved: analytics.delivery_risk_scores")
