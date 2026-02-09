from math import exp
from datetime import datetime
import os
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_USER = os.getenv("DB_USER", "blinkit_user")
DB_PASS = os.getenv("DB_PASS", "blinkit_pass")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "blinkit_ops")

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    if "sslmode=" not in DATABASE_URL:
        joiner = "&" if "?" in DATABASE_URL else "?"
        DATABASE_URL = f"{DATABASE_URL}{joiner}sslmode=require"
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True
    )


def create_case(order_id, risk_score, priority, assigned_to, notes):
    q = """
    INSERT INTO analytics.delay_cases
    (order_id, risk_score_at_creation, priority, assigned_to, notes)
    VALUES (:order_id, :risk_score, :priority, :assigned_to, :notes)
    """
    with engine.begin() as conn:
        conn.execute(text(q), {
            "order_id": order_id,
            "risk_score": risk_score,
            "priority": priority,
            "assigned_to": assigned_to,
            "notes": notes,
        })

def load_cases(status_filter=None):
    base = """
    SELECT
      case_id::text AS case_id,
      order_id,
      risk_score_at_creation,
      priority,
      status,
      assigned_to,
      notes,
      created_at,
      updated_at,
      resolved_at
    FROM analytics.delay_cases
    """
    if status_filter:
        base += " WHERE status = :status"
        return pd.read_sql(text(base), engine, params={"status": status_filter})
    return pd.read_sql(base, engine)


def update_case_status(case_id, new_status):
    q = """
    UPDATE analytics.delay_cases
    SET status = :status,
        updated_at = now(),
        resolved_at = CASE WHEN :status = 'Resolved' THEN now() ELSE resolved_at END
    WHERE case_id = :case_id
    """
    with engine.begin() as conn:
        conn.execute(text(q), {
            "case_id": case_id,
            "status": new_status
        })

@st.cache_data(ttl=60)
def load_order_explanations(order_id: str):
    q = """
    SELECT rank, feature, feature_value, shap_value, direction
    FROM analytics.delivery_risk_explanations
    WHERE order_id = :order_id
    ORDER BY rank ASC
    """
    return pd.read_sql(text(q), engine, params={"order_id": order_id})


st.set_page_config(page_title="Blinkit Ops Risk Dashboard", layout="wide")

st.title("Blinkit Ops: Delivery Risk & Performance")
st.caption("Ops intelligence dashboard with risk scoring, SHAP explainability, and intervention case management.")

@st.cache_data(ttl=60)
def load_kpis():
    daily = pd.read_sql("SELECT * FROM analytics.kpi_daily_ontime ORDER BY order_date", engine)
    partners = pd.read_sql(
        "SELECT * FROM analytics.kpi_partner_scorecard ORDER BY late_rate DESC", engine
    )
    stores = pd.read_sql(
        "SELECT * FROM analytics.kpi_store_scorecard ORDER BY late_rate DESC", engine
    )
    reasons = pd.read_sql("SELECT * FROM analytics.kpi_delay_reasons", engine)
    return daily, partners, stores, reasons

@st.cache_data(ttl=60)
def load_risk_queue():
    # Uses your view
    q = """
    SELECT * FROM analytics.v_today_risk_queue
    """
    df = pd.read_sql(q, engine)
    return df

def metric_block(daily_df: pd.DataFrame):
    latest = daily_df.iloc[-1]

    deliveries_latest = int(latest["deliveries"])
    late_latest = int(latest["late_deliveries"])

    # Last 7 days (or fewer if not available)
    last_n = daily_df.tail(7)
    on_time_7d = 1 - last_n["late_deliveries"].sum() / last_n["deliveries"].sum()
    avg_delay_7d = last_n["avg_delay_minutes"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Deliveries (latest day)", f"{deliveries_latest}")
    c2.metric("Late deliveries (latest day)", f"{late_latest}")
    c3.metric("On-time rate (last 7 days)", f"{on_time_7d:.2%}")
    c4.metric("Avg delay (last 7 days)", f"{avg_delay_7d:.2f}")


def overview_tab(daily, partners, stores, reasons):
    st.subheader("Operations Overview")

    metric_block(daily)

    st.markdown("### On-time trend")
    fig = px.line(daily, x="order_date", y="on_time_rate")
    st.plotly_chart(fig, use_container_width=True)

    colA, colB = st.columns(2)

    with colA:
        st.markdown("### Worst stores (by late rate)")
        st.dataframe(
            stores.sort_values(["late_rate", "avg_delay_minutes"], ascending=False).head(10),
            use_container_width=True
        )

    with colB:
        st.markdown("### Worst partners (by late rate)")
        st.dataframe(
            partners.sort_values(["late_rate", "avg_delay_minutes"], ascending=False).head(10),
            use_container_width=True
        )

    st.markdown("### Delay reasons")

    top = reasons.head(12)

    if len(top) <= 1:
        st.info(f"Only one delay reason found in data: **{top.iloc[0]['reason']}**")
        st.metric("Occurrences", int(top.iloc[0]["occurrences"]))
    else:
        fig2 = px.bar(top, x="reason", y="occurrences")
        st.plotly_chart(fig2, use_container_width=True)


def risk_queue_tab(risk_df: pd.DataFrame):
    st.subheader("Risk Queue")

    # Add a risk band in-app (you can also do this in SQL)
    def band(x: float) -> str:
        if x >= 0.85:
            return "High"
        if x >= 0.60:
            return "Medium"
        return "Low"

    risk_df = risk_df.copy()
    risk_df["risk_band"] = risk_df["risk_score"].apply(band)

    col1, col2, col3 = st.columns(3)
    band_filter = col1.multiselect(
        "Risk band",
        options=["High", "Medium", "Low"],
        default=["High", "Medium"]
    )
    store_filter = col2.multiselect(
        "Store",
        options=sorted(risk_df["store_id"].unique().tolist()),
        default=[]
    )
    partner_filter = col3.multiselect(
        "Partner",
        options=sorted(risk_df["delivery_partner_id"].unique().tolist()),
        default=[]
    )

    df = risk_df[risk_df["risk_band"].isin(band_filter)]
    if store_filter:
        df = df[df["store_id"].isin(store_filter)]
    if partner_filter:
        df = df[df["delivery_partner_id"].isin(partner_filter)]

    st.markdown("### Highest-risk deliveries")
    st.dataframe(
        df.sort_values("risk_score", ascending=False).head(200),
        use_container_width=True
    )

    st.markdown("### Risk score distribution")
    fig = px.histogram(df, x="risk_score", nbins=30)
    st.plotly_chart(fig, use_container_width=True)

def order_drilldown_tab(risk_df: pd.DataFrame):
    st.subheader("Order Drilldown")

    order_id = st.selectbox(
        "Select an order_id",
        options=risk_df.sort_values("risk_score", ascending=False)["order_id"].head(500).tolist()
    )

    if "case_created_msg" in st.session_state:
        st.success(st.session_state.pop("case_created_msg"))

    # Pull detailed row from fact + score
    query = """
    SELECT
      f.order_id,
      f.order_date,
      f.store_id,
      f.delivery_partner_id,
      f.distance_km,
      f.delivery_time_minutes,
      f.delay_minutes,
      f.is_late,
      s.delay_risk_score
    FROM analytics.fact_delivery_enriched f
    JOIN analytics.delivery_risk_scores s USING (order_id)
    WHERE f.order_id = :order_id
    """
    detail = pd.read_sql(text(query), engine, params={"order_id": order_id})

    st.markdown("### Details")
    st.dataframe(detail, use_container_width=True)

    st.markdown("### Why this is risky (SHAP top drivers)")
    exp = load_order_explanations(order_id)

    if exp.empty:
        st.warning("No SHAP explanation found for this order yet. Run the XGBoost+SHAP script again.")
    else:
        st.dataframe(exp, use_container_width=True)

        exp_plot = exp.copy()
        exp_plot["abs_impact"] = exp_plot["shap_value"].abs()
        exp_plot = exp_plot.sort_values("abs_impact", ascending=True)

        fig = px.bar(
            exp_plot,
            x="shap_value",
            y="feature",
            orientation="h",
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("### Create intervention case")

    with st.form("create_case_form"):
        priority = st.selectbox("Priority", ["High", "Medium", "Low"], index=0)
        assigned_to = st.text_input("Assign to (optional)", "")
        notes = st.text_area("Notes", placeholder="e.g. Call rider, reroute, notify store")

        submitted = st.form_submit_button("Create Case")

        if submitted:
            risk_score = float(detail["delay_risk_score"].iloc[0])
            create_case(
                order_id=order_id,
                risk_score=risk_score,
                priority=priority,
                assigned_to=assigned_to,
                notes=notes
            )

            st.session_state["case_created_msg"] = f"Case created for order {order_id}"
            st.rerun()

def cases_tab():
    st.subheader("Delay Intervention Cases")

    status_filter = st.selectbox(
        "Filter by status",
        ["All", "Open", "In Progress", "Resolved", "Cancelled"]
    )

    df = load_cases(None if status_filter == "All" else status_filter)

    if df.empty:
        st.info("No cases found")
        return

    st.dataframe(df, use_container_width=True)

    st.markdown("### Update case status")
    case_id = st.selectbox("Select case", df["case_id"])
    new_status = st.selectbox(
        "New status",
        ["Open", "In Progress", "Resolved", "Cancelled"]
    )

    if st.button("Update status"):
        update_case_status(case_id, new_status)
        st.success("Case updated")
        st.rerun()


def main():
    daily, partners, stores, reasons = load_kpis()
    risk = load_risk_queue()

    tab1, tab2, tab3, tab4 = st.tabs(
        ["Overview", "Risk Queue", "Order Drilldown", "Cases"]
    )


    with tab1:
        overview_tab(daily, partners, stores, reasons)

    with tab2:
        risk_queue_tab(risk)

    with tab3:
        order_drilldown_tab(risk)

    with tab4:
        cases_tab()
    

if __name__ == "__main__":
    main()
