"""
Payment Ops Intelligence Dashboard — reads directly from Gold Delta tables.

Design note: Streamlit re-runs the whole script top-to-bottom on every
interaction/refresh. We use st.cache_data with a short TTL so repeated
reruns within the same few seconds don't re-trigger a full Spark read,
while still picking up new data on the next refresh cycle.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import sys

st.set_page_config(page_title="Payment Ops Intelligence", layout="wide", page_icon="💳")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))          # ← moved here, after PROJECT_ROOT exists
DATA_ROOT = PROJECT_ROOT / "data" / "processed"

# ============================================================
# CUSTOM CSS — professional styling only, no logic changes
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* App background — light, clean */
    .stApp {
        background: #f4f6fb;
    }

    /* Main content padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1400px;
    }

    /* Title header */
    h1 {
        font-weight: 800 !important;
        color: #1e2433 !important;
        letter-spacing: -0.02em;
        padding-bottom: 0.2rem;
    }

    /* Caption under title */
    .stCaption, [data-testid="stCaptionContainer"] {
        color: #6b7280 !important;
        font-size: 0.9rem !important;
        font-weight: 500;
    }

    /* Section headers */
    h2, h3 {
        color: #1e2433 !important;
        font-weight: 700 !important;
        letter-spacing: -0.01em;
        margin-top: 0.5rem;
    }

    /* Body / generic text */
    p, span, label {
        color: #1e2433;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #ffffff;
        border-right: 1px solid #e5e7eb;
    }
    section[data-testid="stSidebar"] .stRadio label,
    section[data-testid="stSidebar"] .stRadio label p {
        color: #374151 !important;
        font-weight: 500;
    }
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3,
    section[data-testid="stSidebar"] .stMarkdown p {
        color: #1e2433 !important;
    }
    section[data-testid="stSidebar"] .stCaption,
    section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
        color: #9ca3af !important;
    }

    /* Sidebar refresh button */
    section[data-testid="stSidebar"] .stButton button {
        background: linear-gradient(90deg, #4f46e5, #7c3aed);
        color: #ffffff !important;
        border: none;
        border-radius: 10px;
        font-weight: 600;
        padding: 0.55rem 1rem;
        width: 100%;
        transition: all 0.15s ease;
        box-shadow: 0 2px 10px rgba(79,70,229,0.3);
    }
    section[data-testid="stSidebar"] .stButton button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 16px rgba(79,70,229,0.45);
    }
    section[data-testid="stSidebar"] .stButton button p {
        color: #ffffff !important;
    }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 14px;
        padding: 1.1rem 1.3rem;
        box-shadow: 0 2px 10px rgba(15,23,42,0.06);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 22px rgba(15,23,42,0.1);
        border-color: #c7d2fe;
    }
    div[data-testid="stMetricLabel"] {
        color: #6b7280 !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
    }
    div[data-testid="stMetricLabel"] p {
        color: #6b7280 !important;
    }
    div[data-testid="stMetricValue"] {
        color: #111827 !important;
        font-weight: 800 !important;
        font-size: 1.7rem !important;
    }
    div[data-testid="stMetricDelta"] {
        font-weight: 600 !important;
    }

    /* Dividers */
    hr {
        border-color: #e5e7eb !important;
        margin: 1.6rem 0 !important;
    }

    /* Dataframes / tables */
    div[data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #e5e7eb;
    }

    /* Info / warning / error boxes */
    div[data-testid="stAlert"] {
        border-radius: 12px;
    }

    /* Markdown text in correlation feed */
    .stMarkdown p {
        font-size: 0.98rem;
        color: #374151;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_spark():
    import os, sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages io.delta:delta-spark_2.12:3.2.0 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "pyspark-shell"
    )
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("StreamlitDashboard")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def to_spark_path(p: Path) -> str:
    return "file:///" + str(p).replace("\\", "/")


@st.cache_data(ttl=10)
def load_delta_as_pandas(table_name: str) -> pd.DataFrame:
    spark = get_spark()
    path = to_spark_path(DATA_ROOT / table_name)
    try:
        return spark.read.format("delta").load(path).toPandas()
    except Exception as e:
        st.error(f"Could not load {table_name}: {e}")
        return pd.DataFrame()


st.title("💳 Payment Operations Intelligence Platform")
st.caption(f"Last refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

st.sidebar.markdown("### 🧭 Navigation")
page = st.sidebar.radio(
    "Navigate",
    ["Live Operations Center", "Cross-Bank Correlation", "Settlement & Reconciliation", "AI Ops Intelligence"],
    label_visibility="collapsed",
)

st.sidebar.divider()
st.sidebar.subheader("⚙️ Connection Health")
st.sidebar.caption("Kafka, Spark, Delta status checks would appear here in a full deployment.")

if st.sidebar.button("🔄 Refresh Now"):
    st.cache_data.clear()
    st.rerun()

# ============================================================
# PAGE 1 — LIVE OPERATIONS CENTER
# ============================================================
if page == "Live Operations Center":
    ops_df = load_delta_as_pandas("mart_ops_dashboard_metrics")

    if ops_df.empty:
        st.warning("No data available yet in mart_ops_dashboard_metrics.")
    else:
        col1, col2, col3 = st.columns(3)
        total_txns = int(ops_df["total_transactions"].sum())
        total_success = int(ops_df["success_count"].sum())
        overall_success_rate = (total_success / total_txns * 100) if total_txns else 0
        total_volume = ops_df["total_volume_inr"].sum()

        col1.metric("Total Transactions", f"{total_txns:,}")
        col2.metric("Overall Success Rate", f"{overall_success_rate:.1f}%")
        col3.metric("Total Volume (₹)", f"{total_volume:,.0f}")

        st.divider()
        st.subheader("Success Rate by Payment Method")

        by_method = (
            ops_df.groupby("payment_method")
            .agg(
                total_transactions=("total_transactions", "sum"),
                success_count=("success_count", "sum"),
                avg_response_time_ms=("avg_response_time_ms", "mean"),
            )
            .reset_index()
        )
        by_method["success_rate_pct"] = (
            by_method["success_count"] / by_method["total_transactions"] * 100
        )

        def color_for_rate(rate):
            if rate >= 95:
                return "🟢"
            elif rate >= 90:
                return "🟡"
            return "🔴"

        cols = st.columns(len(by_method)) if len(by_method) <= 6 else st.columns(6)
        for i, row in by_method.iterrows():
            col = cols[i % len(cols)]
            col.metric(
                f"{color_for_rate(row['success_rate_pct'])} {row['payment_method']}",
                f"{row['success_rate_pct']:.1f}%",
                f"{row['avg_response_time_ms']:.0f}ms avg",
            )

        st.divider()
        st.subheader("Volume by Merchant Category")
        import plotly.express as px
        by_category = ops_df.groupby("merchant_category")["total_volume_inr"].sum().reset_index()
        fig = px.bar(
            by_category, x="merchant_category", y="total_volume_inr",
            labels={"total_volume_inr": "Volume (₹)", "merchant_category": "Category"},
            color="merchant_category",
            color_discrete_sequence=["#6366f1", "#8b5cf6", "#a78bfa", "#60a5fa", "#f472b6", "#34d399", "#fbbf24"],
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#374151", family="Inter"),
            showlegend=False,
            xaxis=dict(gridcolor="#e5e7eb"),
            yaxis=dict(gridcolor="#e5e7eb"),
            margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# PAGE 2 — CROSS-BANK FAILURE CORRELATION
# ============================================================
elif page == "Cross-Bank Correlation":
    corr_df = load_delta_as_pandas("mart_correlation_summary")
    ops_df = load_delta_as_pandas("mart_ops_dashboard_metrics")

    st.subheader("Correlation Engine — Recent Classifications")
    if corr_df.empty:
        st.info("No correlation events recorded yet.")
    else:
        corr_df_sorted = corr_df.sort_values("detected_at", ascending=False)
        for _, row in corr_df_sorted.iterrows():
            classification = row["classification"]
            icon = {
                "NPCI_SIDE_ISSUE": "🔴",
                "PLATFORM_SIDE_ISSUE": "🟠",
                "BANK_SIDE_ISSUE": "🟡",
                "HEALTHY": "🟢",
            }.get(classification, "⚪")
            border_color = {
                "NPCI_SIDE_ISSUE": "#ef4444",
                "PLATFORM_SIDE_ISSUE": "#f97316",
                "BANK_SIDE_ISSUE": "#eab308",
                "HEALTHY": "#22c55e",
            }.get(classification, "#6b7280")
            st.markdown(
                f"""
                <div style="
                    background: #ffffff;
                    border: 1px solid #e5e7eb;
                    border-left: 4px solid {border_color};
                    border-radius: 10px;
                    padding: 0.7rem 1rem;
                    margin-bottom: 0.5rem;
                    box-shadow: 0 2px 8px rgba(15,23,42,0.05);
                ">
                    <span style="font-size:1rem; color:#111827;">{icon} <b>{classification}</b></span><br/>
                    <span style="color:#6b7280; font-size:0.85rem;">
                        {row['affected_banks'] or 'none'} &nbsp;•&nbsp; {row['detected_at']}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.divider()
    st.subheader("Bank Health Heatmap (by Payment Method)")
    if not ops_df.empty:
        bank_methods = ops_df[ops_df["payment_method"].str.startswith("upi_")]
        if not bank_methods.empty:
            pivot = bank_methods.pivot_table(
                index="payment_method", columns="event_date",
                values="success_count", aggfunc="sum", fill_value=0
            )
            st.dataframe(
                pivot.style.background_gradient(cmap="Blues"),
                use_container_width=True,
            )
        else:
            st.info("No UPI bank data available yet.")

# ============================================================
# PAGE 3 — SETTLEMENT & RECONCILIATION
# ============================================================
elif page == "Settlement & Reconciliation":
    recon_df = load_delta_as_pandas("mart_settlement_reconciliation")

    if recon_df.empty:
        st.warning("No settlement reconciliation data available yet.")
    else:
        total_settled = int(recon_df["total_settled_transactions"].sum())
        total_delayed = int(recon_df["delayed_count"].sum())
        match_rate = ((total_settled - total_delayed) / total_settled * 100) if total_settled else 0
        total_discrepancy = recon_df["amount_discrepancy"].sum()

        col1, col2, col3 = st.columns(3)
        col1.metric("Settlement Match Rate", f"{match_rate:.1f}%")
        col2.metric("Delayed Settlements", f"{total_delayed:,}")
        col3.metric("Amount Discrepancy (₹)", f"{total_discrepancy:,.0f}")

        st.divider()
        st.subheader("Top Merchants by Discrepancy")
        top_discrepancy = (
            recon_df.groupby("merchant_id")["amount_discrepancy"]
            .sum()
            .reset_index()
            .sort_values("amount_discrepancy", ascending=False)
            .head(15)
        )
        st.dataframe(
            top_discrepancy.style.background_gradient(cmap="Reds", subset=["amount_discrepancy"]),
            use_container_width=True,
        )
# ===========================================================
# PAGE 5 — AI OPS INTELLIGENCE (Groq agent)
# ============================================================
elif page == "AI Ops Intelligence":
    st.subheader("🤖 AI-Generated Ops Intelligence Report")
    st.caption("Powered by Groq (Llama 3.3 70B) — reads live Gold tables, summarizes system health.")

    reports_df = load_delta_as_pandas("ops_intelligence_reports")

    if st.button("🔁 Regenerate Report Now"):
        with st.spinner("Agent is querying Gold tables and generating report..."):
            try:
                from ai_layer.ops_intelligence_agent import generate_report, save_report
                new_report = generate_report()
                save_report(new_report)
                st.cache_data.clear()
                st.success("New report generated.")
                st.rerun()
            except Exception as e:
                st.error(f"Agent run failed: {e}")

    if reports_df.empty:
        st.info("No reports generated yet. Click 'Regenerate Report Now' to create the first one.")
    else:
        latest = reports_df.sort_values("generated_at", ascending=False).iloc[0]
        st.caption(f"Last generated: {latest['generated_at']}")
        st.markdown(
            f"""
            <div style="
                background: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 14px;
                padding: 1.4rem 1.6rem;
                box-shadow: 0 2px 10px rgba(15,23,42,0.06);
                white-space: pre-wrap;
                font-size: 0.95rem;
                line-height: 1.6;
                color: #1e2433;
            ">
            {latest['report']}
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.divider()
        st.subheader("Report History")
        history = reports_df.sort_values("generated_at", ascending=False)[["generated_at", "report"]]
        st.dataframe(history, use_container_width=True, height=200)