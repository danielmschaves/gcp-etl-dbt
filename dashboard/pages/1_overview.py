import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.db import get_conn, load
from utils.formatting import fmt_currency, fmt_pct, fmt_number
from utils.theme import COLORS, PLOTLY_LAYOUT, CATEGORY_PALETTE

conn = get_conn()

# ── Sidebar: global date range ───────────────────────────────────────────────
revenue_df = load(conn, "SELECT date, total_revenue FROM main_gold.mart_revenue_over_time ORDER BY date")

if revenue_df.empty:
    st.error("No revenue data found. Run `dbt run` first.")
    st.stop()

revenue_df["date"] = pd.to_datetime(revenue_df["date"])
min_date = revenue_df["date"].min().date()
max_date = revenue_df["date"].max().date()

with st.sidebar:
    st.markdown("### Date Range")
    start_date, end_date = st.date_input(
        "Select range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key="global_dates",
    )
    st.session_state["start_date"] = start_date
    st.session_state["end_date"] = end_date

start_dt = pd.to_datetime(start_date)
end_dt   = pd.to_datetime(end_date)
period_days = max((end_dt - start_dt).days, 1)
prior_start = start_dt - pd.Timedelta(days=period_days)
prior_end   = start_dt - pd.Timedelta(days=1)

# ── Load data ────────────────────────────────────────────────────────────────
profit_df = load(conn, """
    SELECT
        dd.date,
        SUM(foi.sale_price) AS revenue,
        SUM(foi.profit)     AS profit
    FROM main_gold.fact_order_items foi
    JOIN main_gold.dim_date dd ON foi.order_date_key = dd.date_key
    GROUP BY dd.date
    ORDER BY dd.date
""")
profit_df["date"] = pd.to_datetime(profit_df["date"])

status_df   = load(conn, "SELECT status, order_count FROM main_gold.mart_order_status_distribution")
return_df   = load(conn, "SELECT return_rate_pct FROM main_gold.mart_return_rate")

def filter_period(df, col="date"):
    return df[(df[col] >= start_dt) & (df[col] <= end_dt)]

def filter_prior(df, col="date"):
    return df[(df[col] >= prior_start) & (df[col] <= prior_end)]

cur  = filter_period(profit_df)
prev = filter_prior(profit_df)

total_rev   = cur["revenue"].sum()
total_profit = cur["profit"].sum()
margin_pct  = (total_profit / total_rev * 100) if total_rev else 0
total_orders = len(cur["date"].unique())  # proxy; mart lacks order_count
aov         = total_rev / total_orders if total_orders else 0
return_rate = return_df["return_rate_pct"].mean() if not return_df.empty else 0

prev_rev    = prev["revenue"].sum()
prev_profit = prev["profit"].sum()
prev_orders = len(prev["date"].unique())

def delta(cur, prev):
    if prev == 0:
        return None
    return f"{(cur - prev) / prev * 100:+.1f}%"

# ── Page header ──────────────────────────────────────────────────────────────
st.title("Executive Overview")
st.caption(f"Showing {start_date} → {end_date}")

# ── KPI row ──────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Revenue",    fmt_currency(total_rev),    delta(total_rev, prev_rev))
c2.metric("Gross Profit",     fmt_currency(total_profit), delta(total_profit, prev_profit))
c3.metric("Profit Margin",    fmt_pct(margin_pct))
c4.metric("Active Days",      fmt_number(total_orders))
c5.metric("Avg Daily Rev",    fmt_currency(aov))
c6.metric("Avg Return Rate",  fmt_pct(return_rate))

st.divider()

# ── Revenue vs Profit monthly trend ──────────────────────────────────────────
st.subheader("Revenue & Profit Over Time")
monthly = (
    cur.set_index("date")
       .resample("ME")[["revenue", "profit"]]
       .sum()
       .reset_index()
)
monthly["month"] = monthly["date"].dt.strftime("%b %Y")

fig = go.Figure()
fig.add_bar(x=monthly["month"], y=monthly["revenue"], name="Revenue",
            marker_color=COLORS["primary"], opacity=0.85)
fig.add_scatter(x=monthly["month"], y=monthly["profit"], name="Gross Profit",
                mode="lines+markers", line=dict(color=COLORS["success"], width=2.5),
                marker=dict(size=6))
fig.update_layout(**PLOTLY_LAYOUT, barmode="overlay",
                  legend=dict(orientation="h", y=1.08),
                  xaxis_title="Month", yaxis_title="USD ($)")
st.plotly_chart(fig, use_container_width=True)

# ── Day-of-week & heatmap ─────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Avg Revenue by Day of Week")
    dow = cur.copy()
    dow["day_of_week"] = dow["date"].dt.day_name()
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    dow_agg = dow.groupby("day_of_week")["revenue"].mean().reindex(order).reset_index()
    fig2 = px.bar(dow_agg, x="day_of_week", y="revenue",
                  color="revenue", color_continuous_scale=[[0,"#E0E7FF"],[1,COLORS["primary"]]],
                  labels={"day_of_week":"Day","revenue":"Avg Revenue ($)"})
    fig2.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False,
                       xaxis_title="", yaxis_title="Avg Revenue ($)")
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("Order Status Distribution")
    if not status_df.empty:
        # Build funnel ordering
        funnel_order = ["Complete", "Shipped", "Processing", "Cancelled", "Returned"]
        status_sorted = (
            status_df.set_index("status")
                     .reindex([s for s in funnel_order if s in status_df["status"].values])
                     .dropna()
                     .reset_index()
        )
        fig3 = go.Figure(go.Funnel(
            y=status_sorted["status"],
            x=status_sorted["order_count"],
            textinfo="value+percent initial",
            marker=dict(color=[COLORS["primary"], COLORS["success"],
                                COLORS["warning"], COLORS["neutral"], COLORS["danger"]]),
        ))
        fig3.update_layout(**PLOTLY_LAYOUT, title_text="")
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No order status data available.")
