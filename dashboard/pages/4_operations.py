import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from utils.db import get_conn, load
from utils.formatting import fmt_pct
from utils.theme import COLORS, PLOTLY_LAYOUT, CATEGORY_PALETTE

conn = get_conn()

# ── Load data ────────────────────────────────────────────────────────────────
ship_df    = load(conn, "SELECT * FROM main_gold.mart_average_time_to_ship")
status_df  = load(conn, "SELECT * FROM main_gold.mart_order_status_distribution")
return_df  = load(conn, "SELECT * FROM main_gold.mart_return_rate ORDER BY return_rate_pct DESC")

st.title("Operations & Fulfillment")
st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)

avg_ship = ship_df["avg_days_to_ship"].iloc[0] if not ship_df.empty else 0
total_items   = return_df["total_items"].sum() if not return_df.empty else 0
returned_items = return_df["returned_items"].sum() if not return_df.empty else 0
overall_return = (returned_items / total_items * 100) if total_items else 0

delivered = status_df[status_df["status"] == "Complete"]["order_count"].sum() if not status_df.empty else 0
total_orders = status_df["order_count"].sum() if not status_df.empty else 0
delivery_rate = (delivered / total_orders * 100) if total_orders else 0

c1.metric("Avg Days to Ship", f"{avg_ship:.1f} days")
c2.metric("Overall Return Rate", fmt_pct(overall_return))
c3.metric("Completion Rate", fmt_pct(delivery_rate))

st.divider()

# ── Gauge + Status pie ─────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Average Shipping Time")
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=avg_ship,
        delta={"reference": 3, "increasing": {"color": COLORS["danger"]},
               "decreasing": {"color": COLORS["success"]}},
        title={"text": "Days to Ship"},
        gauge={
            "axis": {"range": [0, 14], "tickwidth": 1},
            "bar":  {"color": COLORS["primary"]},
            "steps": [
                {"range": [0, 3],   "color": "#D1FAE5"},
                {"range": [3, 7],   "color": "#FEF3C7"},
                {"range": [7, 14],  "color": "#FEE2E2"},
            ],
            "threshold": {"line": {"color": COLORS["danger"], "width": 3},
                          "thickness": 0.75, "value": 7},
        },
    ))
    fig.update_layout(**PLOTLY_LAYOUT, height=300)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Order Status Breakdown")
    if not status_df.empty:
        fig = px.pie(status_df, values="order_count", names="status",
                     color_discrete_sequence=CATEGORY_PALETTE, hole=0.4)
        fig.update_layout(**PLOTLY_LAYOUT)
        fig.update_traces(textposition="outside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Return rate by category ───────────────────────────────────────────────────
st.subheader("Return Rate by Product Category")
if not return_df.empty:
    df = return_df.sort_values("return_rate_pct", ascending=True)
    fig = px.bar(df, x="return_rate_pct", y="category", orientation="h",
                 color="return_rate_pct",
                 color_continuous_scale=[[0,"#D1FAE5"],[0.5,"#FEF3C7"],[1,"#FEE2E2"]],
                 labels={"return_rate_pct":"Return Rate (%)","category":"Category"},
                 text=df["return_rate_pct"].apply(lambda x: f"{x:.1f}%"))
    fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False,
                      xaxis_title="Return Rate (%)", yaxis_title="")
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("View return rate table"):
        display = return_df[["category","total_items","returned_items","return_rate_pct"]].copy()
        display.columns = ["Category","Total Items","Returned","Return Rate (%)"]
        st.dataframe(display, use_container_width=True, hide_index=True)
