import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.db import get_conn, load
from utils.formatting import fmt_currency, fmt_number
from utils.theme import COLORS, PLOTLY_LAYOUT, CATEGORY_PALETTE

conn = get_conn()

# ── Load data ────────────────────────────────────────────────────────────────
traffic_df    = load(conn, "SELECT * FROM main_gold.mart_traffic_source_performance ORDER BY total_revenue DESC")
age_df        = load(conn, "SELECT * FROM main_gold.mart_age_group_revenue")
country_df    = load(conn, "SELECT * FROM main_gold.mart_sales_by_country")
state_df      = load(conn, "SELECT * FROM main_gold.mart_sales_by_state")
top_cust_df   = load(conn, "SELECT * FROM main_gold.mart_top_customers ORDER BY total_revenue DESC LIMIT 10")
demo_df       = load(conn, "SELECT * FROM main_gold.mart_customer_demographics")

st.title("Customer Analytics")
st.divider()

# ── Traffic source ────────────────────────────────────────────────────────────
st.subheader("Acquisition Channel Performance")
col1, col2 = st.columns(2)

with col1:
    if not traffic_df.empty:
        fig = px.pie(traffic_df, values="total_revenue", names="traffic_source",
                     hole=0.45, color_discrete_sequence=CATEGORY_PALETTE,
                     title="Revenue Share by Channel")
        fig.update_layout(**PLOTLY_LAYOUT)
        fig.update_traces(textposition="outside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    if not traffic_df.empty:
        fig = go.Figure()
        fig.add_bar(name="Orders", x=traffic_df["traffic_source"], y=traffic_df["order_count"],
                    marker_color=COLORS["primary"], yaxis="y")
        fig.add_scatter(name="Avg Order Value ($)", x=traffic_df["traffic_source"],
                        y=traffic_df["avg_order_value"], mode="lines+markers",
                        line=dict(color=COLORS["warning"], width=2),
                        marker=dict(size=7), yaxis="y2")
        fig.update_layout(
            **PLOTLY_LAYOUT,
            title="Orders vs Avg Order Value by Channel",
            yaxis=dict(title="Orders"),
            yaxis2=dict(title="AOV ($)", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Age groups & gender ───────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by Age Group")
    if not age_df.empty:
        fig = px.bar(age_df, x="age_group", y="total_revenue",
                     color="age_group", color_discrete_sequence=CATEGORY_PALETTE,
                     labels={"age_group":"Age Group","total_revenue":"Revenue ($)"},
                     text_auto=".2s")
        fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Gender Distribution")
    if not demo_df.empty:
        fig = px.pie(demo_df, values="user_count", names="gender",
                     color_discrete_sequence=[COLORS["primary"], COLORS["warning"]],
                     hole=0.45)
        fig.update_layout(**PLOTLY_LAYOUT)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Geography ─────────────────────────────────────────────────────────────────
st.subheader("Geographic Sales")
col1, col2 = st.columns(2)

with col1:
    if not country_df.empty:
        fig = px.choropleth(country_df, locations="country", locationmode="country names",
                            color="total_sales", hover_name="country",
                            color_continuous_scale="Blues", title="Sales by Country")
        fig.update_layout(**PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

with col2:
    if not state_df.empty:
        us_states = state_df[state_df["state"].str.len() == 2] if state_df["state"].str.len().max() == 2 else state_df
        fig = px.choropleth(state_df, locations="state", locationmode="USA-states",
                            color="total_sales", hover_name="state", scope="usa",
                            color_continuous_scale="Blues", title="Sales by US State")
        fig.update_layout(**PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Top customers ─────────────────────────────────────────────────────────────
st.subheader("Top 10 Customers by Revenue")
if not top_cust_df.empty:
    display = top_cust_df.copy()
    display.columns = [c.replace("_", " ").title() for c in display.columns]
    st.dataframe(
        display,
        use_container_width=True,
        column_config={
            "Total Revenue": st.column_config.NumberColumn(format="$%.2f"),
        },
        hide_index=True,
    )
