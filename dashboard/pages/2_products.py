import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.db import get_conn, load
from utils.theme import COLORS, PLOTLY_LAYOUT, CATEGORY_PALETTE

conn = get_conn()

# ── Load data ────────────────────────────────────────────────────────────────
top_products_df  = load(conn, "SELECT * FROM main_gold.mart_top_selling_products")
profit_cat_df    = load(conn, "SELECT * FROM main_gold.mart_profit_by_category ORDER BY total_revenue DESC")
brand_df         = load(conn, "SELECT * FROM main_gold.mart_brand_performance ORDER BY total_revenue DESC")
product_margin_df = load(conn, "SELECT * FROM main_gold.mart_product_margin ORDER BY avg_margin_pct DESC")

st.title("Product Intelligence")
st.divider()

# ── Top by revenue vs top by margin ──────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 10 by Items Sold")
    if not top_products_df.empty:
        df = top_products_df.head(10).sort_values("total_items_sold")
        fig = px.bar(df, x="total_items_sold", y="product_name", orientation="h",
                     color="total_items_sold",
                     color_continuous_scale=[[0,"#E0E7FF"],[1,COLORS["primary"]]],
                     labels={"total_items_sold":"Items Sold","product_name":""})
        fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False,
                          yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top 10 by Profit Margin")
    if not product_margin_df.empty:
        df = product_margin_df.head(10).sort_values("avg_margin_pct")
        fig = px.bar(df, x="avg_margin_pct", y="product_name", orientation="h",
                     color="avg_margin_pct",
                     color_continuous_scale=[[0,"#D1FAE5"],[1,COLORS["success"]]],
                     labels={"avg_margin_pct":"Avg Margin (%)","product_name":""})
        fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False,
                          yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Category bubble chart: Revenue vs Margin ─────────────────────────────────
st.subheader("Category Performance: Revenue vs Margin")
if not profit_cat_df.empty:
    fig = px.scatter(
        profit_cat_df,
        x="total_revenue", y="avg_margin_pct",
        size="items_sold", color="category",
        hover_name="category",
        hover_data={"total_revenue":":.2f","avg_margin_pct":":.1f","items_sold":True},
        color_discrete_sequence=CATEGORY_PALETTE,
        labels={"total_revenue":"Total Revenue ($)","avg_margin_pct":"Avg Margin (%)","items_sold":"Items Sold"},
        size_max=60,
    )
    fig.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Brand performance table ───────────────────────────────────────────────────
st.subheader("Brand Performance")
if not brand_df.empty:
    display_df = brand_df[["brand","category","total_revenue","total_profit","avg_margin_pct","items_sold"]].copy()
    display_df.columns = ["Brand","Category","Revenue ($)","Profit ($)","Margin (%)","Items Sold"]
    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "Revenue ($)":  st.column_config.NumberColumn(format="$%.2f"),
            "Profit ($)":   st.column_config.NumberColumn(format="$%.2f"),
            "Margin (%)":   st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
            "Items Sold":   st.column_config.NumberColumn(format="%d"),
        },
        hide_index=True,
    )
