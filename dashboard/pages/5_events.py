import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.db import get_conn, load
from utils.theme import COLORS, PLOTLY_LAYOUT, CATEGORY_PALETTE

conn = get_conn()

# ── Load data ────────────────────────────────────────────────────────────────
funnel_df = load(conn, "SELECT * FROM main_gold.mart_event_funnel ORDER BY event_count DESC")

traffic_events_df = load(conn, """
    SELECT traffic_source, event_type, COUNT(*) AS event_count
    FROM main.stg_events
    GROUP BY traffic_source, event_type
    ORDER BY event_count DESC
""")

sessions_df = load(conn, """
    SELECT
        CAST(created_at AS DATE) AS event_date,
        COUNT(DISTINCT session_id) AS sessions,
        COUNT(*) AS events
    FROM main.stg_events
    GROUP BY event_date
    ORDER BY event_date
""")

top_uris_df = load(conn, """
    SELECT uri, COUNT(*) AS event_count
    FROM main.stg_events
    WHERE uri IS NOT NULL
    GROUP BY uri
    ORDER BY event_count DESC
    LIMIT 15
""")

st.title("Marketing & Events")
st.divider()

# ── Event funnel ──────────────────────────────────────────────────────────────
st.subheader("User Event Funnel")
if not funnel_df.empty:
    # Order by a logical funnel sequence
    funnel_order = ["home", "category", "brand", "product", "cart", "purchase", "cancel"]
    ordered = []
    for step in funnel_order:
        match = funnel_df[funnel_df["event_type"].str.lower().str.contains(step)]
        if not match.empty:
            ordered.append(match.iloc[0])
    remaining = funnel_df[~funnel_df["event_type"].isin([r["event_type"] for r in ordered])]
    for _, row in remaining.iterrows():
        ordered.append(row)

    funnel_sorted = pd.DataFrame(ordered)

    col1, col2 = st.columns([2, 1])
    with col1:
        fig = go.Figure(go.Funnel(
            y=funnel_sorted["event_type"],
            x=funnel_sorted["event_count"],
            textinfo="value+percent initial",
            marker=dict(color=CATEGORY_PALETTE[:len(funnel_sorted)]),
        ))
        fig.update_layout(**PLOTLY_LAYOUT, title_text="Events by Type")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Engagement Summary**")
        for _, row in funnel_sorted.head(8).iterrows():
            st.metric(
                label=row["event_type"].replace("_", " ").title(),
                value=f"{row['event_count']:,}",
                help=f"{row['unique_sessions']:,} sessions · {row['unique_users']:,} users",
            )

st.divider()

# ── Session volume over time ──────────────────────────────────────────────────
st.subheader("Session Volume Over Time")
if not sessions_df.empty:
    sessions_df["event_date"] = pd.to_datetime(sessions_df["event_date"])
    monthly = (
        sessions_df.set_index("event_date")
                   .resample("ME")[["sessions","events"]]
                   .sum()
                   .reset_index()
    )
    monthly["month"] = monthly["event_date"].dt.strftime("%b %Y")

    fig = go.Figure()
    fig.add_bar(x=monthly["month"], y=monthly["sessions"], name="Sessions",
                marker_color=COLORS["primary"], opacity=0.8)
    fig.add_scatter(x=monthly["month"], y=monthly["events"], name="Total Events",
                    mode="lines+markers", line=dict(color=COLORS["warning"], width=2),
                    marker=dict(size=5), yaxis="y2")
    fig.update_layout(
        **PLOTLY_LAYOUT,
        yaxis=dict(title="Sessions"),
        yaxis2=dict(title="Total Events", overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.1),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Traffic source × event type ───────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Events by Traffic Source & Type")
    if not traffic_events_df.empty:
        pivot = (
            traffic_events_df.pivot_table(
                index="traffic_source", columns="event_type",
                values="event_count", aggfunc="sum", fill_value=0
            )
            .reset_index()
        )
        fig = px.bar(
            traffic_events_df,
            x="traffic_source", y="event_count", color="event_type",
            barmode="stack",
            color_discrete_sequence=CATEGORY_PALETTE,
            labels={"traffic_source":"Channel","event_count":"Events","event_type":"Event Type"},
        )
        fig.update_layout(**PLOTLY_LAYOUT, xaxis_title="", legend_title="Event Type")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top 15 Pages by Event Volume")
    if not top_uris_df.empty:
        df = top_uris_df.sort_values("event_count", ascending=True)
        fig = px.bar(df, x="event_count", y="uri", orientation="h",
                     color="event_count",
                     color_continuous_scale=[[0,"#E0E7FF"],[1,COLORS["primary"]]],
                     labels={"event_count":"Events","uri":"Page URI"})
        fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)
