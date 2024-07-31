import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import os
from datetime import datetime, timedelta

# Set page config at the very beginning
st.set_page_config(page_title="The Look E-commerce Dashboard", layout="wide")

# DuckDB connection setup
@st.cache_resource
def get_duckdb_connection():
    db_path = 'transform/gcp_etl_dbt/dbt.duckdb'
    if not os.path.exists(db_path):
        st.error(f"Database file not found: {db_path}")
        st.stop()
    return duckdb.connect(db_path)

conn = get_duckdb_connection()

# Load data from DuckDB
@st.cache_data
def load_data(query):
    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            st.warning(f"Query executed but returned no data. Check if the table is empty.")
        return df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()

# Define queries
queries = {
    "revenue_over_time": "SELECT * FROM main_gold.mart_revenue_over_time;",
    "top_selling_products": "SELECT * FROM main_gold.mart_top_selling_products;",
    "sales_by_category": "SELECT * FROM main_gold.mart_sales_by_category;",
    "customer_demographics": "SELECT * FROM main_gold.mart_customer_demographics;",
    "top_customers": "SELECT * FROM main_gold.mart_top_customers;",
    "order_status_distribution": "SELECT * FROM main_gold.mart_order_status_distribution;",
    "average_time_to_ship": "SELECT * FROM main_gold.mart_average_time_to_ship;",
    "sales_by_country": "SELECT * FROM main_gold.mart_sales_by_country;",
    "sales_by_state": "SELECT * FROM main_gold.mart_sales_by_state;",
}

# Load all required tables
data = {key: load_data(query) for key, query in queries.items()}

# Set min_date and max_date from revenue_over_time
min_date = data["revenue_over_time"]['date'].min()
max_date = data["revenue_over_time"]['date'].max()

st.title("The Look E-commerce Dashboard")

# Date range selector
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", min_date)
with col2:
    end_date = st.date_input("End Date", max_date)

# Convert start_date and end_date to datetime
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Filter data based on date range
filtered_revenue_over_time = data["revenue_over_time"][
    (data["revenue_over_time"]['date'] >= start_date) & 
    (data["revenue_over_time"]['date'] <= end_date)
]

# Sales Overview
st.header("Sales Overview")
col1, col2, col3, col4 = st.columns(4)

total_revenue = filtered_revenue_over_time['total_revenue'].sum()
total_orders = len(filtered_revenue_over_time['date'].unique())
average_order_value = total_revenue / total_orders if total_orders > 0 else 0
revenue_growth = ((filtered_revenue_over_time['total_revenue'].iloc[-1] - filtered_revenue_over_time['total_revenue'].iloc[0]) / filtered_revenue_over_time['total_revenue'].iloc[0]) * 100

col1.metric("Total Revenue", f"${total_revenue:,.2f}")
col2.metric("Total Orders", f"{total_orders:,}")
col3.metric("Average Order Value", f"${average_order_value:.2f}")
col4.metric("Revenue Growth", f"{revenue_growth:.2f}%")

# Revenue over time
fig = px.line(filtered_revenue_over_time, x='date', y='total_revenue', title="Daily Revenue")
fig.update_layout(xaxis_title="Date", yaxis_title="Revenue ($)")
st.plotly_chart(fig, use_container_width=True)

# Product Performance
st.header("Product Performance")

col1, col2 = st.columns(2)

with col1:
    # Top selling products
    fig = px.bar(data["top_selling_products"], x='product_name', y='total_items_sold', title="Top 10 Selling Products")
    fig.update_layout(xaxis_title="Product", yaxis_title="Items Sold")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Sales by category
    fig = px.pie(data["sales_by_category"], values='total_sales', names='category', title="Sales by Category")
    st.plotly_chart(fig, use_container_width=True)

# Customer Analysis
st.header("Customer Analysis")

col1, col2 = st.columns(2)

with col1:
    # Customer demographics
    fig = px.pie(data["customer_demographics"], values='user_count', names='gender', title="Customer Gender Distribution")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Top customers by revenue
    fig = px.bar(data["top_customers"], x='user_id', y='total_revenue', title="Top 10 Customers by Revenue")
    fig.update_layout(xaxis_title="User ID", yaxis_title="Total Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

# Order Fulfillment
st.header("Order Fulfillment")

col1, col2 = st.columns(2)

with col1:
    # Order status distribution
    fig = px.pie(data["order_status_distribution"], values='order_count', names='status', title="Order Status Distribution")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Average time to ship
    avg_ship_time = data["average_time_to_ship"]['avg_ship_time'].iloc[0]
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = avg_ship_time,
        title = {'text': "Average Time to Ship (Days)"},
        gauge = {'axis': {'range': [None, 10]},
                 'steps' : [
                     {'range': [0, 3], 'color': "lightgreen"},
                     {'range': [3, 7], 'color': "yellow"},
                     {'range': [7, 10], 'color': "red"}],
                 'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 7}}))
    st.plotly_chart(fig, use_container_width=True)

# Geographical Insights
st.header("Geographical Insights")

col1, col2 = st.columns(2)

with col1:
    # Sales by country
    fig = px.choropleth(data["sales_by_country"], locations='country', locationmode='country names', 
                        color='total_sales', hover_name='country', color_continuous_scale="Viridis",
                        title="Sales by Country")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Sales by state (assuming US states)
    fig = px.choropleth(data["sales_by_state"], locations='state', locationmode='USA-states', 
                        color='total_sales', hover_name='state', scope="usa", color_continuous_scale="Viridis",
                        title="Sales by State (US)")
    st.plotly_chart(fig, use_container_width=True)

# Additional Insights
st.header("Additional Insights")

col1, col2 = st.columns(2)

with col1:
    # Monthly revenue trend
    monthly_revenue = filtered_revenue_over_time.set_index('date').resample('M')['total_revenue'].sum().reset_index()
    fig = px.line(monthly_revenue, x='date', y='total_revenue', title="Monthly Revenue Trend")
    fig.update_layout(xaxis_title="Month", yaxis_title="Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Day of week analysis
    filtered_revenue_over_time['day_of_week'] = filtered_revenue_over_time['date'].dt.day_name()
    day_of_week_revenue = filtered_revenue_over_time.groupby('day_of_week')['total_revenue'].mean().reset_index()
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_of_week_revenue['day_of_week'] = pd.Categorical(day_of_week_revenue['day_of_week'], categories=day_order, ordered=True)
    day_of_week_revenue = day_of_week_revenue.sort_values('day_of_week')
    fig = px.bar(day_of_week_revenue, x='day_of_week', y='total_revenue', title="Average Daily Revenue by Day of Week")
    fig.update_layout(xaxis_title="Day of Week", yaxis_title="Average Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

st.write("Dashboard updated successfully!")