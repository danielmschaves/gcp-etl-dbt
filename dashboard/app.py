import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
import os
from datetime import datetime

# DuckDB connection setup
@st.cache_resource
def get_duckdb_connection():
    db_path = 'transform/gcp_etl_dbt/dbt.duckdb'  # Ensure this path is correct
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

# Define the queries for loading data from dbt models
revenue_over_time_query = "SELECT * FROM main_gold.mart_revenue_over_time;"
top_selling_products_query = "SELECT * FROM main_gold.mart_top_selling_products;"
sales_by_category_query = "SELECT * FROM main_gold.mart_sales_by_category;"
customer_demographics_query = "SELECT * FROM main_gold.mart_customer_demographics;"
top_customers_query = "SELECT * FROM main_gold.mart_top_customers;"
order_status_distribution_query = "SELECT * FROM main_gold.mart_order_status_distribution;"
average_time_to_ship_query = "SELECT * FROM main_gold.mart_average_time_to_ship;"
sales_by_country_query = "SELECT * FROM main_gold.mart_sales_by_country;"

# Load all required tables
revenue_over_time = load_data(revenue_over_time_query)
top_selling_products = load_data(top_selling_products_query)
sales_by_category = load_data(sales_by_category_query)
customer_demographics = load_data(customer_demographics_query)
top_customers = load_data(top_customers_query)
order_status_distribution = load_data(order_status_distribution_query)
average_time_to_ship = load_data(average_time_to_ship_query)
sales_by_country = load_data(sales_by_country_query)

# Set min_date and max_date from revenue_over_time
min_date = revenue_over_time['date'].min()
max_date = revenue_over_time['date'].max()

if isinstance(min_date, pd.Timestamp) and isinstance(max_date, pd.Timestamp):
    st.write(f"Min Date: {min_date}, Max Date: {max_date}")
else:
    st.error(f"Error: min_date and max_date are not datetime objects. Min Date: {min_date}, Max Date: {max_date}")
    st.stop()

st.title("The Look E-commerce Dashboard")

# Date range selector
start_date, end_date = st.date_input("Select date range", [min_date, max_date])

# Convert start_date and end_date to datetime
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Filter data based on date range
filtered_revenue_over_time = revenue_over_time[
    (revenue_over_time['date'] >= start_date) & 
    (revenue_over_time['date'] <= end_date)
]

# Sales Overview
st.header("Sales Overview")
col1, col2, col3 = st.columns(3)

total_revenue = filtered_revenue_over_time['total_revenue'].sum()
total_orders = len(revenue_over_time['date'].unique())  # Assuming each date corresponds to an order
average_order_value = total_revenue / total_orders

col1.metric("Total Revenue", f"${total_revenue:,.2f}")
col2.metric("Total Orders", f"{total_orders:,}")
col3.metric("Average Order Value", f"${average_order_value:.2f}")

# Revenue over time
fig = px.line(filtered_revenue_over_time, x='date', y='total_revenue', title="Daily Revenue")
st.plotly_chart(fig)

# Product Performance
st.header("Product Performance")

# Top selling products
fig = px.bar(top_selling_products, x='product_name', y='total_items_sold', title="Top 10 Selling Products")
st.plotly_chart(fig)

# Sales by category
fig = px.pie(sales_by_category, values='total_sales', names='category', title="Sales by Category")
st.plotly_chart(fig)

# Customer Analysis
st.header("Customer Analysis")

# Customer demographics
fig = px.pie(customer_demographics, values='user_count', names='gender', title="Customer Gender Distribution")
st.plotly_chart(fig)

# Top customers by revenue
fig = px.bar(top_customers, x='user_id', y='total_revenue', title="Top 10 Customers by Revenue")
st.plotly_chart(fig)

# Order Fulfillment
st.header("Order Fulfillment")

# Order status distribution
fig = px.pie(order_status_distribution, values='order_count', names='status', title="Order Status Distribution")
st.plotly_chart(fig)

# Average time to ship
st.metric("Average Time to Ship", f"{average_time_to_ship['avg_ship_time'].iloc[0]:.2f} days")

# Geographical Insights
st.header("Geographical Insights")

# Sales by country
fig = px.bar(sales_by_country, x='country', y='total_sales', title="Sales by Country")
st.plotly_chart(fig)

st.write("Dashboard updated successfully!")
