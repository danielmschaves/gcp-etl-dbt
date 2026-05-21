import os
import duckdb
import pandas as pd
import streamlit as st


@st.cache_resource
def get_conn():
    db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "transform", "gcp_etl_dbt", "dbt.duckdb",
    )
    if not os.path.exists(db_path):
        st.error(f"Database not found: {db_path}")
        st.stop()
    return duckdb.connect(db_path, read_only=True)


@st.cache_data(ttl=3600)
def load(_conn, query: str) -> pd.DataFrame:
    try:
        df = _conn.execute(query).fetchdf()
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()
