import streamlit as st

st.set_page_config(
    page_title="The Look E-commerce",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = [
    st.Page("pages/1_overview.py",    title="Executive Overview",    icon="📊"),
    st.Page("pages/2_products.py",    title="Product Intelligence",  icon="📦"),
    st.Page("pages/3_customers.py",   title="Customer Analytics",    icon="👥"),
    st.Page("pages/4_operations.py",  title="Operations",            icon="🚚"),
]

pg = st.navigation(pages)
pg.run()
