import streamlit as st
import pandas as pd
from pinotdb import connect
import time

# Connect to Pinot
conn = connect(host='pinot-broker', port=8099, path='/query/sql', scheme='http')

st.set_page_config(layout="wide")
st.title(" 🚀 DSP Real-Time Ad Dashboard")

# Auto-refresh loop
if st.button('Refresh Data'):
    st.rerun()

col1, col2 = st.columns(2)

# QUERY 1: Real-time KPIs
with col1:
    st.subheader("📊 Campaign Performance")
    query_kpi = """
        SELECT 
            count(*) as impressions,
            sum(is_clicked) as clicks,
            sum(is_clicked) / count(*) as ctr,
            avg(win_price) as avg_cpm
        FROM dsp_impressions
    """
    df_kpi = pd.read_sql(query_kpi, conn)
    
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Total Impressions", f"{df_kpi['impressions'][0]:,}")
    kpi2.metric("Total Clicks", f"{df_kpi['clicks'][0]:,}")
    kpi3.metric("Global CTR", f"{df_kpi['ctr'][0]:.2%}")

# QUERY 2: Performance by Category
with col2:
    st.subheader("🎯 CTR by Category")
    query_cat = """
        SELECT 
            site_category,
            sum(is_clicked) / count(*) as ctr
        FROM dsp_impressions
        GROUP BY site_category
        ORDER BY ctr DESC
    """
    df_cat = pd.read_sql(query_cat, conn)
    st.bar_chart(df_cat.set_index('site_category'))

# QUERY 3: Recent Transactions
st.subheader("⏳ Recent Wins Stream")
query_recent = """
    SELECT event_time, user_id, site_category, win_price, is_clicked 
    FROM dsp_impressions 
    ORDER BY event_time DESC 
    LIMIT 10
"""
df_recent = pd.read_sql(query_recent, conn)
st.dataframe(df_recent)

st.caption(f"Data fetched from Apache Pinot at {time.strftime('%H:%M:%S')}")
