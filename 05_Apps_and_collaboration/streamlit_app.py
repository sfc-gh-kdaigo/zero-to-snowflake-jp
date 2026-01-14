# Welcome to Streamlit in Snowflake!

# Import necessary libraries
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# --- App Setup and Data Loading ---

# Get the active Snowpark session to interact with Snowflake.
session = get_active_session()

# Set the title of the Streamlit application
st.title("📊 Menu Item Sales & Weather in Japan")
st.write("2022年2月 東京の売上データと天気データを可視化します")
st.write('---')

# Load sales data (original table)
@st.cache_data()
def load_sales_data():
    """
    Load sales data from the original Japan menu item sales table for February 2022.
    """
    df = session.table("tb_101.analytics.japan_menu_item_sales_feb_2022").to_pandas()
    return df

# Load weather data (filtered to Tokyo, Feb 2022)
@st.cache_data()
def load_weather_data():
    """
    Load weather data from daily_sales_by_weather_v, filtered to Tokyo and February 2022.
    Reference: https://app.snowflake.com/marketplace/listing/GZSOZ1LLEL/weather-source-llc-weather-source-llc-frostbyte
    """
    query = """
        SELECT 
            DATE,
            AVG_TEMP_FAHRENHEIT,
            AVG_PRECIPITATION_INCHES,
            AVG_SNOWDEPTH_INCHES,
            MAX_WIND_SPEED_MPH
        FROM tb_101.analytics.daily_sales_by_weather_v
        WHERE CITY_NAME = 'Tokyo'
          AND YEAR(DATE) = 2022
          AND MONTH(DATE) = 2
    """
    df = session.sql(query).to_pandas()
    df['DATE'] = pd.to_datetime(df['DATE'])
    # Get daily average weather (aggregate by date)
    weather_daily = df.groupby('DATE').agg({
        'AVG_TEMP_FAHRENHEIT': 'mean',
        'AVG_PRECIPITATION_INCHES': 'mean',
        'AVG_SNOWDEPTH_INCHES': 'mean',
        'MAX_WIND_SPEED_MPH': 'mean'
    }).reset_index()
    return weather_daily

# Load data with spinner
with st.spinner('データを読み込んでいます...'):
    japan_sales = load_sales_data()
    weather_data = load_weather_data()

st.success(f"✅ 売上データ: {len(japan_sales):,} 件 / 天気データ: {len(weather_data):,} 日分")

# --- Sidebar Filters ---
st.sidebar.header("🔍 フィルター設定")

# Menu item selection
menu_item_names = sorted(japan_sales['MENU_ITEM_NAME'].unique().tolist())
selected_menu_item = st.sidebar.selectbox("🍽️ メニューアイテムを選択", options=menu_item_names)

# Weather metric selection
weather_metrics = {
    '🌡️ 気温 (°F)': 'AVG_TEMP_FAHRENHEIT',
    '🌧️ 降水量 (inches)': 'AVG_PRECIPITATION_INCHES',
    '❄️ 積雪深 (inches)': 'AVG_SNOWDEPTH_INCHES',
    '💨 最大風速 (mph)': 'MAX_WIND_SPEED_MPH'
}
selected_weather_label = st.sidebar.selectbox("🌤️ 天気指標を選択", options=list(weather_metrics.keys()))
selected_weather_metric = weather_metrics[selected_weather_label]

st.sidebar.write('---')
st.sidebar.info(f"🍽️ {selected_menu_item}\n\n{selected_weather_label}")

# --- Data Preparation ---

# Filter sales data by selected menu item
menu_item_sales = japan_sales[japan_sales['MENU_ITEM_NAME'] == selected_menu_item]

# Group by date for daily totals
daily_sales = menu_item_sales.groupby('DATE')['ORDER_TOTAL'].sum().reset_index()
daily_sales['DATE'] = pd.to_datetime(daily_sales['DATE'])

# Merge sales and weather data
merged_data = pd.merge(daily_sales, weather_data, on='DATE', how='inner')

# --- Display Metrics ---
col1, col2, col3 = st.columns(3)
with col1:
    total_sales = merged_data['ORDER_TOTAL'].sum()
    st.metric("💰 総売上", f"${total_sales:,.0f}")
with col2:
    avg_weather = merged_data[selected_weather_metric].mean()
    st.metric(f"平均 {selected_weather_label}", f"{avg_weather:.1f}")
with col3:
    data_points = len(merged_data)
    st.metric("📊 データポイント", f"{data_points} 日")

st.write('---')

# --- Dual-Axis Chart ---
st.subheader(f"📈 {selected_menu_item} の売上と{selected_weather_label}の推移")

# Base chart
base = alt.Chart(merged_data).encode(
    x=alt.X('DATE:T', 
            axis=alt.Axis(title='日付', format='%b %d'),
            title='日付')
)

# Sales line (Left Y-axis) - Blue
sales_line = base.mark_line(
    color='#1f77b4',
    strokeWidth=2
).encode(
    y=alt.Y('ORDER_TOTAL:Q',
            axis=alt.Axis(title='売上 ($)', titleColor='#1f77b4'))
)

sales_points = base.mark_circle(
    color='#1f77b4',
    size=60
).encode(
    y=alt.Y('ORDER_TOTAL:Q'),
    tooltip=[
        alt.Tooltip('DATE:T', title='日付', format='%Y-%m-%d'),
        alt.Tooltip('ORDER_TOTAL:Q', title='売上', format='$,.0f'),
        alt.Tooltip(f'{selected_weather_metric}:Q', title=selected_weather_label, format='.1f')
    ]
)

# Weather line (Right Y-axis) - Orange
weather_line = base.mark_line(
    color='#ff7f0e',
    strokeWidth=2,
    strokeDash=[5, 5]
).encode(
    y=alt.Y(f'{selected_weather_metric}:Q',
            axis=alt.Axis(title=selected_weather_label, titleColor='#ff7f0e'))
)

weather_points = base.mark_circle(
    color='#ff7f0e',
    size=60
).encode(
    y=alt.Y(f'{selected_weather_metric}:Q'),
    tooltip=[
        alt.Tooltip('DATE:T', title='日付', format='%Y-%m-%d'),
        alt.Tooltip('ORDER_TOTAL:Q', title='売上', format='$,.0f'),
        alt.Tooltip(f'{selected_weather_metric}:Q', title=selected_weather_label, format='.1f')
    ]
)

# Combine with independent Y-axes (dual-axis effect)
combined_chart = alt.layer(
    sales_line + sales_points,
    weather_line + weather_points
).resolve_scale(
    y='independent'
).properties(
    width='container',
    height=450,
    title=f'Tokyo - {selected_menu_item} (February 2022)'
)

# Display the chart
st.altair_chart(combined_chart, use_container_width=True)

# Legend
st.markdown("""
**凡例:**
- 🔵 **青色の実線**: 売上 ($)
- 🟠 **オレンジの破線**: 天気指標
""")

st.write('---')

# --- Data Table ---
with st.expander("📋 データテーブルを表示"):
    display_data = merged_data[['DATE', 'ORDER_TOTAL', selected_weather_metric]].copy()
    display_data['DATE'] = display_data['DATE'].dt.strftime('%Y-%m-%d')
    display_data.columns = ['日付', '売上 ($)', selected_weather_label]
    st.dataframe(display_data, use_container_width=True)
