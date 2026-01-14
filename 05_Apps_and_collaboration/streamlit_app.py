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
st.title("📊 Daily Sales & Weather Analysis")
st.write("売上データと天気データを同時に可視化します（アジア太平洋地域 2022-2023）")
st.write('---')

# Define target cities for Asia-Pacific region
ASIA_CITIES = ['Mumbai', 'Delhi', 'Tokyo', 'Seoul', 'Sydney', 'Melbourne']
YEARS = [2022, 2023]

# Define a function to load data from Snowflake with filters at SQL level
@st.cache_data()
def load_data():
    """
    Connects to the daily_sales_by_weather_v view and fetches filtered data.
    - Filters: Asia-Pacific cities only (Mumbai, Delhi, Tokyo, Seoul, Sydney, Melbourne)
    - Years: 2022 and 2023 only
    Reference: https://app.snowflake.com/marketplace/listing/GZSOZ1LLEL/weather-source-llc-weather-source-llc-frostbyte
    """
    # Build SQL query with filters to reduce data volume
    query = f"""
        SELECT 
            DATE,
            CITY_NAME,
            COUNTRY_DESC,
            DAILY_SALES,
            MENU_ITEM_NAME,
            AVG_TEMP_FAHRENHEIT,
            AVG_PRECIPITATION_INCHES,
            AVG_SNOWDEPTH_INCHES,
            MAX_WIND_SPEED_MPH
        FROM tb_101.analytics.daily_sales_by_weather_v
        WHERE CITY_NAME IN ('Mumbai', 'Delhi', 'Tokyo', 'Seoul', 'Sydney', 'Melbourne')
          AND YEAR(DATE) IN (2022, 2023)
          AND DAILY_SALES > 0
    """
    df = session.sql(query).to_pandas()
    # Convert DATE column to datetime for proper filtering
    df['DATE'] = pd.to_datetime(df['DATE'])
    return df

# Load the data
with st.spinner('データを読み込んでいます...'):
    sales_weather_data = load_data()

st.success(f"✅ {len(sales_weather_data):,} 件のデータを読み込みました")

# --- Sidebar Filters ---
st.sidebar.header("🔍 フィルター設定")

# 1. Country Selection (Dropdown)
countries = sorted(sales_weather_data['COUNTRY_DESC'].dropna().unique().tolist())
selected_country = st.sidebar.selectbox(
    "🌍 国を選択",
    options=countries,
    index=countries.index('Japan') if 'Japan' in countries else 0
)

# Filter data by selected country
country_filtered_data = sales_weather_data[sales_weather_data['COUNTRY_DESC'] == selected_country]

# 2. City Selection (Dropdown) - filtered by country
cities = sorted(country_filtered_data['CITY_NAME'].dropna().unique().tolist())
selected_city = st.sidebar.selectbox(
    "🏙️ 都市を選択",
    options=cities
)

# Filter data by selected city
city_filtered_data = country_filtered_data[country_filtered_data['CITY_NAME'] == selected_city]

# 3. Menu Item Selection (Dropdown)
menu_items = sorted(city_filtered_data['MENU_ITEM_NAME'].dropna().unique().tolist())
selected_menu_item = st.sidebar.selectbox(
    "🍽️ メニューアイテムを選択",
    options=menu_items
)

# Filter data by selected menu item
filtered_data = city_filtered_data[city_filtered_data['MENU_ITEM_NAME'] == selected_menu_item]

# 4. Date Range Selection (Slider)
if len(filtered_data) > 0:
    min_date = filtered_data['DATE'].min().date()
    max_date = filtered_data['DATE'].max().date()
    
    date_range = st.sidebar.slider(
        "📅 期間を選択",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="YYYY-MM-DD"
    )
    
    # Filter data by selected date range
    filtered_data = filtered_data[
        (filtered_data['DATE'].dt.date >= date_range[0]) &
        (filtered_data['DATE'].dt.date <= date_range[1])
    ]

# 5. Weather Metric Selection
weather_metrics = {
    '🌡️ 気温 (°F)': 'AVG_TEMP_FAHRENHEIT',
    '🌧️ 降水量 (inches)': 'AVG_PRECIPITATION_INCHES',
    '❄️ 積雪深 (inches)': 'AVG_SNOWDEPTH_INCHES',
    '💨 最大風速 (mph)': 'MAX_WIND_SPEED_MPH'
}
selected_weather_label = st.sidebar.selectbox(
    "🌤️ 天気指標を選択",
    options=list(weather_metrics.keys())
)
selected_weather_metric = weather_metrics[selected_weather_label]

st.sidebar.write('---')
st.sidebar.info(f"📍 {selected_country} - {selected_city}\n\n🍽️ {selected_menu_item}")

# --- Main Content ---

if len(filtered_data) == 0:
    st.warning("選択した条件に該当するデータがありません。フィルター条件を変更してください。")
else:
    # Prepare data for visualization
    daily_data = filtered_data.groupby('DATE').agg({
        'DAILY_SALES': 'sum',
        selected_weather_metric: 'mean'
    }).reset_index()
    
    # Display summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        total_sales = daily_data['DAILY_SALES'].sum()
        st.metric("💰 総売上", f"${total_sales:,.0f}")
    with col2:
        avg_weather = daily_data[selected_weather_metric].mean()
        st.metric(f"平均 {selected_weather_label}", f"{avg_weather:.1f}")
    with col3:
        data_points = len(daily_data)
        st.metric("📊 データポイント数", f"{data_points} 日")
    
    st.write('---')
    
    # --- Dual-Axis Chart with Altair ---
    st.subheader(f"📈 {selected_menu_item} の売上と{selected_weather_label}の推移")
    
    # Base chart
    base = alt.Chart(daily_data).encode(
        x=alt.X('DATE:T', 
                axis=alt.Axis(title='日付', format='%Y-%m-%d'),
                title='日付')
    )
    
    # Sales line (Left Y-axis) - Blue
    sales_line = base.mark_line(
        color='#1f77b4',
        strokeWidth=2
    ).encode(
        y=alt.Y('DAILY_SALES:Q',
                axis=alt.Axis(title='売上 ($)', titleColor='#1f77b4'),
                title='売上 ($)')
    )
    
    sales_points = base.mark_circle(
        color='#1f77b4',
        size=50
    ).encode(
        y=alt.Y('DAILY_SALES:Q'),
        tooltip=[
            alt.Tooltip('DATE:T', title='日付', format='%Y-%m-%d'),
            alt.Tooltip('DAILY_SALES:Q', title='売上', format='$,.0f'),
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
                axis=alt.Axis(title=selected_weather_label, titleColor='#ff7f0e'),
                title=selected_weather_label)
    )
    
    weather_points = base.mark_circle(
        color='#ff7f0e',
        size=50
    ).encode(
        y=alt.Y(f'{selected_weather_metric}:Q'),
        tooltip=[
            alt.Tooltip('DATE:T', title='日付', format='%Y-%m-%d'),
            alt.Tooltip('DAILY_SALES:Q', title='売上', format='$,.0f'),
            alt.Tooltip(f'{selected_weather_metric}:Q', title=selected_weather_label, format='.1f')
        ]
    )
    
    # Combine charts with independent Y-axes (dual-axis)
    combined_chart = alt.layer(
        sales_line + sales_points,
        weather_line + weather_points
    ).resolve_scale(
        y='independent'  # This creates the dual-axis effect
    ).properties(
        width='container',
        height=450,
        title=f'{selected_city}, {selected_country} - {selected_menu_item}'
    )
    
    # Display the chart
    st.altair_chart(combined_chart, use_container_width=True)
    
    # Legend explanation
    st.markdown("""
    **凡例:**
    - 🔵 **青色の実線**: 売上 ($)
    - 🟠 **オレンジの破線**: 天気指標
    """)
    
    st.write('---')
    
    # --- Data Table ---
    with st.expander("📋 データテーブルを表示"):
        display_data = daily_data.copy()
        display_data['DATE'] = display_data['DATE'].dt.strftime('%Y-%m-%d')
        display_data.columns = ['日付', '売上 ($)', selected_weather_label]
        st.dataframe(display_data, use_container_width=True)
