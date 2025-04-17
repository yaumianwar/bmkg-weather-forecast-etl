import streamlit as st

st.set_page_config(layout="wide")
import pandas as pd
from datetime import datetime, timedelta

from utils import get_forecast_data
from component import get_temperature_facet_grid_figure
import plotly.express as px

# get forecast data
df = get_forecast_data()

# get district filter list
districts = df['kecamatan'].unique().tolist()
district_dropdown_items = ['Semua Kecamatan'] + districts

# get date filter list
today_datetime = datetime.now()
today_date = datetime.now().date()
tomorrow_date = (today_datetime + timedelta(days=1)).date()
day_after_tomorrow_date = (today_datetime + timedelta(days=2)).date()
date_dropdown_items = [today_date.strftime('%d %B %Y'), tomorrow_date.strftime('%d %B %Y'), day_after_tomorrow_date.strftime('%d %B %Y')]


st.title("Prakiraan Cuaca Kabupaten Bulukumba")

district_dropdown, date_dropdown, _ = st.columns(3)

with district_dropdown:
    selected_district = st.selectbox("Pilih Kecamatan:",(district_dropdown_items))

with date_dropdown:
    selected_date = st.segmented_control(
    "Pilih Tanggal", date_dropdown_items, default=date_dropdown_items[0]
) 

# filter forecast data by selected date
df_filtered = df[df['date'] == selected_date]

# group by district and local datetime
temp_facet_graph_data_district = df_filtered.groupby(['kecamatan', 'local_datetime'], as_index=False).agg({'t': ['mean'], 'hu': ['mean'], 'weather_desc': pd.Series.mode, 'image': pd.Series.mode})
temp_facet_graph_data_district.columns = list(map('_'.join, temp_facet_graph_data_district.columns.values))

fig = get_temperature_facet_grid_figure(temp_facet_graph_data_district, 'local_datetime_', 't_mean', 'kecamatan_')

# filter forecast data by kecamatan/district
if selected_district != 'Semua Kecamatan':

    temp_facet_graph_data_sub_district = df_filtered[df_filtered['kecamatan'] == selected_district].sort_values(['desa', 'local_datetime'])

    fig = get_temperature_facet_grid_figure(temp_facet_graph_data_sub_district, 'local_datetime', 't', 'desa')

weather, avg_temp, avg_hu, avg_vs = st.columns(4, vertical_alignment="center")

agg_data = df_filtered if selected_district == 'Semua Kecamatan' else temp_facet_graph_data_sub_district

# get aggregate data for metrics component
temp_hu_agg_data = agg_data.agg({'t': ['mean'], 'hu': ['mean']}).to_dict('records')[0]
weather_agg_data = agg_data.agg({'weather_desc': pd.Series.mode, 'image': pd.Series.mode, 'vs_text': pd.Series.mode}).to_dict('records')[0]

# create custom aggregate weather card
def agg_weather_card(img, desc):
    return f"""
    <div style="
        display: flex;
        flex-direction: column;
        align-items: center;
        padding: 20px;
        background-color: white;
        margin: 10px;
        width: 300px;
    ">
        <img src="{img}" alt="Weather Icon" style="width: 80px; height: 80px;">
        <div style="font-size: 16px; margin-bottom: 5px;">{desc}</div>
    </div>
    """

with weather:
    st.markdown(agg_weather_card(weather_agg_data['image'], weather_agg_data['weather_desc']), unsafe_allow_html=True)

avg_temp.metric("Rata-rata Suhu", f"{round(temp_hu_agg_data['t'])} °C")
avg_hu.metric("Rata-rata Kelembapan", f"{round(temp_hu_agg_data['hu'])} %")
avg_vs.metric("Rata-rata Jarak Pandang", weather_agg_data['vs_text'])


st.subheader("Suhu Kecamatan/Kelurahan")
st.plotly_chart(fig)

# create district/sub-district filter
st.subheader("Prakiraan Cuaca Kecamatan/Kelurahan (per 3 Jam)")
weather_dropdown_items = districts if selected_district == 'Semua Kecamatan' else df[df['kecamatan'] == selected_district]['desa'].unique().tolist()
weather_dropdown, _ , _ = st.columns(3)
with weather_dropdown:
    weather_selected_district = st.selectbox("Pilih Kecamatan/Kelurahan:",(weather_dropdown_items))

# get weather forecast data every 3 hours in 1 day
hours = [1,4,7,10,13,16,19,22]
forecast_filtered = df[(df['date'] == selected_date) & ((df['desa'] == weather_selected_district) | (df['kecamatan'] == weather_selected_district)) & (df['hour'].isin(hours))]
forecast_groupby_hour = forecast_filtered.groupby(['hour'], as_index=False).agg({'t': ['mean'], 'hu': ['mean'], 'weather_desc': pd.Series.mode, 'image': pd.Series.mode})
forecast_groupby_hour.columns = list(map('_'.join, forecast_groupby_hour.columns.values))
forecast_groupby_hour_dict = forecast_groupby_hour.to_dict('records')


# create weather information card custom component
def weather_card(data):
    return f"""
    <div style="
        display: flex;
        flex-direction: column;
        align-items: center;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        background-color: white;
        margin: 10px;
        width: 150px;
    ">
        <div style="font-size: 24px; color: #3498db; margin-bottom: 10px;">{data['hour_']:02d}.00</div>
        <img src="{data['image_mode']}" alt="Weather Icon" style="width: 50px; height: 50px; margin-bottom: 10px;">
        <div style="font-size: 16px; color: #3498db; margin-bottom: 5px;">{data['weather_desc_mode']}</div>
        <div style="font-size: 20px; color: #3498db;">{round(data['t_mean'])}°C</div>
    </div>
    """

# create weather forecast card columns
num_columns = 8
columns = st.columns(num_columns)

# loop through data and render cards
for index, data in enumerate(forecast_groupby_hour_dict):
    with columns[index % num_columns]:
        st.markdown(weather_card(data), unsafe_allow_html=True)
