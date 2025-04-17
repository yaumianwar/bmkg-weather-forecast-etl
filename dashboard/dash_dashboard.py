from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc

import pandas as pd

from datetime import datetime, timedelta

from component import get_card_component, get_card_with_image_component, get_weather_card_component, get_temperature_facet_grid_figure
from utils import get_forecast_data

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
date_dropdown_items = [
    {'label': today_date.strftime('%d %B %Y'), 'value': today_date},
    {'label':tomorrow_date.strftime('%d %B %Y'), 'value': tomorrow_date},
    {'label':day_after_tomorrow_date.strftime('%d %B %Y'), 'value': day_after_tomorrow_date}
]


app = Dash(
    external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True
)
district_item_dropdown = [dbc.DropdownMenuItem(i, id=i) for i in districts]

# Requires Dash 2.17.0 or later
app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H1("Prakiraan Cuaca Kabupaten Bulukumba")
            ], className="header"
        ),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(html.H5("Kecamatan:"), width="auto", className="filter-name"),
                        dbc.Col(
                            dcc.Dropdown(district_dropdown_items, value="Semua Kecamatan", id='district_dropdown'), style={"padding-top": "5px"}, width=2
                        ),
                        dbc.Col(html.H5("Hari/Tanggal:"), width="auto", className="filter-name"),
                        dbc.Col(
                            dbc.RadioItems(
                                id="date_dropdown",
                                className="btn-group",
                                inputClassName="btn-check",
                                labelClassName="btn btn-outline-black",
                                labelCheckedClassName="active",
                                options=date_dropdown_items,
                                value=today_date,
                            ),
                            style={"padding-top": "5px"}, 
                            width=5
                        ),
                        
                    ],
                    justify="start"
                )
            ], className="section"
        ),
        html.Div(
            [
                
                dbc.Row([
                    dbc.Col(html.Div(
                        id="avg_weather_card", className="image-card"
                    )),
                    dbc.Col(html.Div(
                        id="avg_temp_card", className="card"
                    )),
                    dbc.Col(html.Div(
                        id="avg_hu_card", className="card"
                    )),
                    dbc.Col(html.Div(
                        id="avg_vs_card", className="card"
                    ))
                ])
            ], 
        ),
        html.Div(
            [
                dbc.Row(
                    html.H3("Suhu Kecamatan/Kelurahan")
                ),
                dbc.Row(
                    dcc.Graph(figure={}, id="temperature_facet_plot")
                )
            ], className="section"
        ),
        html.Div(
            [
                dbc.Row(
                    html.H3("Prakiraan Cuaca Kecamatan/Kelurahan (per 3 Jam)")
                ),
                dbc.Row([
                    dbc.Col(html.H5("Kecamatan/Kelurahan:"), width="auto", style={"align-self": "center", "align-content": "center", "padding-top": "12px"}),
                    dbc.Col(
                        id="weather_district_dropdown", style={"padding-top": "5px"}, width=2
                    )
                ]),
                html.Div(
                    id="weather_forecast", style={"padding": "20px 10px"}
                )
            ], className="section"
        )
    
])

# callback to update metrics and facet graph of temperature 
# based on selected district and date
@callback(
    [Output('temperature_facet_plot', 'figure'),
    Output('avg_weather_card', 'children'),
    Output('avg_temp_card', 'children'),
    Output('avg_hu_card', 'children'),
    Output('avg_vs_card', 'children')],
    [Input('district_dropdown', 'value'),
    Input('date_dropdown', 'value')]
)
def update_output(selected_district, selected_date):
    # filter forecast data by selected date
    df_filtered = df[df['date'] == selected_date]

    # group by district and local datetime
    temp_facet_graph_data_district = df_filtered.groupby(['kecamatan', 'local_datetime'], as_index=False).agg({'t': ['mean'], 'hu': ['mean'], 'weather_desc': pd.Series.mode, 'image': pd.Series.mode})
    temp_facet_graph_data_district.columns = list(map('_'.join, temp_facet_graph_data_district.columns.values))
    
    fig = get_temperature_facet_grid_figure(temp_facet_graph_data_district, 'local_datetime_', 't_mean', 'kecamatan_')

    # filter forecast data by district
    if selected_district != 'Semua Kecamatan':

        temp_facet_graph_data_sub_district = df_filtered[df_filtered['kecamatan'] == selected_district].sort_values(['desa', 'local_datetime'])

        fig = get_temperature_facet_grid_figure(temp_facet_graph_data_sub_district, 'local_datetime', 't', 'desa')

    # get aggregate data for metrics component
    agg_data = df_filtered if selected_district == 'Semua Kecamatan' else temp_facet_graph_data_sub_district
    temp_hu_agg_data = agg_data.agg({'t': ['mean'], 'hu': ['mean']}).to_dict('records')[0]
    weather_agg_data = agg_data.agg({'weather_desc': pd.Series.mode, 'image': pd.Series.mode, 'vs_text': pd.Series.mode}).to_dict('records')[0]

    avg_weather_card = get_card_with_image_component(weather_agg_data['weather_desc'], weather_agg_data['image'])
    avg_temp_card = get_card_component("Rata-rata suhu", f"{round(temp_hu_agg_data['t'])}°C")
    avg_hu_card = get_card_component("Rata-rata Kelempaban", f"{round(temp_hu_agg_data['hu'])} %")
    avg_vs_card = get_card_component("Rata-rata Jarak Pandang", weather_agg_data['vs_text'])

    return fig, avg_weather_card, avg_temp_card, avg_hu_card, avg_vs_card


# callback to update weather forecast district/sub-district dropdown items
@callback(
    Output('weather_district_dropdown', 'children'),
    [Input('district_dropdown', 'value')]
)
def update_weather_forecast_dropdown(selected_district):
    dropdown_items = districts

    if selected_district != "Semua Kecamatan":

        dropdown_items = df[df['kecamatan'] == selected_district]['desa'].unique().tolist()

    drodpdown_component = html.Div(
        dcc.Dropdown(dropdown_items, value=dropdown_items[0], id='weather_district_dropdown_output'), style={"padding-top": "5px"}
    )
    return drodpdown_component

# callback to update weather information data 
# based on selected district/sub-district
@callback(
    Output('weather_forecast', 'children'),
    [Input('weather_district_dropdown_output', 'value'),
    Input('date_dropdown', 'value')],
    prevent_initial_call=True
)
def update_weather_forecast(location, date_dropdown):
    
    # get weather forecast data every 3 hours in 1 day
    hours = [1,4,7,10,13,16,19,22]
    forecast_filtered = df[(df['date'] == date_dropdown) & ((df['desa'] == location) | (df['kecamatan'] == location)) & (df['hour'].isin(hours))]
    forecast_groupby_hour = forecast_filtered.groupby(['hour'], as_index=False).agg({'t': ['mean'], 'hu': ['mean'], 'weather_desc': pd.Series.mode, 'image': pd.Series.mode})
    forecast_groupby_hour.columns = list(map('_'.join, forecast_groupby_hour.columns.values))
    
    cards = []
    for data in forecast_groupby_hour.to_dict('records'):
        card = get_weather_card_component(f"{data['hour_']:02d}.00", data['image_mode'], data['weather_desc_mode'], f"{round(data['t_mean'])}° C")
        cards.append(card)

    weather_cards = dbc.Row(cards)
    
    return weather_cards



if __name__ == '__main__':
    app.run(debug=True)
