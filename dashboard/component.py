import dash_bootstrap_components as dbc
import plotly.express as px
from dash import html

# create reusable card component
def get_card_component(title, data):
    component = dbc.Col([
                    html.H1(data),
                    html.H4(title)
                ])
    return component

def get_card_with_image_component(title, image_url):
    component = dbc.Col([
                    html.Img(
                        src=image_url, 
                        style={'width': '100px', 'height': '100px'}
                ),
                    html.H4(title)
                ])
    return component

def get_weather_card_component(hour, img, weather, temp):
    component = dbc.Col(
        html.Div([
            html.H5(hour),
            html.Img(
                src=img,
                style={'width': '90px', 'height': '90px', "padding": "10px 20px"}
            ),
            html.H6(weather),
            html.H6(temp)
        ], style={"background": "#fff", "border-radius": "12px", "align-content": "center", "padding": "10px 10px 10px 10px", "align-self": "center", "color": "#3E5879", "margin-top": "20px", "text-align": "center", "box-shadow": "rgba(0, 0, 0, 0.16) 0px 1px 4px"})
    )
    return component

def get_temperature_facet_grid_figure(df, x, y, facet):
    figure = px.line(
            df, 
            x=x, 
            y=y, 
            color=facet, 
            facet_col=facet, 
            labels={
                x: "Tanggal/Waktu",
                y: "Rata-rata Suhu (°C)",
                facet: "Kelurahan/Desa"
            },
            facet_col_wrap=5
    )

    return figure