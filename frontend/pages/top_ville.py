import dash
from dash import html, dcc, Input, Output
import requests
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()
API_BASE_URL = "http://localhost:8000"

dash.register_page(__name__, path="/top_ville")

def get_map_figure(data, label_col):
    df = pd.DataFrame(data)
    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        size="count",
        hover_name=label_col,
        color=label_col,
        size_max=50,
        zoom=4,
        height=600,
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="white",
    )
    return fig

def get_skill_figure(data):
    df = pd.DataFrame(data)
    fig = px.bar(
        df.sort_values("NB_OFFRES", ascending=False).head(20),
        x="NB_OFFRES",
        y="SKILL",
        orientation="h",
        title="Top Compétences Demandées sur le Marché",
        labels={"NB_OFFRES": "Nombre d'offres", "SKILL": "Compétence"},
        height=600
    )
    fig.update_layout(
        yaxis=dict(autorange="reversed"),
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin={"r": 10, "t": 40, "l": 80, "b": 40},
    )
    return fig

def layout():
    return html.Div(
        style={"padding": "30px", "backgroundColor": "#f9f9f9", "fontFamily": "Arial"},
        children=[

            html.H1("🧭 Analyse du Marché de l'Emploi", style={
                "textAlign": "center",
                "color": "#333",
                "marginBottom": "30px",
                "fontSize": "2.5em"
            }),

            dcc.RadioItems(
                id="view-selector",
                options=[
                    {"label": "📍 Carte des zones", "value": "map"},
                    {"label": "💼 Top Compétences", "value": "skills"}
                ],
                value="map",
                labelStyle={"display": "inline-block", "margin": "0 20px", "fontSize": "1.2em"},
                style={"textAlign": "center", "marginBottom": "30px"},
            ),

            html.Div(id="dynamic-content")
        ]
    )

@dash.callback(
    Output("dynamic-content", "children"),
    Input("view-selector", "value")
)
def update_view(selected_view):
    try:
        if selected_view == "map":
            return html.Div([
                html.Div("Choisissez le niveau de regroupement géographique :", style={
                    "textAlign": "center",
                    "color": "#555",
                    "fontSize": "1.1em",
                    "marginBottom": "10px"
                }),

                dcc.RadioItems(
                    id="zone-selector",
                    options=[
                        {"label": "🧭 Villes", "value": "ville"},
                        {"label": "🗺️ Département", "value": "departement"},
                        {"label": "🗺️ Régions", "value": "region"}
                    ],
                    value="ville",
                    labelStyle={"display": "inline-block", "margin": "0 20px"},
                    style={"textAlign": "center", "marginBottom": "20px"},
                ),

                html.Div(id="map-container")
            ])

        elif selected_view == "skills":
            response = requests.get(f"{API_BASE_URL}/top_skills")
            data = response.json()["data"]
            fig = get_skill_figure(data)
            return dcc.Graph(figure=fig)

    except Exception as e:
        return html.Div([
            html.H2("❌ Erreur de chargement", style={"color": "red"}),
            html.P(str(e))
        ])

@dash.callback(
    Output("map-container", "children"),
    Input("zone-selector", "value")
)
def update_map(zone_type):
    try:
        if zone_type == "ville":
            response = requests.get(f"{API_BASE_URL}/top_ville")
            data = response.json()["data"]
            fig = get_map_figure(data, label_col="ville")
        elif zone_type == "departement":
            response = requests.get(f"{API_BASE_URL}/top_departement")
            data = response.json()["data"]
            fig = get_map_figure(data, label_col="departement")
        else:
            response = requests.get(f"{API_BASE_URL}/top_region")
            data = response.json()["data"]
            fig = get_map_figure(data, label_col="region") 

        return dcc.Graph(figure=fig)

    except Exception as e:
        return html.Div([
            html.H2("❌ Erreur de chargement", style={"color": "red"}),
            html.P(str(e))
        ])
