import dash
from dash import html, dcc, Output, Input, callback, State
import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_BASE_URL = "http://localhost:8000"

dash.register_page(__name__, path="/candidat")

def layout():
    # Appels API pour remplir les listes
    villes_request = requests.get(f"{API_BASE_URL}/candidat/ville").json()["data"]
    villes = [v[0] for v in villes_request]
    departements_request = requests.get(f"{API_BASE_URL}/candidat/departement").json()["data"]
    departements = [d[0] for d in departements_request]
    regions_requests = requests.get(f"{API_BASE_URL}/candidat/region").json()["data"]
    regions = [r[0] for r in regions_requests]
    skills_requests =  requests.get(f"{API_BASE_URL}/skills/").json()
    skills  = [s["skill"] for s in skills_requests]
    contrat_requests =  requests.get(f"{API_BASE_URL}/candidat/contrat").json()["data"]
    contrats  = [s[0] for s in contrat_requests]

    return html.Div([
        html.H2("🎯 Filtres géographiques", style={"textAlign": "center", "marginBottom": "30px"}),

        html.Div([
            html.Label("🏙️ Villes", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="ville-dropdown",
                options=[{"label": ville, "value": ville} for ville in villes],
                multi=True,
                placeholder="Sélectionnez une ou plusieurs villes"
            )
        ], style={"marginBottom": "20px"}),

        html.Div([
            html.Label("🏞️ Départements", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="departement-dropdown",
                options=[{"label": dep, "value": dep} for dep in departements],
                multi=True,
                placeholder="Sélectionnez un ou plusieurs départements"
            )
        ], style={"marginBottom": "20px"}),

        html.Div([
            html.Label("🌍 Régions", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="region-dropdown",
                options=[{"label": reg, "value": reg} for reg in regions],
                multi=True,
                placeholder="Sélectionnez une ou plusieurs régions"
            )
        ], style={"marginBottom": "20px"}),

        html.Div([
            html.Label("🧠 Skills", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="skills-dropdown",  # ✅ ID corrigé ici
                options=[{"label": skill, "value": skill} for skill in skills],
                multi=True,
                placeholder="Sélectionnez une ou plusieurs compétences"
            )
        ], style={"marginBottom": "30px"}),

        html.Div([
            html.Label("🧠 contrat", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="contrat-dropdown",  # ✅ ID corrigé ici
                options=[{"label": contrat, "value": contrat} for contrat in contrats],
                multi=True,
                placeholder="Sélectionnez une ou plusieurs compétences"
            )
        ], style={"marginBottom": "30px"}),

        html.Button("🔍 Rechercher", id="search-button", n_clicks=0, className="btn btn-primary"),
        html.Br(), html.Br(),

        html.Div(id="search-results")

        ], style={"padding": "40px", "fontFamily": "Arial"})

@callback(
    Output("search-results", "children"),
    Input("search-button", "n_clicks"),
    State("ville-dropdown", "value"),
    State("departement-dropdown", "value"),
    State("region-dropdown", "value"),
    State("skills-dropdown", "value"),
    State("contrat-dropdown", "value")

)
def rechercher(n_clicks, villes, departements, regions, skills, contrat):
    if n_clicks == 0:
        return ""

    # Préparation dynamique des paramètres GET
    params = []

    if villes:
        for v in villes:
            params.append(("ville", v))
    if departements:
        for d in departements:
            params.append(("departement", d))
    if regions:
        for r in regions:
            params.append(("region", r))
    if skills:
        for s in skills:
            params.append(("skill", s))
    if contrat:
        for c in contrat:
            params.append(("contrat", c))


    try:
        response = requests.get(f"{API_BASE_URL}/search", params=params)
        data = response.json()

        if not data or not data.get("data"):
            return html.Div("Aucune offre trouvée.", style={"marginTop": "20px", "color": "red"})

        offres = data["data"]

        return html.Ul([
            html.Li(f"{offre['TITLE']} - {offre.get('VILLE', 'Lieu inconnu')} ({offre.get('REGION', 'Région inconnue')}) | {offre.get('SKILL', 'Compétence inconnue')} - {offre.get('SOURCE_URL', 'url inconnue')} ")
            for offre in offres
        ], style={"marginTop": "20px"})

    except Exception as e:
        return html.Div(f"❌ Erreur lors de la recherche : {str(e)}", style={"color": "red", "marginTop": "20px"})


