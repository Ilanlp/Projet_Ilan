import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

load_dotenv()

# Tu peux changer le thème ici (exemples : BOOTSTRAP, FLATLY, LUX, MINTY, DARKLY, etc.)
app = dash.Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.FLATLY]
)
server = app.server

app.layout = dbc.Container(
    fluid=True,
    children=[
        # En-tête avec style pro
        html.Div(
            style={
                "textAlign": "center",
                "padding": "50px 20px",
                "background": "linear-gradient(135deg, #0d6efd, #6610f2)",
                "color": "white",
                "borderRadius": "10px",
                "boxShadow": "0 4px 12px rgba(0,0,0,0.2)",
                "marginBottom": "30px"
            },
            children=[
                html.H1("💼 Job Market Dashboard", style={
                    "fontSize": "3.2em",
                    "fontWeight": "bold",
                    "marginBottom": "10px"
                }),
                html.P("Explorez les données du marché de l'emploi en un clic", style={
                    "fontSize": "1.3em",
                    "opacity": 0.9
                })
            ]
        ),

        # Navigation stylisée
        dbc.Row(
            dbc.Col(
                dbc.Nav(
                    [
                        dbc.NavLink("📍 Carte Villes / Régions + skills", href="/top_ville", active="exact", className="nav-button"),
                        dbc.NavLink("👤 Profile", href="/candidat", active="exact", className="nav-button")

                    ],
                    pills=True,
                    justified=True,
                    className="mb-4"
                ),
                width=12
            )
        ),

        html.Hr(),

        # Zone d'affichage des pages
        dash.page_container
    ]
)
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080, dev_tools_hot_reload=True)