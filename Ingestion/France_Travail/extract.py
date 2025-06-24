import os
import json
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


class FranceTravailClient:
    BASE_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2"
    TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"

    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv("FRANCE_TRAVAIL_ID")
        self.client_secret = os.getenv("FRANCE_TRAVAIL_KEY")
        self.keyword = os.getenv("FRANCE_TRAVAIL_KEYWORD", "data")
        self.date_creation = os.getenv("FRANCE_TRAVAIL_DATE_CREATION")
        self.token = self._get_token()
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    def _get_token(self):
        response = requests.post(
            self.TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "api_offresdemploiv2 o2dsoffre",
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def search_offres(self):
        params = {
            "motsCles": self.keyword,
            "dateCreationMin": self.date_creation
        }
        url = f"{self.BASE_URL}/offres/search"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def save_to_file(self, data):
        base_dir = Path(__file__).resolve().parent.parent / "data" / "data_raw"
        base_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.today().strftime("%Y-%m-%d")
        file_path = base_dir / f"france_travail_results_{today}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Données sauvegardées dans {file_path}")



def extract():
    client = FranceTravailClient()
    results = client.search_offres()
    client.save_to_file(results)
