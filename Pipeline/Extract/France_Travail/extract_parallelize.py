import os
import json
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed


class FranceTravailClient:
    BASE_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2"
    TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"

    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv("FRANCE_TRAVAIL_ID")
        self.client_secret = os.getenv("FRANCE_TRAVAIL_KEY")
        self.keyword = os.getenv("FRANCE_TRAVAIL_KEYWORD", "data")
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

    def fetch_range(self, start, step):
        headers = {
            **self.headers,
            "Range": f"items={start}-{start + step - 1}"
        }
        params = {
            "motsCles": self.keyword,
            "publieeDepuis": 14
        }
        page = start // step + 1
        print(f"🔍 Page {page} → items {start}-{start + step - 1}")
        response = requests.get(f"{self.BASE_URL}/offres/search", headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("resultats", [])

    def search_offres_parallel(self, max_results=10000, step=150, max_workers=3):
        all_results = []
        ranges = [(i, step) for i in range(0, max_results, step)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_range = {executor.submit(self.fetch_range, start, step): (start, step) for start, step in ranges}

            for future in as_completed(future_to_range):
                try:
                    data = future.result()
                    all_results.extend(data)
                except Exception as e:
                    print(f"❌ Erreur pour la plage {future_to_range[future]} : {e}")

        print(f"✅ Total {len(all_results)} offres collectées.")
        return {"resultats": all_results}

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
    results = client.search_offres_parallel()
    client.save_to_file(results)

if __name__ == "__main__":
    extract()
