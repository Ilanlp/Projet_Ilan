from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os
import requests
import json
import time

class AdzunaClient:
    BASE_URL = "https://api.adzuna.com/v1/api"

    def __init__(self):
        load_dotenv()
        self.app_id = os.getenv("ADZUNA_APP_ID")
        self.app_key = os.getenv("ADZUNA_APP_KEY")
        self.search_term = os.getenv("ADZUNA_WHAT", "data")
        self.max_days_old = os.getenv("ADZUNA_MAX_DAYS_OLD", "1")
        self.params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": 50,
            "what": self.search_term,
            "max_days_old": self.max_days_old,
            "content-type": "application/json"
        }

    def test_credentials(self):
        """Teste si les identifiants sont valides"""
        url = f"{self.BASE_URL}/jobs/fr/search/1"
        response = requests.get(url, params=self.params)
        if response.status_code == 200:
            print("✅ Identifiants valides.")
            return True
        else:
            print(f"❌ Identifiants invalides ({response.status_code})")
            print(response.text)
            return False

    def fetch_job_data(self, max_pages=5):
        """Récupère les offres d'emploi et les sauvegarde dans ./data/ avec datestamp"""
        all_results = []
        for page in range(1, max_pages + 1):
            print(f"🔎 Récupération de la page {page}...")
            url = f"{self.BASE_URL}/jobs/fr/search/{page}"
            response = requests.get(url, params=self.params)

            if response.status_code != 200:
                print(f"❌ Erreur page {page} :", response.status_code)
                print(response.text)
                break

            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)

            total_results = data.get("count", 0)
            if len(all_results) >= total_results:
                break

            time.sleep(0.5)

        # Créer dossier ./data si besoin
        data_dir = Path(__file__).resolve().parent.parent / "data" / "data_raw"
        data_dir.mkdir(parents=True, exist_ok=True)

        # Créer un nom de fichier avec la date du jour
        today = datetime.today().strftime("%Y-%m-%d")
        output_file = data_dir / f"adzuna_results_{today}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)

        print(f"✅ {len(all_results)} offres sauvegardées dans '{output_file}'")


def extract():
    client = AdzunaClient()
    if client.test_credentials():
        client.fetch_job_data()