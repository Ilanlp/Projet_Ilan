from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

class AdzunaClient:
    BASE_URL = "https://api.adzuna.com/v1/api"

    def __init__(self):
        load_dotenv()
        self.app_id = os.getenv("ADZUNA_APP_ID")
        self.app_key = os.getenv("ADZUNA_APP_KEY")
        self.search_term = os.getenv("ADZUNA_WHAT", "data")
        self.max_days_old = os.getenv("ADZUNA_MAX_DAYS_OLD", "30")
        self.params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": 50,
            "what_or": self.search_term,
            "max_days_old": self.max_days_old,
            "content-type": "application/json"
        }

    def test_credentials(self):
        url = f"{self.BASE_URL}/jobs/fr/search/1"
        response = requests.get(url, params=self.params)
        if response.status_code == 200:
            print("✅ Identifiants valides.")
            return True
        else:
            print(f"❌ Identifiants invalides ({response.status_code})")
            print(response.text)
            return False

    def get_total_results(self):
        url = f"{self.BASE_URL}/jobs/fr/search/1"
        response = requests.get(url, params=self.params)
        if response.status_code != 200:
            print(f"❌ Erreur lors de l'obtention du total :", response.status_code)
            return 0
        data = response.json()
        return data.get("count", 0)

    def fetch_page(self, page: int):
        print(f"🔎 Récupération de la page {page}...")
        url = f"{self.BASE_URL}/jobs/fr/search/{page}"
        response = requests.get(url, params=self.params)
        if response.status_code != 200:
            print(f"❌ Erreur page {page} :", response.status_code)
            return []
        return response.json().get("results", [])

    def fetch_job_data(self, max_workers=5):
        total_results = self.get_total_results()
        if total_results == 0:
            print("❌ Aucune donnée disponible.")
            return

        max_pages = (total_results // 50) + 1
        print(f"🔢 Total résultats : {total_results} → {max_pages} pages à récupérer")

        all_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.fetch_page, page) for page in range(1, max_pages + 1)]
            for future in as_completed(futures):
                all_results.extend(future.result())

        # Sauvegarde
        data_dir = Path(__file__).resolve().parent.parent / "data" / "data_raw"
        data_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.today().strftime("%Y-%m-%d")
        output_file = data_dir / f"adzuna_results_{today}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)

        print(f"✅ {len(all_results)} offres sauvegardées dans '{output_file}'")

def extract():
    client = AdzunaClient()
    if client.test_credentials():
        client.fetch_job_data()

if __name__ == "__main__":
    extract()
