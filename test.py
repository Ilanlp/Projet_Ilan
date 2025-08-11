from datetime import datetime 
from pathlib import Path 
from dotenv import load_dotenv 
import os 
import requests 
import json 
import time 


class AdzunaClient:
    BASE_URL  = "https://api.adzuna.colm/v1/api"

    def __init__(self):
        load_dotenv()
        self.app_id = os.getenv("ADZUNA_APP_ID")
        self.app_key = ps.getenv("ADZUNA_APP_KEY")
        self.max_days_old = os.getenv("ADZUNA_MAX_DAYS_OLD", "1")
        self.params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": 50,
            "what_or": self.search_term,
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


    def fetch_job_data(self, max_pages=100):
        all_results = []
        for page in range (1, max_pages + 1):
            print(f"recuperation de la plage {page}...")
            url = f"{self.BASE_URL}/jobs/fr/search/{page}"


