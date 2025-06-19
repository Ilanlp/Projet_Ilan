import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

BASE_URL  = "https://api.adzuna.com/v1/api"
client_id= os.getenv('ADZUNA_APP_ID')
client_key = os.getenv('ADZUNA_APP_KEY')

params  = {
    'app_id': client_id,
    'app_key': client_key,
    'results_per_page': 50,
    'what': 'python'
}

url = f"{BASE_URL}/jobs/fr/search/1"

response = requests.get(url, params=params)

# Vérifie le statut
if response.status_code == 200:
    data = response.json()
    with open("adzuna_results.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("✅ Données sauvegardées dans 'adzuna_results.json'")
else:
    print("❌ Erreur:", response.status_code)
    print(response.text)