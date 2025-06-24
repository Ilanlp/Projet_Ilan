import json
from typing import List
from pathlib import Path
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from typing import Optional


# Sous-modèles
class Location(BaseModel):
    display_name: Optional[str]
    area: Optional[List[str]]

class Category(BaseModel):
    label: Optional[str]
    tag: Optional[str]

class Company(BaseModel):
    display_name: Optional[str]


# Ton modèle principal
class Job(BaseModel):
    id: str = Field(...)
    title: str = Field(...)
    description: str = Field(...)
    created: str = Field(...)
    location: Optional[Location] = None
    category: Optional[Category] = None
    company: Optional[Company] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_is_predicted: Optional[bool] = None
    contract_time: Optional[str] = None
    contract_type: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    redirect_url: Optional[str] = None


# La classe Transforme
class Transforme:
    def __init__(self, json_path: str):
        self.json_path = Path(json_path)
        self.validated_jobs: List[Job] = []
        self.df = pd.DataFrame()

    def charger_et_valider(self):
        with open(self.json_path, encoding='utf-8') as f:
            raw_data = json.load(f)

        if isinstance(raw_data[0], list):
            raw_data = [item for sublist in raw_data for item in sublist]

        for item in raw_data:
            try:
                job = Job(**item)
                self.validated_jobs.append(job)
            except ValidationError as e:
                print("❌ Erreur de validation :")
                print(e)

        print(f"✅ {len(self.validated_jobs)} offres validées.")

    def transformer_en_dataframe(self):
        self.df = pd.DataFrame([job.model_dump() for job in self.validated_jobs])
        print("✅ Données transformées en DataFrame.")

    def sauvegarder_en_csv(self, output_folder="data"):
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        today = datetime.today().strftime("%Y-%m-%d")
        csv_path = Path(output_folder) / f"adzuna_clean_{today}.csv"
        self.df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"📁 CSV sauvegardé ici : {csv_path}")


if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    json_path = f"data/adzuna_results_{today}.json"
    transforme = Transforme(json_path)
    transforme.charger_et_valider()
    transforme.transformer_en_dataframe()
    transforme.sauvegarder_en_csv()