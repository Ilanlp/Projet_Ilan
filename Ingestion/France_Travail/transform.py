import json
from typing import List, Optional
from pathlib import Path
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field, ValidationError


# Sous-modèles
class LieuTravail(BaseModel):
    libelle: Optional[str]

class Entreprise(BaseModel):
    nom: Optional[str]

class Salaire(BaseModel):
    libelle: Optional[str]

# Modèle principal
class Offre(BaseModel):
    id: str
    intitule: str
    description: Optional[str] = None
    dateCreation: Optional[str] = None
    typeContrat: Optional[str] = None
    typeContratLibelle: Optional[str] = None
    lieuTravail: Optional[LieuTravail] = None
    entreprise: Optional[Entreprise] = None
    salaire: Optional[Salaire] = None


# Classe de transformation
class TransformeFranceTravail:
    def __init__(self, json_path: str):
        self.json_path = Path(json_path)
        self.validated_offres: List[Offre] = []
        self.df = pd.DataFrame()

    def charger_et_valider(self):
        with open(self.json_path, encoding='utf-8') as f:
            raw_data = json.load(f)

        raw_offres = raw_data.get("resultats", [])

        for item in raw_offres:
            try:
                offre = Offre(**item)
                self.validated_offres.append(offre)
            except ValidationError as e:
                print("❌ Erreur de validation :")
                print(e)

        print(f"✅ {len(self.validated_offres)} offres validées.")

    def transformer_en_dataframe(self):
        self.df = pd.DataFrame([offre.model_dump() for offre in self.validated_offres])
        print("✅ Données transformées en DataFrame.")

    def sauvegarder_en_csv(self, output_folder="data"):
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        today = datetime.today().strftime("%Y-%m-%d")
        csv_path = Path(output_folder) / f"france_travail_clean_{today}.csv"
        self.df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"📁 CSV sauvegardé ici : {csv_path}")


if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    json_path = f"data/france_travail_results_{today}.json"
    transforme = TransformeFranceTravail(json_path)
    transforme.charger_et_valider()
    transforme.transformer_en_dataframe()
    transforme.sauvegarder_en_csv()
