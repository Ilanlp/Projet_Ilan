import pandas as pd
import json
from datetime import datetime
from France_Travail.extract import extract as extract_france_travail
from Adzuna.extract import extract as extract_adzuna

def normalize_job_offers(source_name: str, data: list) -> pd.DataFrame:
    """Normalise les offres d'emploi issues d'Adzuna ou France Travail."""
    
    if source_name == "france_travail":
        df = pd.json_normalize(data)
        df_normalized = pd.DataFrame({
            "id": df.get("id"),
            "title": df.get("intitule"),
            "description": df.get("description"),
            "date_creation": df.get("dateCreation"),
            "location": df.get("lieuTravail"),
            "latitude": df.get("lieuTravail.latitude"),
            "longitude": df.get("lieuTravail.longitude"),
            "company": df.get("entreprise.nom"),
            "url": df.get("origineOffre.urlOrigine"),
            "contract_type": df.get("typeContrat"),
        })

    elif source_name == "adzuna":
        df = pd.json_normalize(data)
        df_normalized = pd.DataFrame({
            "id": df.get("id"),
            "title": df.get("title"),
            "description": df.get("description"),
            "date_creation": df.get("created"),
            "location": (
            df.get("location.display_name").fillna("") + " - " +
            df.get("location.area").apply(lambda x: " > ".join(x) if isinstance(x, list) else str(x)).fillna("")
            ),

            "latitude": df.get("latitude"),
            "longitude": df.get("longitude"),
            "company": df.get("company.display_name"),
            "url": df.get("redirect_url"),
            "contract_type": df.get("contract_type"),
        })

    else:
        raise ValueError("Source inconnue : 'france_travail' ou 'adzuna' attendus.")

    df_normalized["source"] = source_name
    return df_normalized

# ================================
# 🔁 Chargement auto + export CSV
# ================================
if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")

    extract_france_travail()
    extract_adzuna()

    # Chemins vers les fichiers source
    ft_path = f"data/data_raw/france_travail_results_{today}.json"
    adz_path = f"data/data_raw/adzuna_results_{today}.json"

    # Chargement des fichiers
    with open(ft_path, encoding="utf-8") as f:
        data_ft = json.load(f)

    with open(adz_path, encoding="utf-8") as f:
        data_adz = json.load(f)

    # Normalisation
    df_ft = normalize_job_offers("france_travail", data_ft["resultats"])
    df_adz = normalize_job_offers("adzuna", data_adz)

    df_adz["contract_type"] = df_adz["contract_type"].replace({
    "permanent": "CDI",
    "contract": "CDD"
})

    # Fusion et export
    df_all = pd.concat([df_ft, df_adz], ignore_index=True)
    #df_all["date_creation"] = pd.to_datetime(df_all["date_creation"], errors="coerce", utc=True)

    # Export CSV
    output_path = f"data/data_normalized/normalized_job_offers_{today}.csv"
    df_all.to_csv(output_path, index=False, encoding="utf-8")

    print(f"✅ Fichier exporté : {output_path}")
