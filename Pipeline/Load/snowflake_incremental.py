import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from datetime import date, datetime
from pathlib import Path

# Charger les variables d'environnement
load_dotenv()

# Générer le nom de fichier basé sur la date du jour
today = date.today().isoformat()  # Format YYYY-MM-DD
base_dir = Path(__file__).resolve().parent.parent
csv_path = f"{base_dir}/Extract/data/data_normalized/normalized_job_offers_{today}.csv"

# Vérification
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"❌ Le fichier {csv_path} n'existe pas.")

print(f"📄 Fichier du jour détecté : {csv_path}")

# Lire le fichier CSV
df = pd.read_csv(csv_path)

# Ajouter la colonne date_extraction (date du chargement)
df["date_extraction"] = datetime.now()
df["date_extraction"] = pd.to_datetime(df["date_extraction"]).dt.strftime("%Y-%m-%d %H:%M:%S")
df = df.astype(object)
df = df.where(pd.notnull(df), None)




# Connexion Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)
cursor = conn.cursor()



# Requête INSERT
insert_query = """
INSERT INTO RAW.RAW_OFFRE (
    id_local, title, description, date_created, location,
    latitude, longitude, company, source_url, contract_type, seniorite_type, source, date_extraction
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

# Insertion en lot
cursor.executemany(insert_query, df.values.tolist())
conn.commit()

cursor.close()
conn.close()
print("✅ Insertion des offres du jour terminée avec succès.")