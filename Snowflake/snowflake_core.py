import os
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv

class SnowflakeCSVLoader:
    def __init__(self):
        load_dotenv()
        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        self.cursor = self.conn.cursor()

    def upload_csv(self, local_file_path: str, table_name: str, schema: str = None):

        active_schema = schema if schema else os.getenv("SNOWFLAKE_SCHEMA")

        # 0. S'assurer que la base et le schéma sont actifs
        self.cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        self.cursor.execute(f"USE SCHEMA {active_schema}")
        # 1. Met le fichier dans une stage temporaire
        self.cursor.execute(f"PUT file://{local_file_path} @%{table_name} OVERWRITE = TRUE")

        # 2. Copie les données dans la table
        self.cursor.execute(f"""
            COPY INTO {table_name}
            FILE_FORMAT = (FORMAT_NAME = classic_csv)
        """)
        print(f"✅ {local_file_path} chargé dans {table_name}")

    def close(self):
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    loader = SnowflakeCSVLoader()
    silver_table = ["DIM_TELETRAVAIL","DIM_DATE","DIM_COMPETENCE","DIM_CONTRAT","DIM_DOMAINE"]
    for table in silver_table:
        loader.upload_csv(f"data/{table}.csv", table, "SILVER")
    loader.close()