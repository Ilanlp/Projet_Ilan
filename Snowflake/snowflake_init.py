import os
from dotenv import load_dotenv
import snowflake.connector
from pathlib import Path

class SnowflakeInitializer:
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

    def test_connection(self):
        try:
            self.cursor.execute("SELECT CURRENT_VERSION()")
            version = self.cursor.fetchone()
            print(f"✅ Connexion réussie à Snowflake, version : {version[0]}")
        except Exception as e:
            print("❌ Erreur de connexion :", e)
            raise

    def run_sql_file(self, file_path: Path):
        print(f"🚀 Exécution de {file_path.name}...")
        with open(file_path, "r", encoding="utf-8") as f:
            sql_script = f.read()
        for statement in sql_script.split(";"):
            if statement.strip():
                self.cursor.execute(statement.strip())
        print(f"✅ Fichier {file_path.name} exécuté avec succès.")

    def initialize(self):
        base_dir = Path(__file__).resolve().parent
        files_to_run = [
            "create_database.sql",
            "create_schemas.sql",
            "create_warehouses.sql",
            "create_file_format.sql",
        ]
        create_tables_dir = base_dir / "create_tables"
        table_files = ["bronze_init.sql", "silver_init.sql", "gold_init.sql"]

        for file_name in files_to_run:
            self.run_sql_file(base_dir / file_name)

        for table_file in table_files:
            self.run_sql_file(create_tables_dir / table_file)

        print("🏁 Initialisation complète.")

if __name__ == "__main__":
    init = SnowflakeInitializer()
    init.test_connection()
    init.initialize()
