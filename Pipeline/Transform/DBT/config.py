from pathlib import Path
from dotenv import dotenv_values
import yaml
import os

def generate_profiles_yml(env_path: str, output_path: str):
    env_vars = dotenv_values(env_path)

    # Créer le contenu YAML
    profiles = {
        "demo_dbt": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "snowflake",
                    "account": env_vars["SNOWFLAKE_ACCOUNT"],
                    "user": env_vars["SNOWFLAKE_USER"],
                    "password": env_vars["SNOWFLAKE_PASSWORD"],
                    "role": env_vars["SNOWFLAKE_ROLE"],
                    "warehouse": env_vars["SNOWFLAKE_WAREHOUSE"],
                    "database": env_vars["SNOWFLAKE_DATABASE"],
                    "schema": env_vars["SNOWFLAKE_SCHEMA"],
                    "threads": 4
                }
            }
        }
    }

    # Créer le dossier si nécessaire
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Écriture dans le fichier
    with open(output_file, "w") as f:
        yaml.dump(profiles, f, default_flow_style=False)

    print(f"[✓] profiles.yml écrit dans {output_file}")

if __name__ == "__main__":
    generate_profiles_yml(env_path=".env", output_path="/root/.dbt/profiles.yml")