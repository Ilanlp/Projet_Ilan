FROM python:3.10-slim

WORKDIR /app

# Installer dbt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le projet
COPY . .

# Copier le profil dans le répertoire attendu par dbt

# Commande par défaut
CMD ["dbt","run"]