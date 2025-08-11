# Image de base
FROM python:3.10-slim

# Créer un répertoire de travail
WORKDIR /app

# Copier tous les fichiers dans le conteneur
COPY . .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Créer les dossiers de sortie
RUN mkdir -p data/data_raw && mkdir -p data/data_normalized

# Lancer le script
CMD ["python", "Normalizer.py"]