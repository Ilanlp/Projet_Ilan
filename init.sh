#!/bin/bash

set -e  # Arrêt du script en cas d'erreur

echo ""
echo "🚀 Exécution séquentielle des conteneurs..."

echo "▶️ Étape 1 : extract"
docker-compose run --rm extract

echo "▶️ Étape 2 : load"
docker-compose run --rm load

echo "▶️ Étape 3 : transform"
docker-compose run --rm transform

echo ""
echo "✅ Pipeline terminé avec succès."