#!/bin/bash
set -e

cd /home/ilanlp/Projet_Ilan/Pipeline/Extract
python3 Normalizer.py
cd /home/ilanlp/Projet_Ilan

# Script 2
cd /home/ilanlp/Projet_Ilan/Pipeline/Load
python3 snowflake_incremental.py
cd /home/ilanlp/Projet_Ilan

# Script 3
cd /home/ilanlp/Projet_Ilan/Pipeline/Transform/DBT
dbt run