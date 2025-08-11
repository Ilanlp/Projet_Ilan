##Install

pip install dbt-core\
            dbt-postgres\
            dbt-redshift\
            dbt-snowflake\
            dbt-bigquery
dbt --version           

dbt init demo_dbt

nano ~/.dbt/profiles.yml

## TOUCHES 

```bash
dbt debug #voir si tout est ok 
dbt run #

```

## models 
```bash
lower(o.description) LIKE '% ' || lower(c.skill) || ' %' #besoin de | car insertion de colonne dynamique et %x% attend une chaine de caractère
cast(location as string) #force a un certain type
substr(l.code_postal, 1, 2) #extraire 2 premiere en partant du 1er
```