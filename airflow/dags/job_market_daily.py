from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from docker.types import Mount
import os
from dotenv import load_dotenv
from airflow.models import Variable

load_dotenv('/home/ilanlp/Projet_Ilan/Pipeline/Transform/DBT/.env')



default_args = {
    "owner": "ilan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="jobmarket_etl_daily",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=["jobmarket", "etl"],
) as dag:

    extract = DockerOperator(
         task_id="run_extract",
         image="jobmarket-extract",
         api_version="auto",
         auto_remove=True,
         docker_url="unix://var/run/docker.sock",
         network_mode="bridge",
         mounts=[
             Mount(source="/home/ilanlp/Projet_Ilan/Pipeline/Extract/data", target="/app/data", type="bind"),
             Mount(source="/home/ilanlp/Projet_Ilan/Pipeline/Extract/.env", target="/app/.env", type="bind"),
         ],
     )

    load = DockerOperator(
         task_id="run_load",
         image="jobmarket-load",
         api_version="auto",
         auto_remove=True,
         docker_url="unix://var/run/docker.sock",
         network_mode="bridge",
         mounts=[
             Mount(source="/home/ilanlp/Projet_Ilan/Pipeline/Extract/data", target="/app/data", type="bind"),
             Mount(source="/home/ilanlp/Projet_Ilan/Pipeline/Load/.env", target="/app/.env", type="bind"),
         ],
     )

    transform = DockerOperator(
        task_id="run_transform_dbt",
        image="jobmarket-dbt",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "SNOWFLAKE_USER": Variable.get("SNOWFLAKE_USER"),
            "SNOWFLAKE_PASSWORD": Variable.get("SNOWFLAKE_PASSWORD"),
            "SNOWFLAKE_ACCOUNT": Variable.get("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_WAREHOUSE": Variable.get("SNOWFLAKE_WAREHOUSE"),
            "SNOWFLAKE_DATABASE": Variable.get("SNOWFLAKE_DATABASE"),
            "SNOWFLAKE_SCHEMA": Variable.get("SNOWFLAKE_SCHEMA"),
            "SNOWFLAKE_ROLE": Variable.get("SNOWFLAKE_ROLE"),
        },
    )

    extract >> load >> transform