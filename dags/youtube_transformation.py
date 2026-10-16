from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from youtube_assets import CHANNEL_SNAPSHOT_ASSET, VIDEO_SNAPSHOT_ASSET

DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt" 
DBT_PROJECT_DIR = "/opt/airflow/dbt_youtube_transformation"  # Mounted project location
DBT_PROFILES_DIR = "/opt/airflow/.dbt"        # Mounted profiles.yml

default_args = {
    "owner": "nipphit",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="youtube_transformation",
    default_args=default_args,
    description="A simple DAG to run dbt models using volume-mapped profiles",
    # Runs when youtube_daily_ingestion has refreshed both staging tables
    schedule=(CHANNEL_SNAPSHOT_ASSET & VIDEO_SNAPSHOT_ASSET),
    start_date=datetime(2026, 10, 15),
    catchup=False,
    tags=["dbt", "analytics"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} debug --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_debug >> dbt_build
