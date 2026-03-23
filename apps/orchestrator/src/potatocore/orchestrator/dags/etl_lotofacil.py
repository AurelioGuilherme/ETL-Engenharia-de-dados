from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from potatocore.ingestion.bronze_loader import load_lotofacil_to_bronze
from potatocore.orchestrator.jobs.api_smoke import check_api_health
from potatocore.orchestrator.jobs.dbt_runner import run_dbt
from potatocore.orchestrator.jobs.publish_api import publish_gold_to_api

with DAG(
    dag_id="potatocore_etl_lotofacil",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-platform",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    max_active_runs=1,
    tags=["potatocore", "etl", "dbt"],
) as dag:
    extract_load_bronze = PythonOperator(
        task_id="extract_load_bronze",
        python_callable=load_lotofacil_to_bronze,
    )

    dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=run_dbt,
        op_kwargs={"command": "deps"},
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
        op_kwargs={"command": "run"},
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt,
        op_kwargs={"command": "test"},
    )

    publish_api_db = PythonOperator(
        task_id="publish_api_db",
        python_callable=publish_gold_to_api,
    )

    api_smoke = PythonOperator(
        task_id="api_smoke",
        python_callable=check_api_health,
    )

    extract_load_bronze >> dbt_deps >> dbt_run >> dbt_test >> publish_api_db >> api_smoke
