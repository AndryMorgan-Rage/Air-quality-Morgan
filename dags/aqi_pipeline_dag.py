import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")

from extract_current import run_extract
from transform_clean import build_clean
from load_warehouse import run_load


default_args = {
    "owner": "groupe",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aqi_pipeline",
    description="Pipeline AQI : extract -> transform -> load",
    start_date=datetime(2026, 7, 11),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["aqi", "pipeline"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=build_clean,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    extract_task >> transform_task >> load_task
