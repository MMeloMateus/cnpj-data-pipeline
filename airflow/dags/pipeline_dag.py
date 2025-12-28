from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

from src.extract.downloader import download_files_for_range  # função atualizada

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="download_pipeline_manual",
    start_date=datetime(2025, 1, 1),  # obrigatório pelo Airflow
    schedule_interval=None,           # nenhuma execução automática
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=download_files_for_range,
        op_kwargs={
            "start_date": pendulum.datetime(2023, 2, 1),
            "end_date": pendulum.datetime(2023, 4, 1)
        },
        provide_context=True,
    )