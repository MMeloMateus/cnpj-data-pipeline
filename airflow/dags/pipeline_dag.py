from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.extract.downloader import download_files_for_range  # função atualizada

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="download_pipeline_manual",
    start_date=datetime(2025, 1, 1),  # obrigatório no Airflow
    schedule_interval=None,           # nenhuma execução automática no momento (teste)
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=download_files_for_range,  # função que usa param opcionais
        provide_context=True,  # Para acessar param
    )