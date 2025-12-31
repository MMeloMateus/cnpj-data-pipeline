from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

from src.extract.downloader import download_files_for_range
from src.extract.decrompress import uncompress_zip_file_range

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="download_pipeline_manual",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=download_files_for_range,
        op_kwargs={
            "start_date": pendulum.datetime(2025, 1, 1),
            "end_date": pendulum.datetime(2025, 3, 1),
        },
    )

    uncompress = PythonOperator(
        task_id="uncompress_zip_range",
        python_callable=uncompress_zip_file_range,
        op_kwargs={
            "origin_base_path": "/opt/project/data/raw",
            "output_dir": "/opt/project/data/bronze",
            "start_date": "2025-01",
            "end_date": "2025-03",
        },
    )

    download >> uncompress