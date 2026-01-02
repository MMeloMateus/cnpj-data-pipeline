from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

from cnpj_pipeline.extract.downloader import download_files_for_range
from cnpj_pipeline.extract.decompress import unzip_zip_to_parquet_range

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="pipeline_csv_dag",
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
            "start_date": pendulum.datetime(2025, 2, 1),
            "end_date": pendulum.datetime(2025, 2, 1),
        },
    )

    uncompress_csv = PythonOperator(
        task_id="uncompress_zip_range",
        python_callable=uncompress_zip_file_range,
        op_kwargs={
            "origin_base_path": "/opt/project/data/raw",
            "output_dir": "/opt/project/data/bronze/csv",
            "start_date": "2025-02",
            "end_date": "2025-02",
        },
    )

    download >> uncompress_csv