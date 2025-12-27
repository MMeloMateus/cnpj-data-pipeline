from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.dates import days_ago

from src.extract.downloader import download_files_for_period

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="download_pipeline_monthly",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@monthly",
    catchup=True,  # permite backfill quando quiser processar range
    default_args=default_args,
    max_active_runs=1,
) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=download_files_for_period,
        provide_context=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/project/dbt && dbt run --profiles-dir ."
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/project/dbt && dbt test --profiles-dir ."
    )

    download >> dbt_run >> dbt_test