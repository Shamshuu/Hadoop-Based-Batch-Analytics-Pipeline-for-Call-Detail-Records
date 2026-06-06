from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'top_callers_by_spend_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark_job = BashOperator(
        task_id='submit_spark_job',
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/jobs/top_callers.py "
            "--run-id \"{{ dag_run.conf.get('run_id', 'manual_run') }}\" "
            "--input /data/cdr_data.csv"
        )
    )
