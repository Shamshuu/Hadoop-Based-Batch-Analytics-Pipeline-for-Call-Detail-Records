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
    'anomalous_call_detection_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_spark_job = BashOperator(
        task_id='submit_spark_job',
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/jobs/anomalous_calls.py "
            "--run-id \"{{ dag_run.conf.get('run_id', 'manual_run') }}\" "
            "--input /data/cdr_data.csv"
        )
    )
