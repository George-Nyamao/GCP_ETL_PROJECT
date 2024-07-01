import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 25),
    'depends_on_past': False,
    'email': ['georgenyamao23@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('employee_data_pipeline',
          default_args=default_args,
          description='Runs an external python script to upload data to GCS',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_to_gcs',
        bash_command='/home/airflow/gcs/dags/scripts/extract_to_gcs.py',
    )
