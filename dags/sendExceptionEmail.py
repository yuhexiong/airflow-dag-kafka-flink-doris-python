import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

log_file = Variable.get("log_file")

def sendExceptionEmail():
    if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
        # read log file
        with open(log_file, 'r') as file:
            content = file.read()
        
        # clear log file
        with open(log_file, 'w') as file:
            file.truncate(0)

        raise Exception(content)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 12),
    'email': ['myemail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'max_retries': 0,
}

with DAG(dag_id='sendExceptionEmail', default_args=default_args, schedule_interval='30 0 * * *', max_active_runs=1, catchup=False) as dag:

    task0 = PythonOperator(
        task_id='sendExceptionEmail',
        python_callable=sendExceptionEmail
    )

    task0
