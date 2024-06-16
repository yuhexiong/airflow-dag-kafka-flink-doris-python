from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pytz

flink_url = Variable.get("flink_url")
log_file = Variable.get("log_file")

def check_flink_job_state():
    response = requests.get(f"{flink_url}/jobs/overview")
    if response.status_code != 200:
        raise Exception(response.json())

    jobs = response.json()['jobs']
    
    error_message = ""
    for job in jobs:
        if job['state'] != 'RUNNING':
            error_message += f"{datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')} - flink job {job['name']} is not running, state: {job['state']}\n"
    
    if error_message != "":
        with open(log_file, 'a') as file:
            file.write(error_message)
        raise Exception(error_message)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 11),
    'email': ['myemail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'max_retries': 0,
}

with DAG(dag_id='check_flink_job_state', default_args=default_args, schedule_interval='0 0 * * *', max_active_runs=1, catchup=False) as dag:

    task0 = PythonOperator(
        task_id='check_flink_job_state',
        python_callable=check_flink_job_state
    )

    task0
