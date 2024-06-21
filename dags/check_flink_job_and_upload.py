from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import yaml

log_file = Variable.get("log_file")

def task1_read_yaml(**kwargs):
    ti = kwargs['ti']

    with open("/opt/airflow/dags" + "flink_pipeline.yaml", 'r') as file:
        yaml_data = yaml.safe_load(file)
    ti.xcom_push(key='yaml_data', value=yaml_data)

def task2_check_flink_job_and_upload(**kwargs):
    ti = kwargs['ti']
    yaml_data = ti.xcom_pull(key='yaml_data', task_ids='task1_read_yaml')

    host = yaml_data['host']
    response = requests.get(f"{host}/jobs/overview")
    if response.status_code != 200:
        raise Exception(response.json())

    exist_jobs = response.json()['jobs']
    
    error_message = ""

    for jar in yaml_data['jars']:
        jar_id = jar['jar_id']   
        for job in jar['jobs']:
            entry_class = job['entry_class']
            program_file = job['program_file']
            job_name = job['job_name']

            should_upload = True
            for exist_job in exist_jobs:
                if exist_job['name'] == job_name:
                    if exist_job['state'] == 'RUNNING':
                        should_upload = False
                    else:
                        # run with error, cancel and upload again
                        response = requests.get(f"{host}/jobs/{exist_job['jid']}/yarn-cancel")
                        if response.status_code != 200:
                            raise Exception(response.json())
                        error_message += f"job {job_name} state: {exist_job['state']}, cancel now\n"
                    break
            
            # upload job
            if should_upload:
                body = {
                    "entryClass": entry_class,
                    "parallelism": 1,
                    "programArgsList": [
                        "-f",
                        program_file
                        ]
                }
                response = requests.post(f"{host}/jars/{jar_id}/run", json=body)
                if response.status_code != 200:
                    raise Exception(response.json())
                error_message += f"job {job_name} uploaded\n"

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

with DAG(dag_id='check_flink_job_and_upload', default_args=default_args, schedule_interval='0 0 * * *', max_active_runs=1, catchup=False) as dag:

    task1 = PythonOperator(
        task_id='task1_read_yaml',
        python_callable=task1_read_yaml
    )

    task2 = PythonOperator(
        task_id='task2_check_flink_job_and_upload',
        python_callable=task2_check_flink_job_and_upload
    )

    task1 >> task2
