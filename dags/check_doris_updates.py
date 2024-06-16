from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pytz
import pymysql

doris_host = Variable.get("doris_host")
username = Variable.get("username")
password = Variable.get("password")
database = Variable.get("database")
doris_table = Variable.get("doris_table")
doris_timestamp_column = Variable.get("doris_timestamp_column")
log_file = Variable.get("log_file")

def check_doris_updates():
    print(f"doris_host:                                    {doris_host}")
    print(f"username:                                      {username}")
    print(f"password:                                      {password}")
    print(f"database:                                      {database}")

    connection = connect_to_doris(doris_host, username, password, database)
     
    current_time = datetime.now()
    current_time_tw = current_time.astimezone(pytz.timezone('Asia/Taipei'))
    check_date = current_time_tw - timedelta(days=1)
    print(f"check_date:                                    {check_date}")
    
    latest_time = get_table_latest_time(connection, doris_table, doris_timestamp_column)
    if latest_time is not None and latest_time.tzinfo is None:
        latest_time = pytz.timezone('Asia/Taipei').localize(latest_time)
    if latest_time is not None and latest_time < check_date:
        error_message = f"doris table {doris_table} has no new data since {check_date}, the latest time is {latest_time}\n"
        with open(log_file, 'a') as file:
            file.write(f"{current_time_tw.strftime('%Y-%m-%d %H:%M:%S')} - {error_message}\n")
        raise Exception(error_message)

def connect_to_doris(host, user, password, database):
    try:
        connection = pymysql.connect(
            host=host,
            port=9030,
            user=user,
            password=password,
            database=database
        )
        return connection
    except pymysql.MySQLError as e:
        print(f"[Error] connecting to Doris database: {e}")
        return None

def get_table_latest_time(connection, table, timestamp_column):
    try:
        with connection.cursor() as cursor:
            query = f"SELECT {timestamp_column} FROM {table} ORDER BY {timestamp_column} DESC LIMIT 1"
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                return result[0]
            else:
                return None
    except pymysql.MySQLError as e:
        print(f"[Error] executing doris sql query {query}: {e}")
        return None


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 13),
    'email': ['myemail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'max_retries': 0,
}

with DAG(dag_id='check_doris_updates', default_args=default_args, schedule_interval='0 0 * * *', max_active_runs=1, catchup=False) as dag:

    task0 = PythonOperator(
        task_id='check_doris_updates',
        python_callable=check_doris_updates
    )

    task0
