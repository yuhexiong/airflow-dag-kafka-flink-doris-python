from airflow import DAG
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import OffsetOutOfRangeError
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pytz

bootstrap_servers = Variable.get("bootstrap_servers")
topic_name = Variable.get("topic_name")
log_file = Variable.get("log_file")

def check_kafka_updates():
    current_time = datetime.now()
    current_time_tw = current_time.astimezone(pytz.timezone('Asia/Taipei'))
    check_date = current_time_tw - timedelta(days=1)
    group_id = "group-id-1"

    print(f"bootstrap_servers:                             {bootstrap_servers}")
    print(f"topic_name:                                    {topic_name}")
    print(f"group_id:                                      {group_id}")
    print(f"check_date:                                    {check_date}")

    latest_time = get_topic_latest_time(bootstrap_servers, topic_name, group_id)
    if latest_time is not None and latest_time.tzinfo is None:
        latest_time = pytz.timezone('Asia/Taipei').localize(latest_time)
    if latest_time is not None and latest_time < check_date:
        error_message = f"topic {topic_name} has no new data since {check_date}, the latest time is {latest_time}\n"
        with open(log_file, 'a') as file:
            file.write(f"{current_time_tw.strftime('%Y-%m-%d %H:%M:%S')} - {error_message}\n")
        raise Exception(error_message)

def get_topic_latest_time(bootstrap_servers, topic_name, group_id):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=group_id)
    
    partitions = consumer.partitions_for_topic(topic_name)
    if not partitions:
        print(f"topic {topic_name} not found")
        return None
    
    latest_timestamp = None
    for partition in partitions:
        tp = TopicPartition(topic_name, partition)
        
        consumer.assign([tp])
        consumer.seek_to_end(tp)
        end_offset = consumer.position(tp)
        
        if end_offset == 0:
            continue
        
        try:
            consumer.seek(tp, end_offset - 1)
            msg_pack = consumer.poll(timeout_ms=500)
            if not msg_pack:
                continue
            
            for tp, messages in msg_pack.items():
                for msg in messages:
                    msg_datetime = datetime.fromtimestamp(msg.timestamp / 1000.0)
                    print("topic {topic_name} partition {partition} message datetime:", msg_datetime)
                    
                    if latest_timestamp is None or msg_datetime > latest_timestamp:
                        latest_timestamp = msg_datetime
        except OffsetOutOfRangeError:
            continue

    consumer.close()
    return latest_timestamp


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

with DAG(dag_id='check_kafka_updates', default_args=default_args, schedule_interval='0 0 * * *', max_active_runs=1, catchup=False) as dag:

    task0 = PythonOperator(
        task_id='check_kafka_updates',
        python_callable=check_kafka_updates
    )

    task0
