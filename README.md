# Airflow Dag Kafka/Flink/Doris

use airflow to maintain operation of pipeline and database.  

## Overview

- Tool: Apache Airflow v2.9.1
- Language: Python v3.8


## Run

### Run Docker
```
docker compose up -d
```

### ENV
setting config/airflow.cfg
```
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = youremail@gmail.com
smtp_password = pass
smtp_port = 587
smtp_mail_from = youremail@gmail.com
```

### UI Page

Open http://localhost:8080 to run DAGs in UI.  


## DAGs

### 1. Check Kafka Updates
confirm that new data comes in every day in kafka's topic.  

- setting variables
```
bootstrap_servers: localhost:9093
topic_name: topic1
log_file: local log file path to collect errors and send them once.
```

### 2. Check Flink Job State
check all jobs on flink are running every day.  

- setting variables
```
flink_url: localhost:8081
log_file: local log file path to collect errors and send them once.
```

### 3. Check Flink Job And Upload
check the jobs are running normally according to the configuration file yaml every day.  
If there are errors, cancel and rerun them.

- setting variables
```
log_file: local log file path to collect errors and send them once.
```

- flink_pipeline.yaml
```
host: http://localhost:8081
jars:
  - jar_id: xxx_xxx_flink-1.0.jar
    jobs:
      - entry_class: com.examples.entry.KafkaToKafka
        program_file: https://storage.googleapis.com/xxx/KafkaToKafka.yaml
        job_name: kafka(topic-1) -> kafka(topic-2)
      - entry_class: com.examples.entry.KafkaToDoris
        program_file: https://storage.googleapis.com/xxx/KafkaToDoris.yaml
        job_name: kafka(topic-1) -> doris(table-1)
```

### 4. Check Doris Updates
check doris has new data coming in every day.  

- setting variables
```
doris_host: localhost
username: root
password: password
database: database
doris_table: doris_table
doris_timestamp_column: timestamp
log_file: local log file path to collect errors and send them once.
```

### 5. Send Exception Email
check there is any data in the log file every day. If there is any data in the log file, send an email.  

- setting variables
```
log_file: local log file path to collect errors and send them once.
```
