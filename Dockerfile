FROM apache/airflow:2.9.1

COPY requirements.txt /requirements.txt

USER root

USER airflow

RUN pip install --upgrade pip
RUN pip install -r /requirements.txt
