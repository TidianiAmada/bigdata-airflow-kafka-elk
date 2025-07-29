FROM apache/airflow:2.8.1

USER airflow

COPY ./airflow/dags /opt/airflow/dags
COPY ./kafka /opt/airflow/kafka
COPY ./kafka/utils /opt/airflow/kafka/utils


RUN pip install --no-cache-dir kafka-python elasticsearch apache-airflow-providers-elasticsearch
