import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator


default_args = {
    'start_date': datetime(2025, 7, 29),
    'retries': 1,
}

with DAG(
    dag_id='dag1_kafka_compute_cost',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description='Consumes travel data from Kafka, computes cost, publishes to Kafka',
) as dag1:

    consume_kafka = ConsumeFromTopicOperator(
        task_id='consume_kafka_source',
        topics=['source_fatou'],
        kafka_conn_id='kafka_default',
        output_file='/tmp/raw_travel_data.json',
    )

    def compute_travel_cost():
        subprocess.run(["python", "/opt/airflow/kafka/consumer_cost.py"])

    compute_cost = PythonOperator(
        task_id='compute_travel_cost',
        python_callable=compute_travel_cost,
        op_kwargs={
            'input_path': '/tmp/raw_travel_data.json',
            'output_path': '/tmp/processed_travel_data.json',
        },
    )

    publish_kafka = ProduceToTopicOperator(
        task_id='publish_kafka_result',
        topics=['result_gora'],
        kafka_conn_id='kafka_default',
        input_file='/tmp/processed_travel_data.json',
    )

    consume_kafka >> compute_cost >> publish_kafka

if __name__ == "__main__":
    dag1.cli()
