import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from datetime import datetime
import json

from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

default_args = {
    'start_date': datetime(2025, 7, 29),
    'retries': 1,
}

def index_to_elasticsearch(input_file: str, index_name: str, **kwargs):
    es_hook = ElasticsearchHook(elasticsearch_conn_id="elasticsearch_default")
    es = es_hook.get_conn()

    with open(input_file, 'r') as f:
        docs = json.load(f)

    for doc in docs:
        es.index(index=index_name, document=doc)

    print(f"✅ Indexed {len(docs)} documents into '{index_name}'")

with DAG(
    dag_id='dag2_kafka_to_elasticsearch',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description='Consumes result data, transforms JSON, and indexes into Elasticsearch',
) as dag2:

    consume_result = ConsumeFromTopicOperator(
        task_id='consume_kafka_result',
        topics=['result_gora'],
        kafka_conn_id='kafka_default',
        output_file='/tmp/kafka_result.json',
    )

    def run_consumer_transform():
        subprocess.run(["python", "/opt/airflow/kafka/consumer_transform.py"])
    
    transform = PythonOperator(
        task_id='transform_json',
        python_callable=run_consumer_transform,
        op_kwargs={
            'input_path': '/tmp/kafka_result.json',
            'output_path': '/tmp/transformed_result.json',
        },
    )

    index_elasticsearch = PythonOperator(
        task_id='index_to_elasticsearch',
        python_callable=index_to_elasticsearch,
        op_kwargs={
            'input_file': '/tmp/transformed_result.json',
            'index_name': 'travel_cost',
        },
    )

    consume_result >> transform >> index_elasticsearch

if __name__ == "__main__":
    dag2.cli()
