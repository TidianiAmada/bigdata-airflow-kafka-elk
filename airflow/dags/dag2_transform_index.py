from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime, timedelta
import json

default_args = {
    'start_date':  datetime.now()+timedelta(seconds=30),
    'retries': 1,
}

# Kafka message processing function (called by ConsumeFromTopicOperator)
def transform_message_to_es(message, **kwargs):
    try:
        record = json.loads(message.value().decode())
        record["agent_timestamp"] = datetime.now()
        return [{"value": json.dumps(record).encode()}]
    except Exception as e:
        print(f"Error parsing message: {e}")
        return []

# Indexer callable
def index_to_elasticsearch(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='consume_kafka_result')  # pulled from Kafka consume task
    if not records:
        print("No data received from Kafka topic.")
        return

    es_hook = ElasticsearchHook(elasticsearch_conn_id="elasticsearch_default")
    es = es_hook.get_conn()
    index_name = 'travel_cost'

    for record in records:
        try:
            data = json.loads(record['value'].decode())
            es.index(index=index_name, document=data)
        except Exception as e:
            print(f"Indexing error: {e}")

    print(f"✅ Indexed {len(records)} records into '{index_name}'")

with DAG(
    dag_id='dag2_kafka_to_elasticsearch',
    default_args=default_args,
    schedule_interval=timedelta(seconds=45),
    catchup=False,
    description='Consumes result data from Kafka and indexes into Elasticsearch',
) as dag2:

    consume_result = ConsumeFromTopicOperator(
        task_id='consume_kafka_result',
        topics=['result_gora'],
        kafka_config_id='kafka_default',
        apply_function=transform_message_to_es,
        commit_cadence="end_of_batch",
        max_messages=100,
    )

    index_elasticsearch = PythonOperator(
        task_id='index_to_elasticsearch',
        python_callable=index_to_elasticsearch,
        provide_context=True,
    )

    consume_result >> index_elasticsearch

if __name__ == "__main__":
    dag2.cli()
