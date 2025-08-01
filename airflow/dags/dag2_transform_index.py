from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import json

log = LoggingMixin().log

default_args = {
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# Kafka message processing function (called by ConsumeFromTopicOperator)
def transform_message_to_es(message, **kwargs):
    try:
        raw = message.value().decode()
        record = json.loads(raw)
        record["agent_timestamp"] = datetime.now()
        log.info(f"📩 Received and transformed record: {record}")
        return [{"value": json.dumps(record).encode()}]
    except Exception as e:
        log.error(f"❌ Error parsing message: {e}")
        return []

# Indexer callable
def index_to_elasticsearch(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='consume_kafka_result')

    if not records:
        log.warning("⚠️ No data received from Kafka topic.")
        return

    es_hook = ElasticsearchHook(elasticsearch_conn_id="elasticsearch_default")
    es = es_hook.get_conn()
    index_name = 'groupe_tfig'

    # Ensure index exists
    if not es.indices.exists(index=index_name):
        log.info(f"🔧 Creating index '{index_name}' with default mapping...")
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "source": {"type": "keyword"},
                    "destination": {"type": "keyword"},
                    "cost": {"type": "float"},
                    "agent_timestamp": {"type": "date"}
                }
            }
        })
    else:
        log.info(f"✅ Index '{index_name}' already exists.")

    indexed_count = 0
    for record in records:
        try:
            data = json.loads(record['value'].decode())
            es.index(index=index_name, document=data)
            indexed_count += 1
        except Exception as e:
            log.error(f"❌ Indexing error: {e} | Record: {record}")

    log.info(f"✅ Indexed {indexed_count}/{len(records)} records into '{index_name}'.")

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
    )

    consume_result >> index_elasticsearch

if __name__ == "__main__":
    dag2.cli()
