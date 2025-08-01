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

from datetime import datetime

def transform(data):
    flat = {
        "nomclient": data["properties-client"]["nomclient"],
        "telephoneClient": data["properties-client"]["telephoneClient"],
        "locationClient": f"{data['properties-client']['logitude']}, {data['properties-client']['latitude']}",
        "distance": data["distance"],
        "confort": data["confort"],
        "prix_travel": data["prix_travel"],
        "nomDriver": data["properties-driver"]["nomDriver"],
        "locationDriver": f"{data['properties-driver']['logitude']}, {data['properties-driver']['latitude']}",
        "telephoneDriver": data["properties-driver"]["telephoneDriver"],
        "agent_timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return flat


# Function used by the Kafka consume operator to receive and prepare records
def transform_message_to_es(message, **kwargs):
    try:
        raw = message.value().decode()
        original = json.loads(raw)

        # 🔁 Apply your custom transformation
        transformed = transform(original)

        log.info(f"📩 Transformed message: {transformed}")
        return [{"value": json.dumps(transformed).encode()}]
    except Exception as e:
        log.error(f"❌ Failed to parse/transform message: {e}")
        return []

# Function to index records into Elasticsearch
def index_to_elasticsearch(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='consume_kafka_output')

    if not records:
        log.warning("⚠️ No messages pulled from Kafka.")
        return

    es_hook = ElasticsearchHook(elasticsearch_conn_id="elasticsearch_default")
    es = es_hook.get_conn()
    index_name = 'groupe_tfig'

    # Create index if it doesn't exist
    if not es.indices.exists(index=index_name):
        log.info(f"🔧 Creating Elasticsearch index '{index_name}'...")
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "nomclient": {"type": "keyword"},
                    "telephoneClient": {"type": "keyword"},
                    "locationClient": {"type": "geo_point"},
                    "distance": {"type": "float"},
                    "confort": {"type": "keyword"},
                    "prix_travel": {"type": "float"},
                    "nomDriver": {"type": "keyword"},
                    "locationDriver": {"type": "geo_point"},
                    "telephoneDriver": {"type": "keyword"},
                    "agent_timestamp": {"type": "date"}
                }
            }
        })
    else:
        log.info(f"✅ Elasticsearch index '{index_name}' exists.")

    indexed = 0
    for record in records:
        try:
            doc = json.loads(record['value'].decode())
            es.index(index=index_name, document=doc)
            indexed += 1
        except Exception as e:
            log.error(f"❌ Error indexing document: {e} | Record: {record}")

    log.info(f"✅ Indexed {indexed}/{len(records)} documents into '{index_name}'.")

# DAG definition
with DAG(
    dag_id='dag2_kafka_to_elasticsearch',
    default_args=default_args,
    schedule_interval=timedelta(seconds=45),  # ⏱️ Every 45 seconds
    catchup=False,
    description='Consumes Kafka result topic and indexes transformed data into Elasticsearch',
) as dag2:

    consume_result = ConsumeFromTopicOperator(
        task_id='consume_kafka_output',
        topics=['result_gora'],
        kafka_config_id='kafka_default',
        apply_function=transform_message_to_es,
        commit_cadence="end_of_batch",
        max_messages=100,
    )

    index_elasticsearch = PythonOperator(
        task_id='write_to_elasticsearch',
        python_callable=index_to_elasticsearch,
    )

    consume_result >> index_elasticsearch

if __name__ == "__main__":
    dag2.cli()
