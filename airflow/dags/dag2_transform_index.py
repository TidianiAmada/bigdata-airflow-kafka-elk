import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import get_current_context
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch

log = LoggingMixin().log

default_args = {
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# Shared directory (should exist in mounted volume)
DATA_DIR = "/opt/airflow/logs/processed_es"
os.makedirs(DATA_DIR, exist_ok=True)


def transform(record: dict) -> dict:
    """Flatten the input travel record and add timestamp."""
    return {
        "nomclient": record["properties-client"]["nomclient"],
        "telephoneClient": record["properties-client"]["telephoneClient"],
        "locationClient": f"{record['properties-client']['logitude']}, {record['properties-client']['latitude']}",
        "distance": record["distance"],
        "confort": record["confort"],
        "prix_travel": record["prix_travel"],
        "nomDriver": record["properties-driver"]["nomDriver"],
        "locationDriver": f"{record['properties-driver']['logitude']}, {record['properties-driver']['latitude']}",
        "telephoneDriver": record["properties-driver"]["telephoneDriver"],
        "agent_timestamp": datetime.utcnow().isoformat() + "Z"
    }


def transform_message_to_file(message):
    """Called for each Kafka message — transform and write to shared file."""
    context = get_current_context()
    dag_run_id = context['dag_run'].run_id
    file_path = os.path.join(DATA_DIR, f"es_records_{dag_run_id}.jsonl")

    try:
        record = json.loads(message.value().decode())
        flat = transform(record)

        with open(file_path, "a") as f:
            f.write(json.dumps(flat) + "\n")

        log.info(f"✅ Written transformed record to {file_path}")
        return {"value": json.dumps(flat).encode("utf-8")}  # still returned for Kafka acknowledgment
    except Exception as e:
        log.error(f"❌ Failed to transform message: {e}")
        return None


def get_elasticsearch_client():
    """Return authenticated Elasticsearch client using Airflow connection."""
    conn = BaseHook.get_connection("elasticsearch_default")
    return Elasticsearch(
        hosts=["http://clustersdaelatsic.eastus.cloudapp.azure.com:9200"],
        basic_auth=("elastic", "changeme"),
    )


def index_to_elasticsearch():
    """Read from file and index into Elasticsearch."""
    context = get_current_context()
    dag_run_id = context['dag_run'].run_id
    file_path = os.path.join(DATA_DIR, f"es_records_{dag_run_id}.jsonl")

    if not os.path.exists(file_path):
        log.warning(f"⚠️ File not found: {file_path}")
        return

    es = get_elasticsearch_client()
    index_name = "groupe_tfig"

    # Ensure index exists
    if not es.indices.exists(index=index_name):
        log.info(f"🔧 Creating index '{index_name}'...")
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
        log.info(f"✅ Index '{index_name}' already exists.")

    # Index each line in the file
    indexed = 0
    with open(file_path, "r") as f:
        for line in f:
            try:
                doc = json.loads(line)
                es.index(index=index_name, document=doc)
                indexed += 1
            except Exception as e:
                log.error(f"❌ Failed to index document: {e}")

    log.info(f"✅ Indexed {indexed} documents to '{index_name}'.")


# DAG Definition
with DAG(
    dag_id="dag2_kafka_to_elasticsearch",
    default_args=default_args,
    schedule_interval=timedelta(seconds=45),
    catchup=False,
    description="Consumes result from Kafka and indexes into Elasticsearch",
) as dag2:

    consume_result = ConsumeFromTopicOperator(
        task_id="consume_kafka_output",
        topics=["result_gora"],
        kafka_config_id="kafka_default",
        apply_function=transform_message_to_file,
        commit_cadence="end_of_batch",
        max_messages=100,
    )

    index_elasticsearch = PythonOperator(
        task_id="write_to_elasticsearch",
        python_callable=index_to_elasticsearch,
    )

    consume_result >> index_elasticsearch

if __name__ == "__main__":
    dag2.cli()
