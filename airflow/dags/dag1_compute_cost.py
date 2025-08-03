import json
import os
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.operators.python import get_current_context
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

# Base path for storing intermediate data — must be a shared volume
DATA_DIR = "/opt/airflow/logs/processed"
os.makedirs(DATA_DIR, exist_ok=True)

# Default arguments
default_args = {
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# Haversine logic
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# Cost computation logic
def compute_cost(record):
    client = record["properties-client"]
    driver = record["properties-driver"]
    distance = haversine(client["latitude"], client["logitude"], driver["latitude"], driver["logitude"])
    cost = distance * record["prix_base_per_km"]
    record["distance"] = round(distance, 3)
    record["prix_travel"] = round(cost, 2)
    return record

# Kafka message processing function writing to shared volume file
def process_kafka_messages(message):
    context = get_current_context()
    dag_run_id = context['dag_run'].run_id
    file_path = os.path.join(DATA_DIR, f"records_{dag_run_id}.jsonl")

    try:
        raw = message.value().decode()
        log.info(f"📥 Received message: {raw}")
        record = json.loads(raw)
        result = compute_cost(record)
        log.info(f"✅ Computed result: {result}")

        with open(file_path, "a") as f:
            f.write(json.dumps({"value": json.dumps(result).encode("utf-8").decode("utf-8")}) + "\n")

        return {"value": json.dumps(result).encode("utf-8")}
    except Exception as e:
        log.error(f"❌ Skipping invalid message: {e}")
        return None

# Kafka producer reading from shared volume file
def produce_from_file():
    context = get_current_context()
    dag_run_id = context['dag_run'].run_id
    file_path = os.path.join(DATA_DIR, f"records_{dag_run_id}.jsonl")

    if not os.path.exists(file_path):
        log.warning(f"⚠️ No file found at {file_path}")
        return

    with open(file_path, "r") as f:
        for i, line in enumerate(f, 1):
            try:
                record = json.loads(line)
                value = record["value"].encode("utf-8")
                log.info(f"🚀 Producing record #{i}: {record}")
                yield (None, value)
            except Exception as e:
                log.error(f"❌ Failed to yield record #{i}: {e}")

# DAG definition
with DAG(
    dag_id='dag1_kafka_compute_cost',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    description='Consumes Kafka travel data, computes cost, and republishes enriched data',
) as dag1:

    consume_kafka = ConsumeFromTopicOperator(
        task_id='consume_kafka_trafic',
        topics=['source_issa'],
        kafka_config_id='kafka_default',
        apply_function=process_kafka_messages,
        commit_cadence="end_of_batch",
        max_messages=100,
    )

    produce_kafka = ProduceToTopicOperator(
        task_id='produce_kafka_result',
        topic='result_gora',
        kafka_config_id='kafka_default',
        producer_function=produce_from_file,
    )

    consume_kafka >> produce_kafka

if __name__ == "__main__":
    dag1.cli()
