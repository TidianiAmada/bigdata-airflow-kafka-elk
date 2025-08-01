import json
import subprocess
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator


default_args = {
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

import json
from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def compute_cost(record):
    client = record["properties-client"]
    driver = record["properties-driver"]
    distance = haversine(client["latitude"], client["logitude"], driver["latitude"], driver["logitude"])
    cost = distance * record["prix_base_per_km"]

    record["distance"] = round(distance, 3)
    record["prix_travel"] = round(cost, 2)
    return record


# Function that Kafka Consume operator will call
def process_kafka_messages(messages, **kwargs):
    results = []
    for msg in messages:
        try:
            data = json.loads(msg.value().decode())
            enriched = compute_cost(data)
            results.append({"value": json.dumps(enriched).encode()})
        except Exception as e:
            print(f"Invalid message: {e}")
    return results

with DAG(
    dag_id='dag1_kafka_compute_cost',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description='Consumes travel data from Kafka, computes cost, and publishes result back to Kafka',
) as dag1:


    consume_kafka = ConsumeFromTopicOperator(
        task_id='consume_kafka_source',
        topics=['source_issa'],
        kafka_config_id='kafka_default',
        apply_function=process_kafka_messages, 
        commit_cadence="end_of_batch",
        max_messages=100
    )

    produce_kafka = ProduceToTopicOperator(
        task_id='publish_kafka_result',
        topic='result_gora',
        kafka_config_id='kafka_default',
        producer_function=process_kafka_messages
    )


    consume_kafka >> produce_kafka

if __name__ == "__main__":
    dag1.cli()
