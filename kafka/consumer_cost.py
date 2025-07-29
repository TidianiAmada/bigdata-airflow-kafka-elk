import os
import json
from kafka import KafkaConsumer, KafkaProducer
from utils.compose_cost import compute_cost

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

consumer = KafkaConsumer('source', bootstrap_servers=KAFKA_BROKER, auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for msg in consumer:
    enriched = compute_cost(msg.value)
    producer.send('result', value=enriched)
