import os
import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from utils.transform_json import transform

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASS = os.getenv("ELASTIC_PASS")

es = Elasticsearch(ELASTIC_HOST, basic_auth=(ELASTIC_USER, ELASTIC_PASS))

consumer = KafkaConsumer('result', bootstrap_servers=KAFKA_BROKER, auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for msg in consumer:
    flat = transform(msg.value)
    es.index(index="travel-data-tjni", document=flat)
