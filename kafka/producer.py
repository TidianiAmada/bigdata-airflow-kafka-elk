import os
import json
import logging
import socket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

if not KAFKA_BROKER:
    logger.error("❌ KAFKA_BROKER environment variable is not set.")
    exit(1)

host, port = KAFKA_BROKER.split(":")
port = int(port)

# Test TCP connection
logger.info(f"🔌 Testing connection to Kafka broker at {KAFKA_BROKER}...")
try:
    with socket.create_connection((host, port), timeout=10):
        logger.info(f"✅ Connection to {KAFKA_BROKER} successful.")
except Exception as e:
    logger.error(f"❌ Cannot connect to Kafka broker at {KAFKA_BROKER}: {e}")
    exit(1)

# Initialize Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("🟢 KafkaProducer initialized successfully.")
except NoBrokersAvailable as e:
    logger.error(f"❌ Kafka broker not available: {e}")
    exit(1)

# Load and send data
try:
    with open("/data/data_projet.json") as f:
        data = json.load(f)

    for i, record in enumerate(data["data"]):
        producer.send('source_fatou', value=record)
        logger.info(f"📤 Record {i+1} sent to topic 'source'.")

    producer.flush()
    producer.close()
    logger.info("✅ All data sent and producer closed cleanly.")

except FileNotFoundError:
    logger.error("❌ Data file '/data/data_projet.json' not found.")
except Exception as e:
    logger.error(f"❌ Unexpected error while sending data: {e}")
