FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir kafka-python elasticsearch apache-airflow-providers-elasticsearch

CMD ["python", "producer.py"]
