import csv
import json
import time
from kafka import KafkaProducer
import os


KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
KAFKA_TOPIC = 'browser_history'
RETRY_COUNT = 10
RETRY_DELAY_S = 5


def create_producer():
    """Creates a Kafka producer instance with a retry mechanism"""
    for i in range(RETRY_COUNT):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not available, retrying in {RETRY_DELAY_S}s... ({i+1}/{RETRY_COUNT})")
            time.sleep(RETRY_DELAY_S)
    raise Exception("Could not connect to Kafka after multiple retries")


def stream_history(producer, file_path='history.csv'):
    """Reads a CSV file with columns: order,id,date,time,title,url,visitCount,typedCount,transition and sends each URL to Kafka"""
    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            url = row.get('url')
            if not url: continue
            message = {'url': url}
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent message: {message}")
            time.sleep(0.01)


if __name__ == "__main__":
    kafka_producer = create_producer()
    stream_history(kafka_producer)
    kafka_producer.flush() # ensure all messages are sent
