from confluent_kafka import Producer
from collection import collector
import json, time

def publish_to_kafka(topic, data):
    try:
        producer.produce(topic, value=json.dumps(data))
        producer.flush()  # Ensure the message is sent
        print("Message published to Kafka")
    except Exception as e:
        print(f"Failed to publish to Kafka: {e}")

if __name__ == '__main__':
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    }

    producer = Producer(kafka_config)
    TOPIC = "live-games"
    while True:
        # Step 1: Retrieve data from API
        data = collector.fetch()
        
        # Step 2: Publish data to Kafka
        if data:
            publish_to_kafka(TOPIC, data)
        
        # Step 3: Sleep for a short duration (polling interval)
        time.sleep(10)  # Fetch data every 10 seconds