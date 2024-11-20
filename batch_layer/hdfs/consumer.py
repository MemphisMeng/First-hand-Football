from confluent_kafka import Consumer, KafkaException
from hdfs.client import InsecureClient
import time

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'hdfs-consumer-group',
    'auto.offset.reset': 'earliest',
}
consumer = Consumer(consumer_conf)
TOPIC = 'live-games'
consumer.subscribe([TOPIC])

# HDFS Configuration
hdfs_client = InsecureClient('http://localhost:9870', user='memphis')  # Replace with your HDFS Namenode URL
hdfs_dir = "/user/memphis/warehouse"
file_name = f"{hdfs_dir}/output_{int(time.time())}.json"

# Ensure HDFS directory exists
if not hdfs_client.content(hdfs_dir, strict=False):
    hdfs_client.makedirs(hdfs_dir)

# Start consuming Kafka messages and writing to HDFS
try:
    print(f"Starting to consume from Kafka topic: {TOPIC}")
    batch, batch_size = [], 10
    with hdfs_client.write(file_name, encoding='utf-8') as hdfs_file:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()}")
                else:
                    print(f"Error: {msg.error()}")
                continue

            batch.append(msg.value().decode('utf-8'))
            if len(batch) >= batch_size:
                hdfs_file.write('\n'.join(batch) + '\n')
                batch = []
                print(f"Written to HDFS!")

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Consumer closed.")

# /tmp/hadoop-memphis/dfs/name