from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import *

TOPIC = 'live-games'
spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

try:
    streaming_df = spark.readStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    print(streaming_df)

except KeyboardInterrupt:
    print("\nStopping consumer...")

except Exception as e:
    print(f"Error: {e}")