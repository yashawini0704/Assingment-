from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType
from google.cloud import pubsub_v1
import pandas as pd

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("StockAnomalyDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Configuration
KAFKA_TOPIC = "input-topic"
KAFKA_BOOTSTRAP_SERVERS = "34.94.213.162:9092"
PROJECT_ID = "verdant-descent-461318-i0"
TOPIC_NAME = "a2_anomaly_topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# Define schema
schema = StructType() \
    .add("stock_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("close_price", DoubleType()) \
    .add("volume", LongType())

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse and prepare timestamped data
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .withWatermark("timestamp", "10 minutes")

# A1: Price deviation
price_window = df \
    .groupBy("stock_id", window("timestamp", "1 minute")) \
    .agg(
        last("close_price").alias("current_price"),
        last("timestamp").alias("current_timestamp"),
        first("close_price").alias("prev_price"),
        first("timestamp").alias("prev_timestamp"),
        last("volume").alias("volume")
    ) \
    .filter(col("current_price").isNotNull() & col("prev_price").isNotNull() &
            (col("current_timestamp") > col("prev_timestamp")))

price_deviation = price_window \
    .withColumn("price_diff_pct", abs(col("current_price") - col("prev_price")) / col("prev_price") * 100)

anomaly_a1 = price_deviation \
    .filter(col("price_diff_pct") > 0.5) \
    .select(
        "stock_id",
        col("current_timestamp").alias("timestamp"),
        col("current_price").alias("close_price"),
        "volume",
        lit("A1").alias("anomaly_type")
    )

# A2: Volume spike
volume_avg = df \
    .groupBy("stock_id", window("timestamp", "10 minutes", "1 minute")) \
    .agg(
        avg("volume").alias("avg_volume"),
        last("close_price").alias("close_price"),
        last("timestamp").alias("timestamp"),
        last("volume").alias("volume")
    )

volume_with_avg = volume_avg \
    .withColumn("volume_diff_pct", (col("volume") - col("avg_volume")) / col("avg_volume") * 100)

anomaly_a2 = volume_with_avg \
    .filter(col("volume_diff_pct") > 2) \
    .select(
        "stock_id",
        "timestamp",
        "close_price",
        "volume",
        lit("A2").alias("anomaly_type")
    )

# Combine anomalies
anomalies = anomaly_a1.unionByName(anomaly_a2)

# Console output for monitoring
console_query = anomalies.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Pub/Sub sink for A2
def publish_to_pubsub(df, epoch_id):
    try:
        filtered_df = df.filter(df.anomaly_type == "A2")
        if filtered_df.isEmpty():
            return

        a2_df = filtered_df.toPandas()

        if "timestamp" in a2_df.columns:
            a2_df["timestamp"] = pd.to_datetime(a2_df["timestamp"], errors="coerce")

        for _, row in a2_df.iterrows():
            message = f"Traded Volume more than 2% of its average: {row.stock_id} at {row.timestamp}"
            publisher.publish(topic_path, message.encode("utf-8")).result()

    except Exception as e:
        print(f"Error publishing to PubSub: {e}")

# Write stream to Pub/Sub
pubsub_query = anomalies.writeStream \
    .outputMode("append") \
    .foreachBatch(publish_to_pubsub) \
    .start()

# Keep streams running
spark.streams.awaitAnyTermination()
