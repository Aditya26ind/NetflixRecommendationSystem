"""Kafka → S3 + Redis streaming job (skeleton)."""
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os
from .stream_utils import create_spark

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
S3_PATH = os.getenv("S3_RAW", "s3a://datalake/raw/user_events")
CHECKPOINT = os.getenv("CHECKPOINT", "s3a://datalake/checkpoints/streaming")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("item_id", StringType()),
    StructField("device", StringType()),
    StructField("context", StringType()),
])


def main():
    spark = create_spark()
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .load()
    )

    events = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    query_raw = (
        events.writeStream
        .format("parquet")
        .option("path", S3_PATH)
        .option("checkpointLocation", f"{CHECKPOINT}/raw")
        .start()
    )

    # TODO: foreachBatch sink to update Redis features

    spark.streams.awaitAnyTermination()
    query_raw.awaitTermination()


if __name__ == "__main__":
    main()
