
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_timestamp,
    to_date,
    expr,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "binance.trades.raw"
)
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-2")

if not S3_BUCKET:
    raise RuntimeError("Set the S3_BUCKET environment variable first.")

S3_OUTPUT_PATH = (
    f"s3a://{S3_BUCKET}/landing/streaming/trades/"
)
S3_CHECKPOINT_PATH = (
    f"s3a://{S3_BUCKET}/metadata/checkpoints/"
    "binance_trades_streaming/"
)

TRADE_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("trade_time_ms", LongType(), True),
    StructField("event_time_ms", LongType(), True),
    StructField("is_buyer_market_maker", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("BinanceWebSocketKafkaToS3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # S3A uses the AWS credential provider chain by default.
    # This allows standard environment or AWS profile credentials.
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    hadoop_conf.set("fs.s3a.endpoint.region", AWS_REGION)

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )

    raw_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    parsed_df = raw_df.withColumn(
        "trade",
        from_json(col("raw_json"), TRADE_SCHEMA),
    )

    valid_df = (
        parsed_df
        .filter(col("trade").isNotNull())
        .select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_json",
            col("trade.source").alias("source"),
            col("trade.event_type").alias("event_type"),
            col("trade.symbol").alias("symbol"),
            col("trade.trade_id").alias("trade_id"),
            col("trade.price").cast("decimal(30, 12)").alias("price"),
            col("trade.quantity").cast("decimal(30, 12)").alias("quantity"),
            (
                expr("timestamp_millis(trade.trade_time_ms)")
            ).alias("trade_timestamp"),
            (
                expr("timestamp_millis(trade.event_time_ms)")
            ).alias("event_timestamp"),
            col("trade.is_buyer_market_maker").alias(
                "is_buyer_market_maker"
            ),
            to_timestamp(
                col("trade.ingested_at")
            ).alias("producer_ingested_at"),
            current_timestamp().alias("spark_processed_at"),
        )
        .filter(
            col("symbol").isNotNull()
            & col("trade_id").isNotNull()
            & col("price").isNotNull()
            & col("quantity").isNotNull()
            & col("trade_timestamp").isNotNull()
        )
        .withColumn(
            "event_date",
            to_date(col("trade_timestamp")),
        )
    )

    query = (
        valid_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", S3_OUTPUT_PATH)
        .option("checkpointLocation", S3_CHECKPOINT_PATH)
        .partitionBy("symbol", "event_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Spark streaming query started")
    logger.info("Kafka topic: %s", KAFKA_TOPIC)
    logger.info("S3 destination: %s", S3_OUTPUT_PATH)

    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
