
import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)

# ============================================================
# 1. CONFIGURATION
# ============================================================

APP_NAME = "binance-kafka-to-s3-streaming"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "binance.trades.raw"
)

S3_OUTPUT_PATH = os.getenv(
    "S3_OUTPUT_PATH",
    "s3a://binance-market-data-lake/landing/streaming/trades_recovery_v2/"
)

CHECKPOINT_PATH = os.getenv(
    "S3_CHECKPOINT_PATH",
    "s3a://binance-market-data-lake/metadata/checkpoints/binance_trades_streaming_v3/"
)

KAFKA_STARTING_OFFSETS = os.getenv(
    "KAFKA_STARTING_OFFSETS", "earliest"
)

MAX_OFFSETS_PER_TRIGGER = os.getenv(
    "MAX_OFFSETS_PER_TRIGGER", "10000"
)

TRIGGER_INTERVAL = os.getenv(
    "TRIGGER_INTERVAL", "30 seconds"
)

# ============================================================
# 2. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(APP_NAME)

# ============================================================
# 3. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)

    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "3")
    .config(
        "spark.streaming.stopGracefullyOnShutdown",
        "true"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

logger.info("Starting Binance streaming ingestion")
logger.info("Kafka topic: %s", KAFKA_TOPIC)
logger.info("Kafka brokers: %s", KAFKA_BOOTSTRAP_SERVERS)
logger.info("S3 output: %s", S3_OUTPUT_PATH)
logger.info("Checkpoint: %s", CHECKPOINT_PATH)
logger.info("Starting offsets: %s", KAFKA_STARTING_OFFSETS)

# ============================================================
# 4. EXPECTED KAFKA JSON SCHEMA
# ============================================================

trade_schema = StructType([
    # Standard Binance fields
    StructField("e", StringType(), True),
    StructField("s", StringType(), True),
    StructField("t", StringType(), True),
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("T", StringType(), True),
    StructField("m", BooleanType(), True),

    # Descriptive project fields
    StructField("event_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("trade_timestamp", StringType(), True),
    StructField(
        "is_buyer_market_maker",
        BooleanType(),
        True
    ),
])

# ============================================================
# 5. READ FROM KAFKA
# ============================================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .option("failOnDataLoss", "true")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .load()
)

# ============================================================
# 6. PARSE JSON
# ============================================================

parsed_stream = (
    kafka_stream
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn(
        "payload",
        F.from_json(F.col("raw_json"), trade_schema)
    )
)

# ============================================================
# 7. NORMALIZE TRADE FIELDS
# ============================================================

normalized = (
    parsed_stream
    .withColumn(
        "symbol",
        F.upper(
            F.coalesce(
                F.col("payload.s"),
                F.col("payload.symbol")
            )
        )
    )
    .withColumn(
        "trade_id",
        F.coalesce(
            F.col("payload.t"),
            F.col("payload.trade_id")
        )
    )
    .withColumn(
        "price",
        F.coalesce(
            F.col("payload.p"),
            F.col("payload.price")
        ).cast("double")
    )
    .withColumn(
        "quantity",
        F.coalesce(
            F.col("payload.q"),
            F.col("payload.quantity")
        ).cast("double")
    )
    .withColumn(
        "timestamp_text",
        F.coalesce(
            F.col("payload.T"),
            F.col("payload.trade_timestamp")
        )
    )
    .withColumn(
        "is_buyer_market_maker",
        F.coalesce(
            F.col("payload.m"),
            F.col("payload.is_buyer_market_maker")
        )
    )
    .withColumn(
        "event_type",
        F.coalesce(
            F.col("payload.e"),
            F.col("payload.event_type")
        )
    )
)

# Accept either epoch milliseconds or an ISO timestamp.
normalized = normalized.withColumn(
    "trade_timestamp",
    F.coalesce(
        F.expr(
            "timestamp_millis(try_cast(timestamp_text AS BIGINT))"
        ),
        F.to_timestamp("timestamp_text"),
        F.col("kafka_timestamp")
    )
)

# ============================================================
# 8. VALIDATE RECORDS
# ============================================================

valid_stream = normalized.filter(
    F.col("payload").isNotNull()
    & F.col("symbol").isNotNull()
    & F.col("trade_id").isNotNull()
    & F.col("price").isNotNull()
    & F.col("quantity").isNotNull()
    & F.col("trade_timestamp").isNotNull()
)

# ============================================================
# 9. FINAL S3 DATASET
# ============================================================

final_stream = (
    valid_stream
    .withColumn(
        "quote_value",
        F.col("price") * F.col("quantity")
    )
    .withColumn(
        "event_date",
        F.to_date("trade_timestamp")
    )
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
    .select(
        "symbol",
        "trade_id",
        "trade_timestamp",
        "price",
        "quantity",
        "quote_value",
        "is_buyer_market_maker",
        "event_type",
        "event_date",
        "ingested_at",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "raw_json"
    )
)

# ============================================================
# 10. WRITE CONTINUOUSLY TO S3
# ============================================================

query = (
    final_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", S3_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("symbol", "event_date")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

logger.info("Streaming query started")
logger.info("Query ID: %s", query.id)
logger.info("Run ID: %s", query.runId)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Shutdown requested. Stopping streaming query.")
    query.stop()
finally:
    spark.stop()
