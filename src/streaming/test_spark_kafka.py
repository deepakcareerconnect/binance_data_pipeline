
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "binance.trades.raw"

schema = StructType([
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

spark = (
    SparkSession.builder
    .appName("BinanceKafkaReadTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

trades = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("trade"))
    .select("trade.*")
    .withColumn("price", col("price").cast("double"))
    .withColumn("quantity", col("quantity").cast("double"))
)

query = (
    trades.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "/tmp/binance-kafka-read-test-checkpoint-v2",
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
