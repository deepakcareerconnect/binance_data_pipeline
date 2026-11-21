# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/00.configure

# COMMAND ----------

print("Catalog:", CATALOG)
print("Historical source:", HISTORICAL_KLINES_PATH)
print("REST source:", REST_KLINES_PATH)
print("Streaming source:", STREAMING_TRADES_PATH)

# COMMAND ----------

# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = "binance_data"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

BUCKET = "binance-market-data-lake"
S3_ROOT = f"s3a://{BUCKET}"

LANDING_ROOT = f"{S3_ROOT}/landing"
METADATA_ROOT = f"{S3_ROOT}/metadata/databricks"

# S3 source paths
HISTORICAL_KLINES_PATH = (
    f"{LANDING_ROOT}/historical/klines/"
)

# Use the rebuilt REST prefix containing corrected Parquet files
REST_KLINES_PATH = (
    f"{LANDING_ROOT}/rest/klines/"
)

STREAMING_TRADES_PATH = (
    f"{LANDING_ROOT}/streaming/trades_recovery/"
)

# Bronze Delta tables
HISTORICAL_TABLE = f"{BRONZE_SCHEMA}.historical_klines_raw"
REST_TABLE = f"{BRONZE_SCHEMA}.rest_klines_raw"
TRADES_TABLE = f"{BRONZE_SCHEMA}.streaming_trades_raw"

# Separate schema and checkpoint locations for each source
RUN_SUFFIX = "bronze_rebuild_20261021_v3"

# Streaming-only suffix. Historical and REST checkpoints remain on RUN_SUFFIX
# so this switch cannot reprocess or duplicate those sources.
STREAMING_RUN_SUFFIX = "bronze_streaming_recovery_20261101_v1"

HISTORICAL_SCHEMA_PATH = (
    f"{METADATA_ROOT}/schemas/historical_klines/{RUN_SUFFIX}/"
)
HISTORICAL_CHECKPOINT = (
    f"{METADATA_ROOT}/checkpoints/historical_klines/{RUN_SUFFIX}/"
)

REST_SCHEMA_PATH = (
    f"{METADATA_ROOT}/schemas/rest_klines/{RUN_SUFFIX}/"
)
REST_CHECKPOINT = (
    f"{METADATA_ROOT}/checkpoints/rest_klines/{RUN_SUFFIX}/"
)

TRADES_SCHEMA_PATH = (
    f"{METADATA_ROOT}/schemas/streaming_trades/{STREAMING_RUN_SUFFIX}/"
)
TRADES_CHECKPOINT = (
    f"{METADATA_ROOT}/checkpoints/streaming_trades/{STREAMING_RUN_SUFFIX}/"
)

# Keep timestamp interpretation consistent with UTC
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Create Bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

print("Catalog:", CATALOG)
print("Bronze schema:", BRONZE_SCHEMA)
print("Historical source:", HISTORICAL_KLINES_PATH)
print("REST source:", REST_KLINES_PATH)
print("Streaming source:", STREAMING_TRADES_PATH)

# COMMAND ----------

# ============================================================
# 2. REST TIMESTAMP CORRECTION
# ============================================================

def fix_rest_timestamps(df):
    """
    Correct REST timestamps affected by the observed
    millisecond-to-microsecond conversion error.

    Only modifies:
        open_time
        close_time

    All other columns remain unchanged.
    """

    required_columns = ["open_time", "close_time"]

    missing = [
        c for c in required_columns
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            f"REST data is missing timestamp columns: {missing}"
        )

    cutoff = F.lit(
        "2020-01-01 00:00:00"
    ).cast("timestamp_ntz")

    for column_name in required_columns:

        # Convert only suspicious historical timestamps.
        # The multiplier corrects the observed 1/1000 epoch-scale error.
        corrected_timestamp = F.expr(
            f"timestamp_micros(unix_micros(`{column_name}`) * 1000)"
        ).cast("timestamp_ntz")

        df = df.withColumn(
            column_name,
            F.when(
                F.col(column_name) < cutoff,
                corrected_timestamp
            ).otherwise(F.col(column_name))
        )

    return df

# COMMAND ----------

# ============================================================
# 3. VALIDATE REST SOURCE
# ============================================================

rest_source_df = spark.read.parquet(REST_KLINES_PATH)

required_rest_columns = [
    "symbol",
    "interval",
    "open_time",
    "close_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
]

missing_columns = [
    c for c in required_rest_columns
    if c not in rest_source_df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing REST columns: {missing_columns}"
    )

# Apply timestamp correction for validation
rest_fixed_df = fix_rest_timestamps(rest_source_df)

# Check that no suspicious timestamps remain
cutoff = F.lit(
    "2020-01-01 00:00:00"
).cast("timestamp_ntz")

invalid_rest_df = rest_fixed_df.filter(
    F.col("symbol").isNull()
    | F.col("open_time").isNull()
    | F.col("close_time").isNull()
    | (F.col("open_time") < cutoff)
    | (F.col("close_time") < cutoff)
    | (F.col("close_time") < F.col("open_time"))
)

invalid_count = invalid_rest_df.count()

print("REST source rows:", rest_fixed_df.count())
print("Invalid REST rows after correction:", invalid_count)

display(
    rest_fixed_df
    .groupBy("symbol")
    .agg(
        F.count("*").alias("total_rows"),
        F.min("open_time").alias("first_open_time"),
        F.max("open_time").alias("last_open_time"),
        F.min("close_time").alias("first_close_time"),
        F.max("close_time").alias("last_close_time"),
    )
    .orderBy("symbol")
)

if invalid_count > 0:
    display(invalid_rest_df.limit(20))
    raise ValueError(
        "REST validation failed. Do not ingest until the source "
        "timestamps and their original conversion are verified."
    )

print("REST validation passed.")

# COMMAND ----------

def ingest_files_to_bronze(
    source_path: str,
    schema_path: str,
    checkpoint_path: str,
    target_table: str,
    source_system: str,
    file_format: str = "parquet",
):
    supported_formats = {"parquet", "json", "csv"}

    if file_format.lower() not in supported_formats:
        raise ValueError(
            f"Unsupported file format: {file_format}"
        )

    print("=" * 60)
    print(f"Starting ingestion: {source_system}")
    print(f"Source: {source_path}")
    print(f"Target: {target_table}")
    print("=" * 60)

    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format.lower())
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .load(source_path)
    )

    # Correct only REST timestamps
    if source_system == "binance_rest":
        source_df = fix_rest_timestamps(source_df)

    # Unity Catalog-compatible source file metadata
    source_df = (
        source_df
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )
        .withColumn(
            "_source_system",
            F.lit(source_system)
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp()
        )
    )

    query = (
        source_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    print(f"Ingestion completed: {target_table}")

    return spark.table(target_table)

# COMMAND ----------

# ============================================================
# 5. HISTORICAL KLINES -> BRONZE
# ============================================================

historical_bronze_df = ingest_files_to_bronze(
    source_path=HISTORICAL_KLINES_PATH,
    schema_path=HISTORICAL_SCHEMA_PATH,
    checkpoint_path=HISTORICAL_CHECKPOINT,
    target_table=HISTORICAL_TABLE,
    source_system="binance_vision",
    file_format="parquet",
)

print("Historical Bronze ingestion completed.")

# COMMAND ----------

# ============================================================
# 6. REST KLINES -> BRONZE
# ============================================================

rest_bronze_df = ingest_files_to_bronze(
    source_path=REST_KLINES_PATH,
    schema_path=REST_SCHEMA_PATH,
    checkpoint_path=REST_CHECKPOINT,
    target_table=REST_TABLE,
    source_system="binance_rest",
    file_format="parquet",
)

print("REST Bronze ingestion completed.")

# COMMAND ----------

# ============================================================
# 7. STREAMING TRADES -> BRONZE
# ============================================================

trades_bronze_df = ingest_files_to_bronze(
    source_path=STREAMING_TRADES_PATH,
    schema_path=TRADES_SCHEMA_PATH,
    checkpoint_path=TRADES_CHECKPOINT,
    target_table=TRADES_TABLE,
    source_system="binance_stream",
    file_format="parquet",
)

print("Streaming Trades Bronze ingestion completed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM binance_data.bronze.streaming_trades_raw
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM binance_data.bronze.historical_klines_raw
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM binance_data.bronze.rest_klines_raw
# MAGIC LIMIT 10;

# COMMAND ----------

spark.table(
    "binance_data.bronze.streaming_trades_raw"
).printSchema()

spark.table(
    "binance_data.bronze.historical_klines_raw"
).printSchema()

spark.table(
    "binance_data.bronze.rest_klines_raw"
).printSchema()