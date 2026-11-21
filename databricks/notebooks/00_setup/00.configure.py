# Databricks notebook source
CATALOG = "binance_data"

BUCKET = "binance-market-data-lake"
S3_ROOT = f"s3a://{BUCKET}"

HISTORICAL_KLINES_PATH = f"{S3_ROOT}/landing/historical/klines/"
REST_KLINES_PATH = f"{S3_ROOT}/landing/rest/klines/"
STREAMING_TRADES_PATH = f"{S3_ROOT}/landing/streaming/trades/"

METADATA_ROOT = f"{S3_ROOT}/metadata/databricks"



# COMMAND ----------

HISTORICAL_SCHEMA_PATH = f"{METADATA_ROOT}/schemas/bronze_historical_klines/"
REST_SCHEMA_PATH = f"{METADATA_ROOT}/schemas/bronze_rest_klines/"
TRADES_SCHEMA_PATH = f"{METADATA_ROOT}/schemas/bronze_streaming_trades/"

HISTORICAL_CHECKPOINT = f"{METADATA_ROOT}/checkpoints/bronze_historical_klines/"
REST_CHECKPOINT = f"{METADATA_ROOT}/checkpoints/bronze_rest_klines/"
TRADES_CHECKPOINT = f"{METADATA_ROOT}/checkpoints/bronze_streaming_trades/"

# COMMAND ----------

print("Catalog:", CATALOG)
print("Historical source:", HISTORICAL_KLINES_PATH)
print("REST source:", REST_KLINES_PATH)
print("Streaming source:", STREAMING_TRADES_PATH)