# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/01.bronze_ingestion

# COMMAND ----------

BRONZE_TRADES = f"{CATALOG}.bronze.streaming_trades_raw"
SILVER_TRADES = f"{CATALOG}.silver.market_trades"
QUARANTINE_TRADES = f"{CATALOG}.silver.quarantine_trades"


raw = spark.table(BRONZE_TRADES)


# Convert epoch values to timestamps.
# Supports epoch seconds, milliseconds, and microseconds.
def epoch_to_timestamp(column_name):
    c = F.col(column_name)
    n = c.cast("long")

    return (
        F.when(
            F.abs(n) >= F.lit(100_000_000_000_000),
            F.expr(f"timestamp_micros(CAST(`{column_name}` AS BIGINT))"),
        )
        .when(
            F.abs(n) >= F.lit(100_000_000_000),
            F.expr(f"timestamp_millis(CAST(`{column_name}` AS BIGINT))"),
        )
        .when(
            F.abs(n) >= F.lit(1_000_000_000),
            F.to_timestamp(F.from_unixtime(n)),
        )
        .otherwise(F.to_timestamp(c))
    )


if "trade_time_ms" in raw.columns:
    trade_ts = epoch_to_timestamp("trade_time_ms")
elif "trade_timestamp" in raw.columns:
    trade_ts = (
        F.to_timestamp("trade_timestamp")
        if dict(raw.dtypes)["trade_timestamp"] == "string"
        else F.col("trade_timestamp").cast("timestamp")
    )
else:
    raise ValueError(
        "No trade timestamp column found. Inspect the Bronze schema."
    )


if "is_buyer_market_maker" in raw.columns:
    buyer_is_maker = F.col("is_buyer_market_maker").cast("boolean")
else:
    buyer_is_maker = F.lit(None).cast("boolean")


# COMMAND ----------

# Normalize names and data types.
prepared = (
    raw
    .withColumn("symbol", F.upper(F.trim(F.col("symbol").cast("string"))))
    .withColumn("trade_id", F.col("trade_id").cast("string"))
    .withColumn("price", F.col("price").cast("decimal(30, 12)"))
    .withColumn("quantity", F.col("quantity").cast("decimal(30, 12)"))
    .withColumn("trade_timestamp", trade_ts)
    .withColumn("is_buyer_market_maker", buyer_is_maker)
    .withColumn(
        "quote_value",
        F.col("price") * F.col("quantity"),
    )
    .withColumn("source_file", F.col("_source_file"))
    .withColumn("ingested_at", F.col("_ingested_at"))
)


# COMMAND ----------

# Keep the quarantine reason explicit.
prepared = prepared.withColumn(
    "rejection_reason",
    F.when(F.col("symbol").isNull(), "missing_symbol")
    .when(~F.col("symbol").isin("BTCUSDT", "ETHUSDT", "SOLUSDT"),
          "unsupported_symbol")
    .when(F.col("trade_id").isNull(), "missing_trade_id")
    .when(F.col("trade_timestamp").isNull(), "invalid_trade_timestamp")
    .when(F.col("price").isNull() | (F.col("price") <= 0), "invalid_price")
    .when(
        F.col("quantity").isNull() | (F.col("quantity") <= 0),
        "invalid_quantity",
    )
)


rejected = prepared.filter(F.col("rejection_reason").isNotNull())
valid = prepared.filter(F.col("rejection_reason").isNull())


# Preserve invalid records and their reasons.
if rejected.limit(1).count() > 0:
    (
        rejected
        .withColumn("quarantined_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TRADES)
    )


# If duplicate records exist, keep the most recently ingested version.
dedup_window = (
    Window
    .partitionBy("symbol", "trade_id")
    .orderBy(F.col("ingested_at").desc_nulls_last())
)

valid = (
    valid
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "rejection_reason")
)


# Select the stable Silver contract.
silver_df = valid.select(
    "symbol",
    "trade_id",
    "trade_timestamp",
    "price",
    "quantity",
    "quote_value",
    "is_buyer_market_maker",
    "ingested_at",
    "source_file",
)

# COMMAND ----------


# Initial creation; subsequent runs perform an idempotent upsert.
if not spark.catalog.tableExists(SILVER_TRADES):
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_TRADES)
    )
else:
    target = DeltaTable.forName(spark, SILVER_TRADES)

    (
        target.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.symbol = s.symbol AND t.trade_id = s.trade_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

print("Silver trades completed")
print("Valid records:", silver_df.count())
print("Rejected records:", rejected.count())