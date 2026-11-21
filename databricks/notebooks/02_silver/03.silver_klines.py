# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/01.bronze_ingestion 

# COMMAND ----------

# ==========================================
# 03_silver_klines
# =========================================

HIST = f"{CATALOG}.bronze.historical_klines_raw"
REST = f"{CATALOG}.bronze.rest_klines_raw"

SILVER_KLINES = f"{CATALOG}.silver.market_klines"
QUARANTINE_KLINES = f"{CATALOG}.silver.quarantine_klines"


def first_available(df, candidates, required=True):
    """Return the first source column that exists."""
    for name in candidates:
        if name in df.columns:
            return F.col(name)

    if required:
        raise ValueError(
            f"Missing expected source columns. Tried: {candidates}. "
            f"Actual columns: {df.columns}"
        )

    return F.lit(None)


def normalize_klines(df, source_name):
    # Identify active timestamp column names in the source dataframe
    open_time_col = next((n for n in ["open_time", "open_time_ms", "open_time_us"] if n in df.columns), None)
    close_time_col = next((n for n in ["close_time", "close_time_ms", "close_time_us"] if n in df.columns), None)

    if not open_time_col:
        raise ValueError(f"Missing open time column in {source_name}. Available: {df.columns}")

    # Helper to safely build timestamp expressions based on source data type
    def build_timestamp_expr(col_name):
        dtype = dict(df.dtypes)[col_name].lower()
        c = F.col(col_name)
        if "timestamp" in dtype:
            return c.cast("timestamp")
        else:
            numeric = c.cast("long")
            return (
                F.when(numeric.isNull(), None)
                .when(F.abs(numeric) >= 100_000_000_000_000, (numeric / 1_000_000.0).cast("timestamp"))
                .when(F.abs(numeric) >= 100_000_000_000, (numeric / 1_000.0).cast("timestamp"))
                .otherwise(numeric.cast("timestamp"))
            )

    open_time_expr = build_timestamp_expr(open_time_col)
    close_time_expr = build_timestamp_expr(close_time_col) if close_time_col else F.lit(None).cast("timestamp")

    return df.select(
        F.upper(F.trim(first_available(df, ["symbol"]).cast("string")))
            .alias("symbol"),

        first_available(df, ["interval", "kline_interval"], required=False)
            .cast("string").alias("interval"),

        open_time_expr.alias("open_time"),
        close_time_expr.alias("close_time"),

        first_available(df, ["open_price", "open"]).cast("decimal(30, 12)").alias("open"),
        first_available(df, ["high_price", "high"]).cast("decimal(30, 12)").alias("high"),
        first_available(df, ["low_price", "low"]).cast("decimal(30, 12)").alias("low"),
        first_available(df, ["close_price", "close"]).cast("decimal(30, 12)").alias("close"),

        first_available(df, ["volume", "base_volume", "base_asset_volume"])
            .cast("decimal(30, 12)").alias("base_volume"),

        first_available(df, ["quote_asset_volume", "quote_volume"], required=False)
            .cast("decimal(30, 12)").alias("quote_volume"),

        first_available(df, ["number_of_trades", "trade_count"],
                        required=False)
            .cast("long").alias("trade_count"),

        first_available(
            df,
            ["taker_buy_base_volume", "taker_buy_base_asset_volume"],
            required=False,
        ).cast("decimal(30, 12)").alias("taker_buy_base_volume"),

        first_available(
            df,
            ["taker_buy_quote_volume", "taker_buy_quote_asset_volume"],
            required=False,
        ).cast("decimal(30, 12)").alias("taker_buy_quote_volume"),

        F.lit(source_name).alias("source_system"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    ).withColumn(
        "interval",
        F.coalesce(F.col("interval"), F.lit("1m")),
    )


# Execute normalization and union
historical = normalize_klines(
    spark.table(HIST), "binance_historical"
)

rest = normalize_klines(
    spark.table(REST), "binance_rest"
)

combined = historical.unionByName(rest, allowMissingColumns=True)

# Data Quality Validation & Quarantine
combined = combined.withColumn(
    "rejection_reason",
    F.when(F.col("symbol").isNull(), "missing_symbol")
    .when(~F.col("symbol").isin("BTCUSDT", "ETHUSDT", "SOLUSDT"),
          "unsupported_symbol")
    .when(F.col("open_time").isNull(), "invalid_open_time")
    .when(F.col("open").isNull() | F.col("high").isNull()
          | F.col("low").isNull() | F.col("close").isNull(),
          "missing_ohlc")
    .when(
        (F.col("low") > F.col("high"))
        | (F.col("open") < F.col("low"))
        | (F.col("open") > F.col("high"))
        | (F.col("close") < F.col("low"))
        | (F.col("close") > F.col("high")),
        "invalid_ohlc_relationship",
    )
    .when(F.col("base_volume").isNull() | (F.col("base_volume") < 0),
          "invalid_base_volume")
)

rejected = combined.filter(F.col("rejection_reason").isNotNull())
valid = combined.filter(F.col("rejection_reason").isNull())

if rejected.limit(1).count() > 0:
    (
        rejected
        .withColumn("quarantined_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_KLINES)
    )

# Deduplication / Source Priority
source_priority = (
    F.when(F.col("source_system") == "binance_historical", 1)
    .when(F.col("source_system") == "binance_rest", 2)
    .otherwise(99)
)

valid = valid.withColumn("_priority", source_priority)

key_window = (
    Window
    .partitionBy("symbol", "interval", "open_time")
    .orderBy(
        F.col("_priority").asc(),
        F.col("ingested_at").desc_nulls_last(),
    )
)

valid = (
    valid
    .withColumn("_row_num", F.row_number().over(key_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_priority", "rejection_reason")
)

# Upsert/Write into Silver Table
if not spark.catalog.tableExists(SILVER_KLINES):
    (
        valid.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_KLINES)
    )
else:
    target = DeltaTable.forName(spark, SILVER_KLINES)

    (
        target.alias("t")
        .merge(
            valid.alias("s"),
            """
            t.symbol = s.symbol
            AND t.interval = s.interval
            AND t.open_time = s.open_time
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print("Silver klines completed")
print("Rejected records:", rejected.count())

# COMMAND ----------

combined = combined.withColumn(
    "rejection_reason",
    F.when(F.col("symbol").isNull(), "missing_symbol")
    .when(~F.col("symbol").isin("BTCUSDT", "ETHUSDT", "SOLUSDT"),
          "unsupported_symbol")
    .when(F.col("open_time").isNull(), "invalid_open_time")
    .when(F.col("open").isNull() | F.col("high").isNull()
          | F.col("low").isNull() | F.col("close").isNull(),
          "missing_ohlc")
    .when(
        (F.col("low") > F.col("high"))
        | (F.col("open") < F.col("low"))
        | (F.col("open") > F.col("high"))
        | (F.col("close") < F.col("low"))
        | (F.col("close") > F.col("high")),
        "invalid_ohlc_relationship",
    )
    .when(F.col("base_volume").isNull() | (F.col("base_volume") < 0),
          "invalid_base_volume")
)

rejected = combined.filter(F.col("rejection_reason").isNotNull())
valid = combined.filter(F.col("rejection_reason").isNull())


if rejected.limit(1).count() > 0:
    (
        rejected
        .withColumn("quarantined_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_KLINES)
    )


# Prefer the historical/official candle for overlapping source keys.
# REST is the fallback. Validate that this precedence matches your use case.
source_priority = (
    F.when(F.col("source_system") == "binance_historical", 1)
    .when(F.col("source_system") == "binance_rest", 2)
    .otherwise(99)
)

valid = valid.withColumn("_priority", source_priority)

key_window = (
    Window
    .partitionBy("symbol", "interval", "open_time")
    .orderBy(
        F.col("_priority").asc(),
        F.col("ingested_at").desc_nulls_last(),
    )
)

valid = (
    valid
    .withColumn("_row_num", F.row_number().over(key_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_priority", "rejection_reason")
)


if not spark.catalog.tableExists(SILVER_KLINES):
    (
        valid.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_KLINES)
    )
else:
    target = DeltaTable.forName(spark, SILVER_KLINES)

    (
        target.alias("t")
        .merge(
            valid.alias("s"),
            """
            t.symbol = s.symbol
            AND t.interval = s.interval
            AND t.open_time = s.open_time
            """,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


print("Silver klines completed")
print("Rejected records:", rejected.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trade duplicate check
# MAGIC SELECT symbol, trade_id, COUNT(*) AS records
# MAGIC FROM binance_data.silver.market_trades
# MAGIC GROUP BY symbol, trade_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Kline duplicate check
# MAGIC SELECT symbol, interval, open_time, COUNT(*) AS records
# MAGIC FROM binance_data.silver.market_klines
# MAGIC GROUP BY symbol, interval, open_time
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check symbol coverage
# MAGIC SELECT 'trades' AS dataset, symbol, COUNT(*) AS row_count
# MAGIC FROM binance_data.silver.market_trades
# MAGIC GROUP BY symbol
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'klines' AS dataset, symbol, COUNT(*) AS row_count
# MAGIC FROM binance_data.silver.market_klines
# MAGIC GROUP BY symbol;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check timestamp coverage
# MAGIC SELECT symbol, MIN(open_time) AS first_candle,
# MAGIC        MAX(open_time) AS latest_candle, COUNT(*) AS candle_count
# MAGIC FROM binance_data.silver.market_klines
# MAGIC WHERE interval = '1m'
# MAGIC GROUP BY symbol;