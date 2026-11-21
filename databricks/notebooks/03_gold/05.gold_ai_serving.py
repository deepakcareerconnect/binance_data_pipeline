# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/04.gold_analytics

# COMMAND ----------

# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = "binance_data"
GOLD_SCHEMA = f"{CATALOG}.gold_analytics"
AI_SCHEMA = f"{CATALOG}.gold_ai"

FACT_KLINES = f"{GOLD_SCHEMA}.fact_market_klines"
FACT_TRADES = f"{GOLD_SCHEMA}.fact_market_trades"

DIM_DATE = f"{GOLD_SCHEMA}.dim_date"
DIM_TIME = f"{GOLD_SCHEMA}.dim_time"
DIM_SYMBOL = f"{GOLD_SCHEMA}.dim_symbol"
DIM_INTERVAL = f"{GOLD_SCHEMA}.dim_interval"

MARKET_1M = f"{AI_SCHEMA}.market_1m"
MARKET_INDICATORS = f"{AI_SCHEMA}.market_indicators"
MARKET_ACTIVITY = f"{AI_SCHEMA}.market_activity"
ASSET_SNAPSHOT = f"{AI_SCHEMA}.asset_snapshot"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AI_SCHEMA}")

# ============================================================
# 2. LOAD DIMENSIONS AND FACT TABLES
# ============================================================

klines = spark.table(FACT_KLINES)
trades = spark.table(FACT_TRADES)

dim_symbol = spark.table(DIM_SYMBOL)
dim_date = spark.table(DIM_DATE)
dim_time = spark.table(DIM_TIME)
dim_interval = spark.table(DIM_INTERVAL)

# ============================================================
# 3. BUILD ENRICHED MARKET CANDLES
# ============================================================

k = klines.alias("k")
s = dim_symbol.alias("s")
d = dim_date.alias("d")
t = dim_time.alias("t")
i = dim_interval.alias("i")

market = (
    k.join(
        s, F.col("k.symbol_key") == F.col("s.symbol_key"), "left"
    )
    .join(
        d, F.col("k.date_key") == F.col("d.date_key"), "left"
    )
    .join(
        t, F.col("k.time_key") == F.col("t.time_key"), "left"
    )
    .join(
        i, F.col("k.interval_key") == F.col("i.interval_key"), "left"
    )
    .select(
        F.col("s.symbol").alias("symbol"),
        F.col("s.base_asset").alias("base_asset"),
        F.col("s.quote_asset").alias("quote_asset"),
        F.col("i.interval").alias("interval"),
        F.col("i.interval_minutes").alias("interval_minutes"),
        F.col("k.symbol_key"),
        F.col("k.interval_key"),
        F.col("k.date_key"),
        F.col("k.time_key"),
        F.to_timestamp("k.open_time").alias("open_time"),
        F.to_timestamp("k.close_time").alias("close_time"),
        F.col("d.calendar_date").alias("calendar_date"),
        F.col("d.year").alias("year"),
        F.col("d.quarter").alias("quarter"),
        F.col("d.month_number").alias("month_number"),
        F.col("d.month_name").alias("month_name"),
        F.col("d.year_month").alias("year_month"),
        F.col("d.day_name").alias("day_name"),
        F.col("d.is_weekend").alias("is_weekend"),
        F.col("t.hour").alias("hour"),
        F.col("t.minute").alias("minute_of_hour"),
        F.col("t.time_label").alias("time_label"),
        F.col("t.day_part").alias("day_part"),
        F.col("k.open_price").cast("double").alias("open_price"),
        F.col("k.high_price").cast("double").alias("high_price"),
        F.col("k.low_price").cast("double").alias("low_price"),
        F.col("k.close_price").cast("double").alias("close_price"),
        F.col("k.base_volume").cast("double").alias("base_volume"),
        F.col("k.quote_volume").cast("double").alias("quote_volume"),
        F.col("k.trade_count").cast("long").alias("trade_count"),
        F.col("k.taker_buy_base_volume")
            .cast("double").alias("taker_buy_base_volume"),
        F.col("k.taker_buy_quote_volume")
            .cast("double").alias("taker_buy_quote_volume"),
    )
    .withColumn("minute", F.date_trunc("minute", "open_time"))
)

# Reject records that cannot be used as market candles.
market = market.filter(
    F.col("symbol").isNotNull()
    & F.col("minute").isNotNull()
    & F.col("close_price").isNotNull()
)

# Idempotent output: one candle per symbol and interval timestamp.
# If duplicates exist, investigate them before relying on these metrics.
market = market.dropDuplicates(["symbol", "interval", "minute"])

market.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(MARKET_1M)

print("Created:", MARKET_1M)

# COMMAND ----------

# ============================================================
# 4. CALCULATE TECHNICAL INDICATORS
# ============================================================

market = spark.table(MARKET_1M)

w = Window.partitionBy("symbol", "interval").orderBy("minute")

# Row-based windows are candle counts, not guaranteed elapsed time.
w_sma20 = w.rowsBetween(-19, 0)
w_sma50 = w.rowsBetween(-49, 0)

market = (
    market
    .withColumn("previous_close_1", F.lag("close_price", 1).over(w))
    .withColumn("previous_close_5", F.lag("close_price", 5).over(w))
    .withColumn("previous_close_60", F.lag("close_price", 60).over(w))
    .withColumn("previous_close_1440", F.lag("close_price", 1440).over(w))
    .withColumn("previous_minute_1", F.lag("minute", 1).over(w))
    .withColumn("previous_minute_5", F.lag("minute", 5).over(w))
    .withColumn("previous_minute_60", F.lag("minute", 60).over(w))
    .withColumn("previous_minute_1440", F.lag("minute", 1440).over(w))
    .withColumn(
        "return_1m",
        F.when(
            (F.col("minute").cast("long")
             - F.col("previous_minute_1").cast("long")) == 60,
            F.col("close_price") / F.col("previous_close_1") - 1
        )
    )
    .withColumn(
        "return_5m",
        F.when(
            (F.col("minute").cast("long")
             - F.col("previous_minute_5").cast("long")) == 300,
            F.col("close_price") / F.col("previous_close_5") - 1
        )
    )
    .withColumn(
        "return_1h",
        F.when(
            (F.col("minute").cast("long")
             - F.col("previous_minute_60").cast("long")) == 3600,
            F.col("close_price") / F.col("previous_close_60") - 1
        )
    )
    .withColumn(
        "return_24h",
        F.when(
            (F.col("minute").cast("long")
             - F.col("previous_minute_1440").cast("long")) == 86400,
            F.col("close_price") / F.col("previous_close_1440") - 1
        )
    )
    .withColumn("sma_20", F.avg("close_price").over(w_sma20))
    .withColumn("sma_50", F.avg("close_price").over(w_sma50))
    .withColumn(
        "volatility_1h",
        F.stddev_samp("return_1m").over(
            Window.partitionBy("symbol", "interval")
            .orderBy(F.col("minute").cast("long"))
            .rangeBetween(-3600, 0)
        )
    )
    .withColumn(
        "calculated_at", F.current_timestamp()
    )
)

# Avoid exposing internal calculation columns.
indicator_output = market.select(
    "symbol", "interval", "minute", "calendar_date",
    "close_price", "quote_volume", "trade_count",
    "return_1m", "return_5m", "return_1h", "return_24h",
    "sma_20", "sma_50", "volatility_1h",
    "calculated_at"
)

indicator_output.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(MARKET_INDICATORS)

print("Created:", MARKET_INDICATORS)


# COMMAND ----------

# ============================================================
# 5. BUILD TRADE ACTIVITY BY SYMBOL AND MINUTE
# ============================================================

trade_base = (
    trades
    .join(
        dim_symbol.select("symbol_key", "symbol"),
        on="symbol_key",
        how="left"
    )
    .withColumn(
        "trade_timestamp",
        F.to_timestamp("trade_timestamp")
    )
    .withColumn(
        "minute",
        F.date_trunc("minute", "trade_timestamp")
    )
    .withColumn("quote_value", F.col("quote_value").cast("double"))
    .filter(
        F.col("symbol").isNotNull()
        & F.col("minute").isNotNull()
    )
)

activity = (
    trade_base
    .groupBy("symbol", "minute")
    .agg(
        F.count("*").alias("total_trades"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("quote_value").alias("total_quote_volume"),
        F.avg("quote_value").alias("avg_trade_value"),
        F.max("quote_value").alias("largest_trade_value"),
        F.sum(
            F.when(
                F.col("is_buyer_market_maker") == F.lit(False), 1
            ).otherwise(0)
        ).alias("buyer_taker_count"),
        F.sum(
            F.when(
                F.col("is_buyer_market_maker") == F.lit(True), 1
            ).otherwise(0)
        ).alias("seller_taker_count"),
        F.sum(
            F.when(
                F.col("is_buyer_market_maker") == F.lit(False),
                F.col("quote_value")
            ).otherwise(0.0)
        ).alias("buyer_taker_quote_volume"),
        F.sum(
            F.when(
                F.col("is_buyer_market_maker") == F.lit(True),
                F.col("quote_value")
            ).otherwise(0.0)
        ).alias("seller_taker_quote_volume"),
    )
    .withColumn("calendar_date", F.to_date("minute"))
    .withColumn("calculated_at", F.current_timestamp())
)

activity.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(MARKET_ACTIVITY)

print("Created:", MARKET_ACTIVITY)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM binance_data.gold_ai.asset_snapshot
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM binance_data.gold_ai.market_relationships
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM binance_data.gold_ai.market_alerts
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM binance_data.gold_ai.data_dictionary
# MAGIC limit 10;