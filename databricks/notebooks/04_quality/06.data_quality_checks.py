# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

def assert_no_rows(df, description):
    sample = df.limit(5).collect()

    if sample:
        raise ValueError(
            f"Quality check failed: {description}. "
            f"Example rows: {sample}"
        )


trades = spark.table("binance_data.silver.market_trades")
klines = spark.table("binance_data.silver.market_klines")
snapshot = spark.table("binance_data.gold_ai.asset_snapshot")

# COMMAND ----------

# 1. No duplicate trades
duplicate_trades = (
    trades
    .groupBy("symbol", "trade_id")
    .count()
    .filter(F.col("count") > 1)
)

assert_no_rows(duplicate_trades, "Duplicate trade keys")

# COMMAND ----------

# 2. No duplicate candles
duplicate_klines = (
    klines
    .groupBy("symbol", "interval", "open_time")
    .count()
    .filter(F.col("count") > 1)
)

assert_no_rows(duplicate_klines, "Duplicate kline keys")

# COMMAND ----------

# 3. No invalid prices or quantities
invalid_trades = trades.filter(
    F.col("price").isNull()
    | (F.col("price") <= 0)
    | F.col("quantity").isNull()
    | (F.col("quantity") <= 0)
)

assert_no_rows(invalid_trades, "Invalid Silver trade values")

# COMMAND ----------

# 4. Check that all three assets have snapshots
expected_symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}

actual_symbols = {
    row["symbol"]
    for row in snapshot.select("symbol").distinct().collect()
}

missing_symbols = expected_symbols - actual_symbols

if missing_symbols:
    raise ValueError(f"Missing AI snapshots: {missing_symbols}")


# COMMAND ----------

# 5. Snapshot freshness is monitored separately.
# Stale data may be a valid pipeline state, but must not be
# presented to users as current data.
display(
    snapshot.select(
        "symbol",
        "event_timestamp",
        "calculated_at",
        "data_age_seconds",
        "is_stale",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Dimension key uniqueness

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_date' AS table_name, COUNT(*) AS duplicate_keys
# MAGIC FROM (
# MAGIC     SELECT date_key
# MAGIC     FROM binance_data.gold_analytics.dim_date
# MAGIC     GROUP BY date_key
# MAGIC     HAVING date_key IS NULL OR COUNT(*) > 1
# MAGIC ) x
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_time', COUNT(*)
# MAGIC FROM (
# MAGIC     SELECT time_key
# MAGIC     FROM binance_data.gold_analytics.dim_time
# MAGIC     GROUP BY time_key
# MAGIC     HAVING time_key IS NULL OR COUNT(*) > 1
# MAGIC ) x
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_symbol', COUNT(*)
# MAGIC FROM (
# MAGIC     SELECT symbol_key
# MAGIC     FROM binance_data.gold_analytics.dim_symbol
# MAGIC     GROUP BY symbol_key
# MAGIC     HAVING symbol_key IS NULL OR COUNT(*) > 1
# MAGIC ) x
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_interval', COUNT(*)
# MAGIC FROM (
# MAGIC     SELECT interval_key
# MAGIC     FROM binance_data.gold_analytics.dim_interval
# MAGIC     GROUP BY interval_key
# MAGIC     HAVING interval_key IS NULL OR COUNT(*) > 1
# MAGIC ) x;

# COMMAND ----------

# MAGIC %md
# MAGIC Date coverage and key consistency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'fact_market_klines' AS fact_table,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS unmatched_date_keys,
# MAGIC     MIN(f.open_time) AS first_timestamp,
# MAGIC     MAX(f.open_time) AS last_timestamp
# MAGIC FROM binance_data.gold_analytics.fact_market_klines f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_date d
# MAGIC     ON f.date_key = d.date_key
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'fact_market_trades',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END),
# MAGIC     MIN(f.trade_timestamp),
# MAGIC     MAX(f.trade_timestamp)
# MAGIC FROM binance_data.gold_analytics.fact_market_trades f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_date d
# MAGIC     ON f.date_key = d.date_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'fact_market_klines' AS fact_table,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS unmatched_date_keys,
# MAGIC     MIN(f.open_time) AS first_timestamp,
# MAGIC     MAX(f.open_time) AS last_timestamp
# MAGIC FROM binance_data.gold_analytics.fact_market_klines f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_date d
# MAGIC     ON f.date_key = d.date_key
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'fact_market_trades',
# MAGIC     COUNT(*),
# MAGIC     SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END),
# MAGIC     MIN(f.trade_timestamp),
# MAGIC     MAX(f.trade_timestamp)
# MAGIC FROM binance_data.gold_analytics.fact_market_trades f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_date d
# MAGIC     ON f.date_key = d.date_key;

# COMMAND ----------

# MAGIC %md
# MAGIC Foreign keys for all applicable dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'klines_symbol' AS check_name,
# MAGIC     COUNT(*) AS unmatched_rows
# MAGIC FROM binance_data.gold_analytics.fact_market_klines f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_symbol d
# MAGIC     ON f.symbol_key = d.symbol_key
# MAGIC WHERE d.symbol_key IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'klines_interval',
# MAGIC     COUNT(*)
# MAGIC FROM binance_data.gold_analytics.fact_market_klines f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_interval d
# MAGIC     ON f.interval_key = d.interval_key
# MAGIC WHERE d.interval_key IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'klines_time',
# MAGIC     COUNT(*)
# MAGIC FROM binance_data.gold_analytics.fact_market_klines f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_time d
# MAGIC     ON f.time_key = d.time_key
# MAGIC WHERE d.time_key IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'trades_symbol',
# MAGIC     COUNT(*)
# MAGIC FROM binance_data.gold_analytics.fact_market_trades f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_symbol d
# MAGIC     ON f.symbol_key = d.symbol_key
# MAGIC WHERE d.symbol_key IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'trades_time',
# MAGIC     COUNT(*)
# MAGIC FROM binance_data.gold_analytics.fact_market_trades f
# MAGIC LEFT JOIN binance_data.gold_analytics.dim_time d
# MAGIC     ON f.time_key = d.time_key
# MAGIC WHERE d.time_key IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC OHLC and volume validity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN open_price <= 0 OR high_price <= 0
# MAGIC           OR low_price <= 0 OR close_price <= 0
# MAGIC           OR open_price IS NULL OR high_price IS NULL
# MAGIC           OR low_price IS NULL OR close_price IS NULL
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS invalid_prices,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN high_price < low_price
# MAGIC           OR high_price < open_price
# MAGIC           OR high_price < close_price
# MAGIC           OR low_price > open_price
# MAGIC           OR low_price > close_price
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS invalid_ohlc,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN base_volume < 0 OR quote_volume < 0
# MAGIC           OR trade_count < 0
# MAGIC           OR taker_buy_base_volume < 0
# MAGIC           OR taker_buy_quote_volume < 0
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS invalid_volumes
# MAGIC
# MAGIC FROM binance_data.gold_analytics.fact_market_klines;

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicate candles and trades

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Candle grain: one row per symbol, interval and opening time.
# MAGIC SELECT
# MAGIC     symbol_key,
# MAGIC     interval_key,
# MAGIC     open_time,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM binance_data.gold_analytics.fact_market_klines
# MAGIC GROUP BY symbol_key, interval_key, open_time
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY row_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trade IDs should be unique within a symbol.
# MAGIC SELECT
# MAGIC     symbol_key,
# MAGIC     trade_id,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM binance_data.gold_analytics.fact_market_trades
# MAGIC GROUP BY symbol_key, trade_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY row_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Date and time dimension consistency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT time_key) AS unique_time_keys,
# MAGIC     MIN(minute_of_day) AS min_minute,
# MAGIC     MAX(minute_of_day) AS max_minute,
# MAGIC     SUM(CASE
# MAGIC         WHEN minute_of_day < 0 OR minute_of_day > 1439
# MAGIC           OR hour <> FLOOR(minute_of_day / 60)
# MAGIC           OR minute <> MOD(minute_of_day, 60)
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS invalid_time_rows
# MAGIC FROM binance_data.gold_analytics.dim_time;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT calendar_date) AS unique_dates,
# MAGIC     MIN(calendar_date) AS first_date,
# MAGIC     MAX(calendar_date) AS last_date,
# MAGIC     SUM(CASE WHEN calendar_date IS NULL THEN 1 ELSE 0 END)
# MAGIC         AS null_dates
# MAGIC FROM binance_data.gold_analytics.dim_date;