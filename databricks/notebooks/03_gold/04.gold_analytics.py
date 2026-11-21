# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/03.silver_klines

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/02.silver_trades

# COMMAND ----------

SILVER_CATALOG = "binance_data"
GOLD_CATALOG = "binance_data"

SILVER_SCHEMA = f"{SILVER_CATALOG}.silver"
GOLD_SCHEMA = f"{GOLD_CATALOG}.gold_analytics"

SILVER_KLINES = f"{SILVER_SCHEMA}.market_klines"
SILVER_TRADES = f"{SILVER_SCHEMA}.market_trades"

DIM_DATE = f"{GOLD_SCHEMA}.dim_date"
DIM_TIME = f"{GOLD_SCHEMA}.dim_time"
DIM_SYMBOL = f"{GOLD_SCHEMA}.dim_symbol"
DIM_INTERVAL = f"{GOLD_SCHEMA}.dim_interval"

FACT_KLINES = f"{GOLD_SCHEMA}.fact_market_klines"
FACT_TRADES = f"{GOLD_SCHEMA}.fact_market_trades"

# All timestamps and date/time keys are standardized to UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

print("Silver source:", SILVER_SCHEMA)
print("Gold destination:", GOLD_SCHEMA)

# COMMAND ----------

# ============================================================
# 2. VERIFY SOURCE TABLES
# ============================================================

for table_name in [SILVER_KLINES, SILVER_TRADES]:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Required source table not found: {table_name}")

klines_raw = spark.table(SILVER_KLINES)
trades_raw = spark.table(SILVER_TRADES)

required_klines = {
    "symbol", "interval", "open_time", "close_time",
    "open", "high", "low", "close",
    "base_volume", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume",
    "ingested_at", "source_file"
}

required_trades = {
    "symbol", "trade_id", "trade_timestamp",
    "price", "quantity", "quote_value",
    "is_buyer_market_maker", "ingested_at", "source_file"
}

missing_klines = required_klines - set(klines_raw.columns)
missing_trades = required_trades - set(trades_raw.columns)

if missing_klines:
    raise ValueError(f"Missing Klines columns: {sorted(missing_klines)}")

if missing_trades:
    raise ValueError(f"Missing Trades columns: {sorted(missing_trades)}")


# COMMAND ----------

# ============================================================
# 3. STANDARDIZE AND DEDUPLICATE SILVER DATA
# ============================================================

def deduplicate_latest(df, business_keys):
    """
    Keep one row per business key.
    Prefer latest ingestion, then source file, then payload hash.
    """
    payload_columns = sorted(
        c for c in df.columns
        if c not in {"ingested_at", "source_file"}
    )

    payload_json = F.to_json(
        F.struct(*[F.col(c) for c in payload_columns])
    )

    prepared = df.withColumn(
        "_payload_hash",
        F.sha2(payload_json, 256)
    )

    window = (
        Window.partitionBy(*business_keys)
        .orderBy(
            F.col("ingested_at").desc_nulls_last(),
            F.col("source_file").desc_nulls_last(),
            F.col("_payload_hash").desc()
        )
    )

    return (
        prepared
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "_payload_hash")
    )


klines = (
    klines_raw
    .withColumn("open_time", F.to_timestamp("open_time"))
    .withColumn("close_time", F.to_timestamp("close_time"))
    .filter(
        F.col("symbol").isNotNull()
        & F.col("interval").isNotNull()
        & F.col("open_time").isNotNull()
    )
)

klines = deduplicate_latest(
    klines,
    ["symbol", "interval", "open_time"]
)

trades = (
    trades_raw
    .withColumn("trade_timestamp", F.to_timestamp("trade_timestamp"))
    .withColumn("trade_id", F.col("trade_id").cast("string"))
    .filter(
        F.col("symbol").isNotNull()
        & F.col("trade_id").isNotNull()
        & F.col("trade_timestamp").isNotNull()
    )
)

trades = deduplicate_latest(
    trades,
    ["symbol", "trade_id"]
)

print("Silver Klines after deduplication:", klines.count())
print("Silver Trades after deduplication:", trades.count())

# COMMAND ----------

# ============================================================
# 4. BUILD DIM_DATE
# Grain: one row per calendar date
# ============================================================

all_dates = (
    klines.select(F.to_date("open_time").alias("calendar_date"))
    .unionByName(
        trades.select(F.to_date("trade_timestamp").alias("calendar_date"))
    )
    .filter(F.col("calendar_date").isNotNull())
)

date_bounds = all_dates.agg(
    F.min("calendar_date").alias("min_date"),
    F.max("calendar_date").alias("max_date")
).first()

if date_bounds["min_date"] is None:
    raise ValueError("No valid dates exist in either Silver table.")

date_values = spark.range(1).select(
    F.explode(
        F.sequence(
            F.lit(date_bounds["min_date"]),
            F.lit(date_bounds["max_date"]),
            F.expr("INTERVAL 1 DAY")
        )
    ).alias("calendar_date")
)

dim_date = (
    date_values
    .withColumn(
        "date_key",
        F.date_format("calendar_date", "yyyyMMdd").cast("int")
    )
    .withColumn("year", F.year("calendar_date"))
    .withColumn("quarter", F.quarter("calendar_date"))
    .withColumn("month_number", F.month("calendar_date"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("year_month", F.date_format("calendar_date", "yyyy-MM"))
    .withColumn("day_of_month", F.dayofmonth("calendar_date"))
    .withColumn("day_of_week_number", F.dayofweek("calendar_date"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn(
        "is_weekend",
        F.dayofweek("calendar_date").isin([1, 7])
    )
    .select(
        "date_key", "calendar_date", "year", "quarter",
        "month_number", "month_name", "year_month",
        "day_of_month", "day_of_week_number",
        "day_name", "is_weekend"
    )
)

# COMMAND ----------

# ============================================================
# 5. BUILD DIM_TIME
# Grain: one row per minute of day, in UTC
# ============================================================

dim_time = (
    spark.range(1440)
    .withColumnRenamed("id", "minute_of_day")
    .withColumn(
        "hour",
        F.floor(F.col("minute_of_day") / 60).cast("int")
    )
    .withColumn(
        "minute",
        F.pmod(F.col("minute_of_day"), F.lit(60)).cast("int")
    )
    .withColumn("time_key", F.col("minute_of_day").cast("int"))
    .withColumn(
        "time_label",
        F.format_string("%02d:%02d", F.col("hour"), F.col("minute"))
    )
    .withColumn(
        "day_part",
        F.when(F.col("hour") < 6, "Night")
         .when(F.col("hour") < 12, "Morning")
         .when(F.col("hour") < 17, "Afternoon")
         .when(F.col("hour") < 21, "Evening")
         .otherwise("Night")
    )
    .select(
        "time_key", "minute_of_day", "hour",
        "minute", "time_label", "day_part"
    )
)


# COMMAND ----------

# ============================================================
# 6. BUILD DIM_SYMBOL
# Grain: one row per unique trading pair
# ============================================================

symbols = (
    klines.select("symbol")
    .unionByName(trades.select("symbol"))
    .distinct()
)

# Recognize common quote assets, longest suffix first.
quote_assets = [
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD",
    "BTC", "ETH", "BNB", "EUR", "TRY", "BRL",
    "GBP", "AUD", "USDP", "DAI"
]

quote_expr = F.lit(None).cast("string")

for quote in sorted(quote_assets, key=len, reverse=True):
    quote_expr = F.when(
        F.col("symbol").endswith(quote),
        F.lit(quote)
    ).otherwise(quote_expr)

dim_symbol = (
    symbols
    .withColumn("quote_asset", quote_expr)
    .withColumn(
        "base_asset",
        F.when(
            F.col("quote_asset").isNotNull(),
            F.expr(
                "substring(symbol, 1, "
                "length(symbol) - length(quote_asset))"
            )
        ).otherwise(F.lit(None).cast("string"))
    )
    # Deterministic key: stable when new symbols are added.
    .withColumn("symbol_key", F.xxhash64("symbol"))
    .select("symbol_key", "symbol", "base_asset", "quote_asset")
)

# COMMAND ----------

# ============================================================
# 7. BUILD DIM_INTERVAL
# Grain: one row per unique candle interval
# ============================================================

intervals = klines.select("interval").distinct()

dim_interval = (
    intervals
    .withColumn("interval_key", F.xxhash64("interval"))
    .withColumn(
        "interval_minutes",
        F.when(
            F.col("interval").rlike(r"^\d+m$"),
            F.regexp_extract("interval", r"^(\d+)m$", 1).cast("int")
        )
        .when(
            F.col("interval").rlike(r"^\d+h$"),
            F.regexp_extract("interval", r"^(\d+)h$", 1).cast("int") * 60
        )
        .when(
            F.col("interval").rlike(r"^\d+d$"),
            F.regexp_extract("interval", r"^(\d+)d$", 1).cast("int") * 1440
        )
        .when(
            F.col("interval").rlike(r"^\d+w$"),
            F.regexp_extract("interval", r"^(\d+)w$", 1).cast("int") * 10080
        )
        .otherwise(F.lit(None).cast("int"))
    )
    .select("interval_key", "interval", "interval_minutes")
)


# COMMAND ----------

# ============================================================
# 8. VALIDATE DIMENSION KEYS
# ============================================================

def fail_if_duplicate(df, key, description):
    duplicate_exists = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_exists:
        raise ValueError(
            f"Duplicate {description} key detected: {key}"
        )


fail_if_duplicate(dim_date, "date_key", "date")
fail_if_duplicate(dim_time, "time_key", "time")
fail_if_duplicate(dim_symbol, "symbol_key", "symbol")
fail_if_duplicate(dim_interval, "interval_key", "interval")

if dim_symbol.filter(F.col("quote_asset").isNull()).limit(1).count():
    print(
        "WARNING: Some quote assets were not recognized. "
        "Review dim_symbol and extend quote_assets if necessary."
    )


# COMMAND ----------

# ============================================================
# 9. BUILD FACT_MARKET_KLINES
# Grain: one row per symbol + interval + open_time
# ============================================================

k = (
    klines
    .withColumn(
        "date_key",
        F.date_format("open_time", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "time_key",
        (F.hour("open_time") * 60 + F.minute("open_time")).cast("int")
    )
    .join(
        dim_symbol.select("symbol", "symbol_key"),
        on="symbol",
        how="inner"
    )
    .join(
        dim_interval.select("interval", "interval_key"),
        on="interval",
        how="inner"
    )
)

fact_market_klines = k.select(
    "symbol_key",
    "interval_key",
    "date_key",
    "time_key",
    "open_time",
    "close_time",
    F.col("open").cast("decimal(30,12)").alias("open_price"),
    F.col("high").cast("decimal(30,12)").alias("high_price"),
    F.col("low").cast("decimal(30,12)").alias("low_price"),
    F.col("close").cast("decimal(30,12)").alias("close_price"),
    F.col("base_volume").cast("decimal(30,12)").alias("base_volume"),
    F.col("quote_volume").cast("decimal(30,12)").alias("quote_volume"),
    F.col("trade_count").cast("long").alias("trade_count"),
    F.col("taker_buy_base_volume")
        .cast("decimal(30,12)").alias("taker_buy_base_volume"),
    F.col("taker_buy_quote_volume")
        .cast("decimal(30,12)").alias("taker_buy_quote_volume")
)


# COMMAND ----------

# ============================================================
# 10. BUILD FACT_MARKET_TRADES
# Grain: one row per symbol + trade_id
# ============================================================

t = (
    trades
    .withColumn(
        "date_key",
        F.date_format("trade_timestamp", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "time_key",
        (F.hour("trade_timestamp") * 60
         + F.minute("trade_timestamp")).cast("int")
    )
    .join(
        dim_symbol.select("symbol", "symbol_key"),
        on="symbol",
        how="inner"
    )
)

fact_market_trades = t.select(
    "symbol_key",
    "date_key",
    "time_key",
    F.col("trade_id").cast("string").alias("trade_id"),
    "trade_timestamp",
    F.col("price").cast("decimal(30,12)").alias("trade_price"),
    F.col("quantity").cast("decimal(30,12)").alias("quantity"),
    # Preserve the Silver source's decimal(38,6) precision.
    F.col("quote_value").cast("decimal(38,6)").alias("quote_value"),
    F.col("is_buyer_market_maker").cast("boolean")
        .alias("is_buyer_market_maker"),
    F.lit(1).cast("long").alias("trade_count"),
    F.when(
        F.col("is_buyer_market_maker") == F.lit(False),
        F.lit("Buyer taker")
    )
    .when(
        F.col("is_buyer_market_maker") == F.lit(True),
        F.lit("Seller taker")
    )
    .otherwise(F.lit("Unknown"))
    .alias("taker_side")
)


# COMMAND ----------

# ============================================================
# 11. VALIDATE FACT GRAINS BEFORE WRITING
# ============================================================

def fail_if_empty(df, description):
    if df.limit(1).count() == 0:
        raise ValueError(f"{description} contains no rows.")


fail_if_empty(dim_date, "dim_date")
fail_if_empty(dim_time, "dim_time")
fail_if_empty(dim_symbol, "dim_symbol")
fail_if_empty(dim_interval, "dim_interval")
fail_if_empty(fact_market_klines, "fact_market_klines")
fail_if_empty(fact_market_trades, "fact_market_trades")

duplicate_candles = (
    fact_market_klines
    .groupBy("symbol_key", "interval_key", "open_time")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_candles:
    raise ValueError("Duplicate candle grain detected.")

duplicate_trades = (
    fact_market_trades
    .groupBy("symbol_key", "trade_id")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_trades:
    raise ValueError("Duplicate trade grain detected.")


# COMMAND ----------

# ============================================================
# 12. WRITE GOLD TABLES
# FULL REFRESH: overwrites existing Gold tables.
# ============================================================

gold_tables = [
    (dim_date, DIM_DATE),
    (dim_time, DIM_TIME),
    (dim_symbol, DIM_SYMBOL),
    (dim_interval, DIM_INTERVAL),
    (fact_market_klines, FACT_KLINES),
    (fact_market_trades, FACT_TRADES),
]

for dataframe, table_name in gold_tables:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Created: {table_name}")


# COMMAND ----------

# ============================================================
# 13. POST-WRITE VALIDATION
# ============================================================

print("\nGOLD TABLE ROW COUNTS")

for _, table_name in gold_tables:
    print(f"\n{table_name}")
    spark.table(table_name).select(
        F.count("*").alias("row_count")
    ).show()


print("\nGOLD TABLES CREATED:")
spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}").show(truncate=False)

print("\nGold star schema build completed.")
