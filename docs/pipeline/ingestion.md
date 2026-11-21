# Ingestion

This page covers the three batch/stream ingestion mechanisms that write Parquet into the S3 landing zone. For the Databricks-side ingestion from S3 into Bronze, see [Bronze Layer](./bronze_layer.md).

## 1. Code map

| Concern | File |
|---|---|
| Historical download URLs | `src/historical/downloader.py` |
| Monthly/daily planning | `src/historical/historical_planner.py` |
| ZIP extraction | `src/historical/archive_extractor.py` |
| SHA-256 checksum | `src/historical/checksum.py` |
| CSV reader | `src/historical/file_reader.py` |
| Historical validation | `src/historical/historical_validator.py` |
| Column normalization | `src/historical/normalizer.py` |
| Parquet writers | `src/historical/parquet_writer.py`, `src/storage/parquet_writer.py` |
| End-to-end historical pipeline | `src/historical/historical_pipeline.py` |
| Batch entry point | `src/historical/run_historical_batch.py` |
| Single-archive entry point | `src/historical/run_historical_ingestion.py` |
| Binance REST client | `src/api/binance_rest.py` |
| REST pipeline | `src/pipeline/rest_pipeline.py` |
| REST response validation | `src/validation/validator.py` |
| S3 upload client | `src/storage/s3_client.py` |
| REST checkpoint | `src/checkpoint/checkpoint_manager.py` |
| Audit records | `src/audit/audit_logger.py`, `src/audit/historical_audit_logger.py` |

## 2. Historical ingestion

### Entry points

```powershell
# All ZIP archives under data/historical/<SYMBOL>/
python -m src.historical.run_historical_batch

# Single hard-coded archive (BTCUSDT-1m-2025-01.zip) — used for smoke tests
python -m src.historical.run_historical_ingestion
```

Configuration in `run_historical_batch.py`:

| Setting | Value |
|---|---|
| Local root | `data/historical` |
| Symbols | `BTCUSDT`, `ETHUSDT`, `SOLUSDT` |
| Interval | `1m` |
| `S3_BUCKET` | Required env var |
| `AWS_REGION` | Defaults to `ap-south-2` in this entry point |

### Archive planning

`create_download_plan(start_date, end_date)` in `historical_planner.py`:

- Caps `end_date` at yesterday — today's data is incomplete.
- Partial starting month → daily archives.
- Complete months → monthly archives.
- Remaining partial month → daily archives.

URL formats built by `HistoricalDownloader`:

```text
https://data.binance.vision/data/spot/monthly/klines/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM.zip
https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/1m/{SYMBOL}-1m-YYYY-MM-DD.zip
```

Downloads are cached locally; an existing file is not downloaded again.

### Processing steps

`HistoricalPipeline.process_archive()` performs:

1. Validate archive path and `.zip` extension.
2. Calculate SHA-256 of the source archive.
3. Extract ZIP (`archive_extractor.py`).
4. Find the single CSV file; warn if multiple.
5. Read CSV records.
6. Validate records against schema/value rules.
7. Normalize columns and types.
8. Convert to Snappy Parquet.
9. Build deterministic S3 key from the archive filename.
10. Check idempotency: if the object exists, skip upload and write a `SKIPPED_ALREADY_EXISTS` audit.
11. Upload Parquet and write a `SUCCESS` audit.
12. Remove the temporary extraction directory on success. On failure, extracted files are retained for debugging.

### Deterministic S3 key

```python
landing/historical/klines/symbol={symbol}/interval={interval}/year={year:04d}/month={month:02d}/{archive_stem}.parquet
```

Both monthly (`BTCUSDT-1m-2025-01`) and daily (`BTCUSDT-1m-2026-10-01`) archive names are supported. The same calendar month always maps to the same key, which is the basis for idempotency.

### Audit record

Written to `metadata/audit/historical/...` with fields including:

- `pipeline_name`, `pipeline_run_id`, `symbol`, `interval`
- `archive_name`, `checksum`, `records_read`, `records_written`
- `s3_key`, `status`, `start_time`

Statuses: `SUCCESS`, `SKIPPED_ALREADY_EXISTS`, plus failure handling by the batch runner.

## 3. REST ingestion

### Entry point

```powershell
python -m src.pipeline.rest_pipeline
```

The Airflow DAG `binance_end_to_end` runs this in `/opt/airflow/project` before triggering Bronze.

### Configuration

| Setting | Value / source |
|---|---|
| Base URL | `https://api.binance.com` (hard-coded in `rest_pipeline.py`) |
| Endpoint | `/api/v3/klines` |
| Symbols | `BTCUSDT`, `ETHUSDT`, `SOLUSDT` |
| Interval | `1m` |
| Limit | 100 candles per symbol |
| Bucket | `binance-market-data-lake` (hard-coded in `rest_pipeline.py`) |
| Region | `ap-south-2` (hard-coded in `rest_pipeline.py`) |

`config/config.yaml` contains the same base URL/endpoint/symbols but is not read by `rest_pipeline.py`; treat the Python module as the source of truth.

### Request behavior (`BinanceRESTClient`)

- Retries up to 3 attempts.
- Retries HTTP 429 and 5xx with `backoff_factor ** attempt` seconds (2, 4, ...).
- Retries `Timeout` and `ConnectionError`.
- Other HTTP errors raise immediately.
- Request timeout: 30 seconds.

### Validation (`validate_kline_response`)

- Response must be a non-empty list.
- Each candle must be a list of exactly 12 fields.
- `open_time` and `close_time` must be integers; `close_time >= open_time`.
- Open times must be strictly increasing.
- OHLC and volume fields must be numeric and non-negative.

### Output and side effects

| Artifact | Key pattern |
|---|---|
| Parquet | `landing/rest/klines/symbol={symbol}/interval=1m/date={YYYY-MM-DD}/klines_{HHMMSS}_{run_id}.parquet` |
| Checkpoint | `metadata/checkpoints/rest/klines/{symbol}_{interval}.json` |
| Success audit | `metadata/ingestion_audit/date={YYYY-MM-DD}/{symbol}_{HHMMSS}_{run_id}.json` |
| Failure audit | `.../{symbol}_{HHMMSS}_{run_id}_FAILED.json` |
| Quarantine | `quarantine/...` via `S3Client.upload_quarantine()` |

A single `pipeline_run_id` (UUID) is shared by all symbols in one run. Each symbol is processed independently: a failure for one symbol writes a failure audit and quarantine but does not stop the others. `CheckpointManager` currently writes checkpoints but does not change the request parameters — each run fetches the latest 100 candles.

## 4. Streaming ingestion (landing zone)

Entry points:

```powershell
# Terminal 1 — producer
python -m src.streaming.websocket_producer

# Terminal 2 — Spark consumer (or use docker compose)
docker compose up -d spark-streaming
```

| Component | Detail |
|---|---|
| WebSocket URL | `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade` |
| Kafka topic | `binance.trades.raw` |
| Producer key | Symbol |
| Producer value | JSON envelope with source fields (`e`, `s`, `t`, `p`, `q`, `T`, `E`, `m`) plus descriptive fields |
| Kafka settings | `acks=all`, `retries=10`, `max_in_flight_requests_per_connection=1`, 30s request timeout |
| Reconnect | Outer loop recreates `WebSocketApp` after disconnect with a 5-second delay |
| Spark job | `src/streaming/spark_to_s3_recovery.py` |
| Output | `landing/streaming/trades_recovery/`, partitioned by `symbol`, `event_date` |
| Checkpoint | `metadata/checkpoints/binance_trades_streaming_v2/` |

The Spark job parses Kafka JSON with an explicit `StructType`, filters invalid records, casts prices/quantities to `decimal(30,12)`, converts epoch millisecond fields to timestamps, and appends Parquet files with a 30-second trigger.

Full details, monitoring thresholds, and recovery procedures are in [Streaming Pipeline](./streaming.md) and [Streaming Recovery](../troubleshooting/streaming_recovery.md).

## 5. Environment variables

| Variable | Used by | Notes |
|---|---|---|
| `S3_BUCKET` | Historical batch, REST batch, Spark recovery, Airflow | Bucket name |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | All AWS clients | Inconsistent defaults; see AWS Setup |
| `KAFKA_BOOTSTRAP_SERVERS` | Producer, Spark | Host `localhost:9092`; Docker `kafka:29092` |
| `KAFKA_TOPIC` | Producer, Spark, Airflow | `binance.trades.raw` |
| `KAFKA_STARTING_OFFSETS` | Spark | `earliest` |
| `S3_OUTPUT_PATH` | Spark | Output prefix override |
| `S3_CHECKPOINT_PATH` | Spark | Checkpoint prefix override |
| `STREAMING_FRESHNESS_MINUTES` | Airflow monitor | Default 10 |
| `STREAMING_CHECKPOINT_FRESHNESS_MINUTES` | Airflow monitor | Default 6 |
| `KAFKA_MAX_LAG_MESSAGES` | Airflow monitor | Default 5000 |
| `MAX_ORPHAN_FILES` | Airflow monitor | Default 12 |

## 6. Verification checklist

After an ingestion run:

- [ ] Check the S3 prefix for new objects with the expected partition keys.
- [ ] Check audit JSON for `SUCCESS` / `SKIPPED_ALREADY_EXISTS` and record counts.
- [ ] Confirm no new `quarantine/` objects, or investigate any that appear.
- [ ] For streaming, confirm Spark checkpoint commits advance and `_spark_metadata` batches are continuous.
- [ ] Run the corresponding Databricks Bronze notebook and verify row counts in the raw tables.
