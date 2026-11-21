# Streaming Pipeline

Scope: Kafka → Spark → S3 trade ingestion and its Airflow monitoring.

```text
Binance WebSocket → websocket_producer.py → Kafka (binance.trades.raw)
                  → spark_to_s3_recovery.py → S3 landing/streaming/trades_recovery/
                  → Databricks Bronze → Silver → Gold
```

Active configuration (from `docker-compose.yml` and the recovery runbook):

| Item | Value |
|---|---|
| Kafka topic | `binance.trades.raw` |
| Kafka partitions | 3 |
| Spark output | `s3a://binance-market-data-lake/landing/streaming/trades_recovery/` |
| Spark checkpoint | `s3a://binance-market-data-lake/metadata/checkpoints/binance_trades_streaming_v2/` |
| File-sink metadata | `s3a://binance-market-data-lake/landing/streaming/trades_recovery/_spark_metadata/` |
| Monitor DAG | `binance_streaming_monitor`, every 5 minutes |
| Monitor source | `airflow/dags/binance_streaming_ingestion.py` |

## 1. WebSocket producer

File: `src/streaming/websocket_producer.py`

- Combined stream: `btcusdt@trade`, `ethusdt@trade`, `solusdt@trade`
- URL: `wss://stream.binance.com:9443/stream?streams=...`
- Required event fields: `e`, `s`, `t`, `p`, `q`, `T` (and `E`, `m` used in the record)
- Only events with `e == "trade"` are published
- Kafka key: symbol; Kafka value: compact JSON
- Delivery: `acks=all`, `retries=10`, `max_in_flight_requests_per_connection=1`, 30s request timeout; waits for broker acknowledgement before logging success
- Reconnect: if `run_forever()` returns, wait 5 seconds and recreate the socket
- Shutdown: `KeyboardInterrupt` → flush and close producer

Record fields:

```json
{
  "source": "binance_spot_websocket",
  "event_type": "trade",
  "symbol": "BTCUSDT",
  "trade_id": 123456,
  "price": "...",
  "quantity": "...",
  "trade_time_ms": 1735689600000,
  "event_time_ms": 1735689600001,
  "is_buyer_market_maker": true,
  "ingested_at": "2026-11-21T...",
  "raw_event": {}
}
```

## 2. Spark streaming jobs

Two variants exist:

| File | Status | Notes |
|---|---|---|
| `src/streaming/spark_to_s3_recovery.py` | **Active** — launched by `spark-streaming` service | Matches the producer schema; uses `S3_BUCKET` env var; sets S3A endpoint and default credential provider chain |
| `src/streaming/spark_to_s3.py` | Legacy/experimental | Uses `trades_recovery_v2/` and `binance_trades_streaming_v3/` defaults; supports `maxOffsetsPerTrigger` and configurable trigger |

### Active job behavior

1. Read Kafka with `startingOffsets` from env (default `earliest`) and `failOnDataLoss=true`.
2. Parse JSON with an explicit `StructType` matching the producer payload.
3. Filter rows where the parsed trade struct is null or required fields are missing.
4. Cast `price` and `quantity` to `decimal(30,12)`.
5. Convert `trade_time_ms` / `event_time_ms` from epoch milliseconds to timestamps.
6. Add `event_date = to_date(trade_timestamp)`.
7. Write Parquet with `outputMode("append")`, `partitionBy("symbol", "event_date")`, 30-second trigger.
8. Write checkpoint to S3 and rely on `_spark_metadata` for file-sink tracking.

### Spark packages

Both jobs use:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262
```

## 3. Operational commands

```powershell
# Start the whole stack
docker compose up -d

# Start only the streaming path
docker compose up -d kafka kafka-offset-exporter spark-streaming airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer

# Stop only Spark streaming (targeted shutdown)
docker compose stop spark-streaming

# Inspect S3 output
aws s3 ls s3://binance-market-data-lake/landing/streaming/trades_recovery/ --recursive

# Inspect checkpoint
aws s3 ls s3://binance-market-data-lake/metadata/checkpoints/binance_trades_streaming_v2/ --recursive

# Inspect file-sink metadata
aws s3 ls s3://binance-market-data-lake/landing/streaming/trades_recovery/_spark_metadata/ --recursive
```

Never kill unrelated Python processes to stop the stream: identify the producer process that owns the WebSocket connection.

## 4. Airflow monitoring

DAG: `binance_streaming_monitor` (`airflow/dags/binance_streaming_ingestion.py`)

| Task | Check | Default threshold |
|---|---|---|
| `check_streaming_data_in_s3` | Newest Parquet object age in the streaming prefix | 10 minutes (`STREAMING_FRESHNESS_MINUTES`) |
| `check_spark_checkpoint_and_file_sink` | Checkpoint freshness, commit/offset alignment, `_spark_metadata` batch coverage | 6 minutes (`STREAMING_CHECKPOINT_FRESHNESS_MINUTES`) |
| `check_orphan_files` | Parquet files present in S3 but absent from file-sink metadata | 12 files (`MAX_ORPHAN_FILES`) |
| `check_kafka_lag` | Latest Kafka offsets minus committed Spark offsets | 5000 messages (`KAFKA_MAX_LAG_MESSAGES`) |

### Check details

**S3 freshness** — lists all `.parquet` objects in the streaming prefix, finds the newest `LastModified`, and fails if older than the freshness threshold.

**Checkpoint/file-sink consistency** — verifies:

- Checkpoint has both `commits/` and `offsets/`
- Latest commit age is within threshold
- `commit <= offset <= commit + 1`
- `_spark_metadata` latest batch is not behind the latest commit
- Every batch from 0 through the latest commit is covered by a numeric metadata file or a `.compact` file

**Orphan inventory** — parses `_spark_metadata` JSON lines (skipping a leading `v1` line), collects tracked file paths, and compares them to actual `.parquet` objects:

- Missing tracked file → immediate failure
- More than `MAX_ORPHAN_FILES` orphans → failure
- Up to the limit is allowed, with a warning-style log

**Kafka lag** — reads `.run/kafka_latest.json` written every 30 seconds by `kafka-offset-exporter`, checks its freshness (3 minutes), selects the latest Spark commit at or before the snapshot time, reads committed offsets from that batch, and computes lag per partition. Partition set mismatch, negative lag, or lag above the limit fails the check.

## 5. Orphan and duplicate policy

Current known state from the recovery runbook: 12 orphan Parquet objects exist in the live prefix. They are exact duplicates of tracked rows (4,505 rows, 1,545 unique `(symbol, trade_id)` keys) and Silver dedupe absorbs them.

Rules:

1. Do not hard-delete orphans without a fresh checkpoint + `_spark_metadata` backup.
2. Move, do not delete: copy to `s3://binance-market-data-lake/quarantine/streaming_orphans/<timestamp>/` preserving relative paths.
3. Never move or delete tracked files or `_spark_metadata` objects during orphan cleanup.
4. Keep the quarantine copy at least 30 days or until the next successful Bronze/Silver validation.
5. Re-run the orphan inventory after cleanup and confirm zero untracked files.

Do not invent metadata for a missing batch. Keep `failOnDataLoss=true`.

## 6. Failure symptoms

| Symptom | Check that fails | First response |
|---|---|---|
| No new Parquet files | S3 freshness | Check producer and `spark-streaming` container logs; check Kafka topic has new messages |
| Commit age too high | Checkpoint freshness | Check whether Spark is alive; do not restore without matching metadata |
| `_spark_metadata` behind commit | Checkpoint/file-sink | Treat as checkpoint/file-sink desync; follow recovery runbook |
| Orphan count increased | Orphan inventory | List new keys before deciding; do not delete blindly |
| Kafka lag high | Kafka lag | Check consumer throughput, `maxOffsetsPerTrigger`, broker health |
| Negative Kafka lag | Kafka lag | Usually clock/snapshot skew; verify health file vs commit timestamps |

Detailed backup/restore procedures: [Streaming Recovery](../troubleshooting/streaming_recovery.md).
