# Streaming Checkpoint and Orphan Recovery Runbook

Scope: the Kafka to Spark to S3 recovery stream.

- Kafka topic: `binance.trades.raw`
- Spark checkpoint: `s3://binance-market-data-lake/metadata/checkpoints/binance_trades_streaming_v2/`
- Spark output: `s3://binance-market-data-lake/landing/streaming/trades_recovery/`
- File-sink metadata: `s3://binance-market-data-lake/landing/streaming/trades_recovery/_spark_metadata/`
- Source job: `src/streaming/spark_to_s3_recovery.py`
- Airflow monitor: `binance_streaming_monitor`

This runbook does not change the Databricks Bronze/Silver/Gold path. Bronze still reads
`landing/streaming/trades/` until the explicit Phase 6 path switch.

## 1. Current verified backup

A point-in-time backup was taken while the stream was live:

- Local root: `C:\Users\jades\AppData\Local\Temp\opencode\phase4_streaming_backup_20261101_131254`
- Checkpoint objects: 162
- File-sink metadata objects: 81
- Latest checkpoint commit: 79
- Latest checkpoint offset: 79
- Latest file-sink metadata batch: 80 (batch 79 is covered by `79.compact`)
- Missing file-sink batches: none
- Total backup size: 443,817 bytes
- Manifest: `<backup-root>\manifest.json` with S3 key, size, ETag, LastModified, and SHA-256

This backup contains checkpoint and file-sink metadata only. It does not contain the
Parquet output files. The output data remains in S3; it is not a full data backup.

The older `checkpoint_v2_backup_20261101_175351` backup is checkpoint-only and must
not be used alone for a restore.

S3 bucket versioning was not enabled at review time. Enable versioning or take a data
inventory/copy before any destructive cleanup or output-prefix change.

## 2. Stop the stream before a restore

Use targeted shutdown only:

1. Stop the producer process that owns the Binance WebSocket connection. Do not kill
   all Python processes.
2. Stop only the Spark streaming service:
   `docker compose stop spark-streaming`
3. Leave Kafka, Airflow, and the S3 data untouched unless the restore requires more.

Confirm no Spark process is still writing the checkpoint or `_spark_metadata`.

## 3. Backup procedure for future changes

Before a restore, path switch, or cleanup:

1. Stop writers or accept a documented point-in-time snapshot.
2. Copy every object under the checkpoint prefix, including:
   - `commits/`
   - `offsets/`
   - `sources/`
3. Copy every object under `_spark_metadata/`, including numeric batches and
   `*.compact` files.
4. Write a manifest with object keys, sizes, ETags, LastModified, and SHA-256.
5. Verify that every batch from `0` through the latest commit is covered by either
   a numeric metadata batch or a compact batch.
6. Verify that the latest commit and latest offset are a consistent pair. A latest
   offset one batch ahead of the latest commit is normal; a larger gap is not.

## 4. Restore procedure

### Option A: in-place restore to the same prefixes

Use this only when the output data after the backup can be accepted as duplicate
or orphaned and Silver dedupe will absorb it.

1. Stop the producer and Spark as described above.
2. Move the current checkpoint and `_spark_metadata` to a quarantine prefix. Do not
   hard-delete them.
3. Restore the backup checkpoint objects to the live checkpoint prefix.
4. Restore the backup `_spark_metadata` objects to the live output prefix.
5. Keep the checkpoint and metadata from the same backup snapshot.
6. Start Spark and confirm the checkpoint and file-sink monitor tasks pass.
7. Review output files written after the backup timestamp as potential duplicates
   or orphans. Do not assume they are safe without reviewing their keys.

### Option B: restore to a new output prefix

The `_spark_metadata` entries contain absolute output paths, so copying metadata
to a new prefix without rewriting those paths is not a valid restore.

Choose one of these supported approaches:

- Copy the tracked Parquet files to the new output prefix and rewrite every
  `_spark_metadata` path to the new prefix, then restore the matching checkpoint.
- Start a new stream with a new checkpoint and output prefix, using an explicit
  `startingOffsets` policy. Record whether that policy can replay or skip data.

Do not use half of one approach and half of the other.

### Hard rules

- Never restore a checkpoint without its matching `_spark_metadata`.
- Do not invent metadata for a missing batch. The empty batch 20 repair was valid
  only because that batch wrote no files and `commits/20` already existed.
- Keep `failOnDataLoss=true`. Do not silently skip missing offsets.
- Kafka retention is currently seven days. A restore that depends on replaying old
  offsets must complete before those offsets expire.
- After restore, run the monitor manually and confirm:
  - S3 freshness
  - checkpoint age
  - file-sink batch coverage
  - Kafka lag against the committed checkpoint

## 5. Orphan Parquet retention and cleanup policy

Current state: 12 orphan Parquet objects are exact duplicates of tracked rows:
4,505 rows, 1,545 unique `(symbol, trade_id)` keys, all present in tracked data,
with no payload conflicts. Three are older objects; nine came from the failed
batch-29 attempts.

Recommended policy:

1. Do not delete them now. They are safe to retain and Silver dedupe already treats
   the keys idempotently.
2. Keep them until the Phase 6 Bronze/Silver path switch and validation complete.
3. Before cleanup, take a fresh checkpoint plus `_spark_metadata` backup and an
   orphan object manifest.
4. Move, do not hard-delete: copy each orphan to a quarantine prefix such as
   `s3://binance-market-data-lake/quarantine/streaming_orphans/<timestamp>/`,
   preserving its relative path, then delete the source object.
5. Never move or delete tracked files or `_spark_metadata` objects as part of an
   orphan cleanup.
6. Verify the tracked file count and `_spark_metadata` are unchanged after the move.
7. Retain the quarantine copy for at least 30 days or until the next successful
   Bronze/Silver validation, whichever is later.
8. Re-run the orphan inventory after the move and confirm zero untracked files in
   the live output prefix.

The `binance_streaming_monitor` DAG now includes a `check_orphan_files` task.
It compares tracked file-sink paths with actual Parquet objects, fails if a
tracked file is missing, and fails if the orphan count exceeds
`MAX_ORPHAN_FILES` (currently 12). Raise the limit only after reviewing and
recording the new orphan files.

## 6. Silver dedupe contract

The Silver notebook uses:

- Window partition: `(symbol, trade_id)`
- Ordering: `ingested_at DESC`
- `row_number() == 1`
- Delta `MERGE` on `(symbol, trade_id)`
- `whenMatchedUpdateAll()` and `whenNotMatchedInsertAll()`

Consequences:

- Exact duplicate orphan rows cannot create duplicate Silver business rows.
- A duplicate can update `ingested_at` and `_source_file` to a later orphan path.
  It cannot change price, quantity, trade ID, or event timestamp if the payload is
  exact.
- If strict source-file lineage is required, keep the orphan files or update the
  Silver lineage policy before cleanup.
- Silver reads the full Bronze table on each run. The dedupe is defensive across
  full-table reprocessing, not only across one batch.

## 7. Known limitations

- Bronze still reads `landing/streaming/trades/`, not `trades_recovery/`.
- The new recovery data has therefore not reached Bronze/Silver/Gold yet.
- The Airflow monitor checks S3, the checkpoint/file-sink pair, and Kafka lag. It
  does not inspect the Spark container process directly; checkpoint age is the
  query-liveness signal.
- S3 bucket versioning is not enabled.
