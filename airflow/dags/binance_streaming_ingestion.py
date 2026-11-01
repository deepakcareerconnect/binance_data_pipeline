from datetime import datetime, timedelta, timezone
import json
import logging
import os

import boto3

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# 1. CONFIGURATION
# ============================================================

S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "binance-market-data-lake"
)

S3_STREAMING_PREFIX = (
    os.getenv(
        "S3_STREAMING_PREFIX",
        "landing/streaming/trades_recovery/"
    ).rstrip("/")
    + "/"
)

S3_CHECKPOINT_PREFIX = (
    os.getenv(
        "S3_CHECKPOINT_PREFIX",
        "metadata/checkpoints/binance_trades_streaming_v2/"
    ).rstrip("/")
    + "/"
)

FRESHNESS_MINUTES = int(
    os.getenv("STREAMING_FRESHNESS_MINUTES", "10")
)

CHECKPOINT_FRESHNESS_MINUTES = int(
    os.getenv("STREAMING_CHECKPOINT_FRESHNESS_MINUTES", "6")
)

AWS_REGION = os.getenv(
    "AWS_DEFAULT_REGION",
    "ap-south-1"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "binance.trades.raw"
)

KAFKA_HEALTH_FILE = os.getenv(
    "KAFKA_HEALTH_FILE",
    "/opt/airflow/project/.run/kafka_latest.json"
)

KAFKA_HEALTH_FRESHNESS_MINUTES = int(
    os.getenv("KAFKA_HEALTH_FRESHNESS_MINUTES", "3")
)

KAFKA_MAX_LAG_MESSAGES = int(
    os.getenv("KAFKA_MAX_LAG_MESSAGES", "5000")
)

MAX_ORPHAN_FILES = int(
    os.getenv("MAX_ORPHAN_FILES", "12")
)


# ============================================================
# 2. HELPERS
# ============================================================


def _s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )


def _list_objects(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix
    ):
        objects.extend(page.get("Contents", []))

    return objects


def _batch_id(key):
    name = key.rsplit("/", 1)[-1]

    if name.isdigit():
        return int(name)

    if name.endswith(".compact"):
        stem = name[: -len(".compact")]

        if stem.isdigit():
            return int(stem)

    return None


def _age_minutes(modified):
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)

    return (
        datetime.now(timezone.utc) - modified
    ).total_seconds() / 60


def _s3_key_from_uri(uri):
    if not uri:
        return None

    for scheme in ("s3a://", "s3://", "s3n://"):
        if uri.startswith(scheme):
            uri = uri[len(scheme):]
            break
    else:
        return None

    bucket, separator, key = uri.partition("/")

    if separator and bucket == S3_BUCKET:
        return key

    return None


# ============================================================
# 3. CHECK S3 STREAMING DATA
# ============================================================


def check_streaming_data():

    logging.info("Checking Binance streaming data in S3")

    s3 = _s3_client()
    latest_object = None
    total_objects = 0

    for obj in _list_objects(s3, S3_STREAMING_PREFIX):

        key = obj["Key"]

        # Only actual Parquet data files
        if not key.lower().endswith(".parquet"):
            continue

        total_objects += 1

        if (
            latest_object is None
            or obj["LastModified"] > latest_object["LastModified"]
        ):
            latest_object = obj

    if latest_object is None:
        raise ValueError(
            "No Parquet streaming files found in S3."
        )

    latest_modified = latest_object["LastModified"]
    age_minutes = _age_minutes(latest_modified)

    logging.info("Total Parquet files: %s", total_objects)
    logging.info("Latest Parquet file: %s", latest_object["Key"])
    logging.info("Last modified: %s", latest_modified)
    logging.info("File age: %.2f minutes", age_minutes)

    if age_minutes > FRESHNESS_MINUTES:
        raise ValueError(
            f"Streaming data is stale: {age_minutes:.2f} minutes"
        )

    logging.info(
        "SUCCESS: Binance streaming data is arriving in S3."
    )


# ============================================================
# 4. CHECK SPARK CHECKPOINT AND FILE-SINK CONSISTENCY
# ============================================================


def check_spark_checkpoint_and_file_sink():

    logging.info("Checking Spark checkpoint and file-sink metadata")

    s3 = _s3_client()

    commit_objects = [
        (_batch_id(obj["Key"]), obj)
        for obj in _list_objects(
            s3,
            f"{S3_CHECKPOINT_PREFIX}commits/"
        )
        if _batch_id(obj["Key"]) is not None
    ]

    offset_objects = [
        (_batch_id(obj["Key"]), obj)
        for obj in _list_objects(
            s3,
            f"{S3_CHECKPOINT_PREFIX}offsets/"
        )
        if _batch_id(obj["Key"]) is not None
    ]

    if not commit_objects or not offset_objects:
        raise ValueError(
            "Spark checkpoint is missing commits or offsets."
        )

    latest_commit_id, latest_commit = max(
        commit_objects,
        key=lambda item: item[0]
    )
    latest_offset_id, _ = max(
        offset_objects,
        key=lambda item: item[0]
    )

    commit_age = _age_minutes(latest_commit["LastModified"])

    logging.info("Latest committed batch: %s", latest_commit_id)
    logging.info("Latest offset batch: %s", latest_offset_id)
    logging.info("Latest commit age: %.2f minutes", commit_age)

    if commit_age > CHECKPOINT_FRESHNESS_MINUTES:
        raise ValueError(
            "Spark checkpoint is stale: "
            f"{commit_age:.2f} minutes since the last commit"
        )

    if latest_commit_id > latest_offset_id:
        raise ValueError(
            "Spark offset checkpoint is behind the commit checkpoint: "
            f"commit={latest_commit_id}, offset={latest_offset_id}"
        )

    if latest_offset_id > latest_commit_id + 1:
        raise ValueError(
            "Spark checkpoint has more than one uncommitted batch: "
            f"commit={latest_commit_id}, offset={latest_offset_id}"
        )

    metadata_objects = _list_objects(
        s3,
        f"{S3_STREAMING_PREFIX}_spark_metadata/"
    )

    numeric_batches = set()
    compact_batches = set()

    for obj in metadata_objects:
        name = obj["Key"].rsplit("/", 1)[-1]

        if name.isdigit():
            numeric_batches.add(int(name))
        elif (
            name.endswith(".compact")
            and name[: -len(".compact")].isdigit()
        ):
            compact_batches.add(
                int(name[: -len(".compact")])
            )

    if not numeric_batches and not compact_batches:
        raise ValueError(
            "Spark file-sink metadata is missing."
        )

    latest_metadata_batch = max(
        numeric_batches | compact_batches
    )

    logging.info(
        "Latest file-sink metadata batch: %s",
        latest_metadata_batch
    )

    if latest_metadata_batch < latest_commit_id:
        raise ValueError(
            "Spark file-sink metadata is behind the checkpoint: "
            f"metadata={latest_metadata_batch}, "
            f"commit={latest_commit_id}"
        )

    missing_batches = [
        batch_id
        for batch_id in range(latest_commit_id + 1)
        if (
            batch_id not in numeric_batches
            and not any(
                compact_batch >= batch_id
                for compact_batch in compact_batches
            )
        )
    ]

    if missing_batches:
        raise ValueError(
            "Spark file-sink metadata has missing batches: "
            f"{missing_batches[:10]}"
        )

    logging.info(
        "SUCCESS: Spark checkpoint and file-sink "
        "metadata are continuous."
    )


# ============================================================
# 5. CHECK ORPHAN PARQUET INVENTORY
# ============================================================


def check_orphan_files():

    logging.info("Checking for untracked Parquet files")

    s3 = _s3_client()

    actual_keys = {
        obj["Key"]
        for obj in _list_objects(s3, S3_STREAMING_PREFIX)
        if obj["Key"].lower().endswith(".parquet")
    }

    metadata_objects = _list_objects(
        s3,
        f"{S3_STREAMING_PREFIX}_spark_metadata/"
    )

    tracked_keys = set()

    for obj in metadata_objects:
        body = s3.get_object(
            Bucket=S3_BUCKET,
            Key=obj["Key"]
        )["Body"].read()

        lines = [
            line.strip()
            for line in body.decode("utf-8", "replace").splitlines()
            if line.strip()
        ]

        if lines and lines[0] == "v1":
            lines = lines[1:]

        for line in lines:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    "Invalid file-sink metadata in "
                    f"{obj['Key']}"
                ) from exc

            if payload.get("isDir"):
                continue

            key = _s3_key_from_uri(payload.get("path"))

            if key:
                tracked_keys.add(key)

    if not actual_keys:
        raise ValueError(
            "No Parquet files found in the streaming output prefix."
        )

    if not tracked_keys:
        raise ValueError(
            "No tracked Parquet paths found in file-sink metadata."
        )

    missing_tracked = tracked_keys - actual_keys
    orphan_keys = actual_keys - tracked_keys

    logging.info("Actual Parquet files: %s", len(actual_keys))
    logging.info("Tracked Parquet files: %s", len(tracked_keys))
    logging.info("Orphan Parquet files: %s", len(orphan_keys))
    logging.info(
        "Orphan limit: %s",
        MAX_ORPHAN_FILES
    )

    if missing_tracked:
        raise ValueError(
            "File-sink metadata references missing Parquet files: "
            f"{sorted(missing_tracked)[:10]}"
        )

    if len(orphan_keys) > MAX_ORPHAN_FILES:
        raise ValueError(
            "Orphan Parquet count exceeds the configured limit: "
            f"{len(orphan_keys)} > {MAX_ORPHAN_FILES}"
        )

    logging.info(
        "SUCCESS: Orphan Parquet inventory is within the configured limit."
    )


# ============================================================
# 6. CHECK KAFKA LAG AGAINST THE COMMITTED SPARK OFFSETS
# ============================================================


def _parse_kafka_offsets(raw_offsets):
    offsets = {}

    for item in raw_offsets.split(","):
        item = item.strip()

        if not item:
            continue

        parts = item.split(":")

        if len(parts) != 3:
            raise ValueError(
                f"Unexpected Kafka offset format: {item}"
            )

        topic, partition, offset = parts

        if topic != KAFKA_TOPIC:
            continue

        offsets[int(partition)] = int(offset)

    if not offsets:
        raise ValueError(
            f"No Kafka offsets found for topic {KAFKA_TOPIC}."
        )

    return offsets


def check_kafka_lag():

    logging.info("Checking Kafka lag from the live topic")

    if not os.path.exists(KAFKA_HEALTH_FILE):
        raise ValueError(
            "Kafka offset health file is missing: "
            f"{KAFKA_HEALTH_FILE}"
        )

    with open(
        KAFKA_HEALTH_FILE,
        encoding="utf-8"
    ) as health_file:
        health = json.load(health_file)

    health_timestamp = health.get("timestamp")

    if not health_timestamp:
        raise ValueError(
            "Kafka offset health file has no timestamp."
        )

    if health_timestamp.endswith("Z"):
        health_timestamp = (
            health_timestamp[:-1] + "+00:00"
        )

    health_time = datetime.fromisoformat(health_timestamp)

    if health_time.tzinfo is None:
        health_time = health_time.replace(tzinfo=timezone.utc)

    health_age = (
        datetime.now(timezone.utc) - health_time
    ).total_seconds() / 60

    logging.info("Kafka health file age: %.2f minutes", health_age)

    if health_age > KAFKA_HEALTH_FRESHNESS_MINUTES:
        raise ValueError(
            "Kafka offset health data is stale: "
            f"{health_age:.2f} minutes"
        )

    if health.get("topic") != KAFKA_TOPIC:
        raise ValueError(
            "Kafka health topic mismatch: "
            f"{health.get('topic')!r} != {KAFKA_TOPIC!r}"
        )

    latest_offsets = _parse_kafka_offsets(
        health.get("offsets", "")
    )

    s3 = _s3_client()

    commit_objects = [
        (_batch_id(obj["Key"]), obj)
        for obj in _list_objects(
            s3,
            f"{S3_CHECKPOINT_PREFIX}commits/"
        )
        if _batch_id(obj["Key"]) is not None
    ]

    if not commit_objects:
        raise ValueError(
            "Spark commit checkpoint is missing."
        )

    # Use the newest commit at or before the Kafka offset snapshot.
    # The exporter and Spark commit on independent 30-second clocks,
    # so comparing against a newer commit can look like negative lag.
    commit_candidates = [
        (batch_id, obj)
        for batch_id, obj in commit_objects
        if obj["LastModified"].astimezone(timezone.utc) <= health_time
    ]

    if not commit_candidates:
        logging.warning(
            "No Spark commit at or before Kafka snapshot %s; "
            "using the latest commit.",
            health_time.isoformat()
        )
        commit_candidates = commit_objects

    lag_commit_id, _ = max(
        commit_candidates,
        key=lambda item: item[0]
    )

    logging.info("Kafka lag checkpoint batch: %s", lag_commit_id)

    offset_objects = _list_objects(
        s3,
        f"{S3_CHECKPOINT_PREFIX}offsets/"
    )

    offset_object = next(
        (
            obj
            for obj in offset_objects
            if _batch_id(obj["Key"]) == lag_commit_id
        ),
        None
    )

    if offset_object is None:
        raise ValueError(
            "Committed Spark offsets are missing for batch "
            f"{lag_commit_id}."
        )

    body = s3.get_object(
        Bucket=S3_BUCKET,
        Key=offset_object["Key"]
    )["Body"].read()

    body_lines = [
        line
        for line in body.decode("utf-8").splitlines()
        if line.strip()
    ]

    if body_lines and body_lines[0].strip() == "v1":
        body_lines = body_lines[1:]

    if not body_lines:
        raise ValueError(
            "Committed Spark offsets are empty for batch "
            f"{lag_commit_id}."
        )

    payload = json.loads(body_lines[-1])
    committed_raw = payload.get(KAFKA_TOPIC)

    if not isinstance(committed_raw, dict):
        raise ValueError(
            "Committed Spark offsets do not contain topic "
            f"{KAFKA_TOPIC}."
        )

    committed_offsets = {
        int(partition): int(offset)
        for partition, offset in committed_raw.items()
    }

    missing_from_checkpoint = (
        set(latest_offsets) - set(committed_offsets)
    )
    missing_from_topic = (
        set(committed_offsets) - set(latest_offsets)
    )

    if missing_from_checkpoint:
        raise ValueError(
            "Kafka topic has partitions absent from the "
            f"committed Spark offsets: {sorted(missing_from_checkpoint)}"
        )

    if missing_from_topic:
        raise ValueError(
            "Committed Spark offsets have partitions absent from "
            f"the Kafka topic: {sorted(missing_from_topic)}"
        )

    lag = {
        partition: latest_offsets[partition] - committed_offsets[partition]
        for partition in latest_offsets
    }

    logging.info("Kafka committed offsets: %s", committed_offsets)
    logging.info("Kafka latest offsets: %s", latest_offsets)
    logging.info("Kafka lag by partition: %s", lag)

    negative_lag = {
        partition: value
        for partition, value in lag.items()
        if value < 0
    }

    if negative_lag:
        raise ValueError(
            "Kafka offsets moved backwards: "
            f"{negative_lag}"
        )

    maximum_lag = max(lag.values())

    if maximum_lag > KAFKA_MAX_LAG_MESSAGES:
        raise ValueError(
            "Kafka lag exceeds the configured limit: "
            f"{maximum_lag} > {KAFKA_MAX_LAG_MESSAGES}"
        )

    logging.info(
        "SUCCESS: Kafka lag is within the configured limit "
        "(max=%s, limit=%s).",
        maximum_lag,
        KAFKA_MAX_LAG_MESSAGES
    )


# ============================================================
# 7. AIRFLOW DAG
# ============================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="binance_streaming_monitor",
    description="Monitor Kafka to Spark streaming ingestion into S3",
    default_args=default_args,
    start_date=datetime(2026, 10, 31),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "binance",
        "kafka",
        "spark",
        "streaming",
        "s3"
    ],
) as dag:

    monitor_s3_streaming = PythonOperator(
        task_id="check_streaming_data_in_s3",
        python_callable=check_streaming_data,
    )

    monitor_spark_checkpoint = PythonOperator(
        task_id="check_spark_checkpoint_and_file_sink",
        python_callable=check_spark_checkpoint_and_file_sink,
    )

    monitor_orphan_files = PythonOperator(
        task_id="check_orphan_files",
        python_callable=check_orphan_files,
    )

    monitor_kafka_lag = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_lag,
    )
