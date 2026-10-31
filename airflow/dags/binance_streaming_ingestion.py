from datetime import datetime, timedelta, timezone
import logging
import os

import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# 1. CONFIGURATION
# ============================================================

S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "binance-market-data-lake"
)

S3_STREAMING_PREFIX = os.getenv(
    "S3_STREAMING_PREFIX",
    "landing/streaming/trades_recovery/"
)

FRESHNESS_MINUTES = int(
    os.getenv("STREAMING_FRESHNESS_MINUTES", "10")
)

AWS_REGION = os.getenv(
    "AWS_DEFAULT_REGION",
    "ap-south-1"
)


# ============================================================
# 2. CHECK S3 STREAMING DATA
# ============================================================


def check_streaming_data():

    logging.info("Checking Binance streaming data in S3")

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    paginator = s3.get_paginator("list_objects_v2")

    latest_object = None
    total_objects = 0

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=S3_STREAMING_PREFIX
    ):

        for obj in page.get("Contents", []):

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

    if latest_modified.tzinfo is None:
        latest_modified = latest_modified.replace(
            tzinfo=timezone.utc
        )

    age_minutes = (
        datetime.now(timezone.utc) - latest_modified
    ).total_seconds() / 60

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
# 3. AIRFLOW DAG
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

    monitor_s3_streaming
