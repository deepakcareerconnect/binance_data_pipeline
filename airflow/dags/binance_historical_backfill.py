
from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="binance_historical_backfill",
    description=(
        "Download historical Binance market data "
        "and ingest it into the S3 data lake."
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["binance", "historical", "s3", "backfill"],
) as dag:

    historical_backfill_to_s3 = BashOperator(
        task_id="historical_backfill_to_s3",
        bash_command=(
            "set -e; "
            "cd /opt/airflow/project && "
            "python -m src.historical.run_historical_batch"
        ),
        execution_timeout=timedelta(hours=6),
        do_xcom_push=False,
    )

    historical_backfill_to_s3
