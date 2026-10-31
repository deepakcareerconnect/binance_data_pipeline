
"""
Binance end-to-end orchestration DAG.

Flow:
REST ingestion -> Bronze -> Silver klines + Silver trades
-> Gold analytics + Gold AI -> Data quality -> AI API health check

Required Airflow connection:
    databricks_default
        Host: https://<your-databricks-workspace-host>
        Password: <Databricks personal access token>

Required Airflow Variables:
    DATABRICKS_BRONZE_JOB_ID
    DATABRICKS_SILVER_KLINES_JOB_ID
    DATABRICKS_SILVER_TRADES_JOB_ID
    DATABRICKS_GOLD_ANALYTICS_JOB_ID
    DATABRICKS_GOLD_AI_JOB_ID
    DATABRICKS_QUALITY_JOB_ID
"""

import os
import time
from datetime import timedelta

import pendulum
import requests

from airflow.sdk import DAG, Variable
from airflow.sdk.bases.hook import BaseHook

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# 1. CONFIGURATION
# ============================================================

DAG_ID = "binance_end_to_end"
PROJECT_DIR = "/opt/airflow/project"

DATABRICKS_CONNECTION_ID = "databricks_default"

DATABRICKS_API_TIMEOUT = 30
DATABRICKS_POLL_INTERVAL = 20
DATABRICKS_MAX_WAIT_SECONDS = 2 * 60 * 60

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

AI_API_BASE_URL = os.getenv(
    "AI_API_BASE_URL",
    "http://ai-serving:8000"
)

AI_API_HEALTH_PATH = os.getenv(
    "AI_API_HEALTH_PATH",
    "/health"
)

# ============================================================
# 2. AIRFLOW VARIABLE HELPER
# ============================================================

def get_required_variable(variable_name: str) -> str:
    """
    Retrieve a required Airflow Variable.

    Airflow 3 SDK Variable.get() does not support the
    default/default_var argument used in older examples.
    """

    try:
        value = Variable.get(variable_name)
    except KeyError as exc:
        raise ValueError(
            f"Missing Airflow Variable: {variable_name}. "
            "Create it under Airflow Admin -> Variables."
        ) from exc

    if value is None or not str(value).strip():
        raise ValueError(
            f"Airflow Variable {variable_name} is empty."
        )

    return str(value).strip()


# ============================================================
# 3. DATABRICKS CONNECTION
# ============================================================

def get_databricks_connection():
    """Read Databricks workspace host and token from Airflow."""

    connection = BaseHook.get_connection(
        DATABRICKS_CONNECTION_ID
    )

    host = (connection.host or "").strip().rstrip("/")
    token = (connection.password or "").strip()

    if host and not host.startswith(("http://", "https://")):
        host = f"https://{host}"

    if not host:
        raise ValueError(
            "Databricks connection is missing its workspace Host."
        )

    if not token:
        raise ValueError(
            "Databricks connection is missing its Password/token."
        )

    return host, token


# ============================================================
# 4. DATABRICKS JOB EXECUTION
# ============================================================

def run_databricks_job(variable_name: str, **context):
    """
    Trigger a Databricks job and wait for completion.

    The task succeeds only when Databricks reports SUCCESS.
    """

    # Read and validate the Databricks job ID.
    job_id_value = get_required_variable(variable_name)

    try:
        job_id = int(job_id_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{variable_name} must contain a numeric "
            f"Databricks Job ID. Received: {job_id_value}"
        ) from exc

    # Read workspace credentials.
    host, token = get_databricks_connection()

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })

    try:
        # ----------------------------------------------------
        # Trigger Databricks job
        # ----------------------------------------------------

        trigger_url = f"{host}/api/2.1/jobs/run-now"

        response = session.post(
            trigger_url,
            json={"job_id": job_id},
            timeout=DATABRICKS_API_TIMEOUT,
        )

        response.raise_for_status()

        run_id = response.json().get("run_id")

        if not run_id:
            raise RuntimeError(
                f"Databricks did not return a run_id "
                f"for job_id={job_id}."
            )

        print(
            f"Databricks job started successfully. "
            f"job_id={job_id}, run_id={run_id}"
        )

        # ----------------------------------------------------
        # Poll Databricks job status
        # ----------------------------------------------------

        status_url = f"{host}/api/2.1/jobs/runs/get"

        deadline = (
            time.monotonic() + DATABRICKS_MAX_WAIT_SECONDS
        )

        terminal_states = {
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR",
        }

        while time.monotonic() < deadline:

            response = session.get(
                status_url,
                params={"run_id": run_id},
                timeout=DATABRICKS_API_TIMEOUT,
            )

            response.raise_for_status()

            run = response.json()
            state = run.get("state", {})

            lifecycle = state.get(
                "life_cycle_state", "UNKNOWN"
            )

            result = state.get("result_state", "")
            message = state.get("state_message", "")

            print(
                f"Databricks job status | "
                f"job_id={job_id} | "
                f"run_id={run_id} | "
                f"lifecycle={lifecycle} | "
                f"result={result} | "
                f"message={message}"
            )

            # ------------------------------------------------
            # Successful execution
            # ------------------------------------------------

            if lifecycle == "TERMINATED":

                if result == "SUCCESS":
                    print(
                        f"Databricks job completed successfully. "
                        f"job_id={job_id}, run_id={run_id}"
                    )
                    return

                raise RuntimeError(
                    f"Databricks job failed. "
                    f"job_id={job_id}, "
                    f"run_id={run_id}, "
                    f"result={result}, "
                    f"message={message}"
                )

            # ------------------------------------------------
            # Other terminal states
            # ------------------------------------------------

            if lifecycle in terminal_states:
                raise RuntimeError(
                    f"Databricks job ended unexpectedly. "
                    f"job_id={job_id}, "
                    f"run_id={run_id}, "
                    f"lifecycle={lifecycle}, "
                    f"message={message}"
                )

            time.sleep(DATABRICKS_POLL_INTERVAL)

        # ----------------------------------------------------
        # Timeout
        # ----------------------------------------------------

        raise TimeoutError(
            f"Databricks job did not finish within "
            f"{DATABRICKS_MAX_WAIT_SECONDS // 60} minutes. "
            f"job_id={job_id}, run_id={run_id}"
        )

    finally:
        session.close()


# ============================================================
# 5. AI API HEALTH CHECK
# ============================================================

def check_ai_api():

    url = (
        f"{AI_API_BASE_URL.rstrip('/')}"
        f"{AI_API_HEALTH_PATH}"
    )

    print(f"Checking AI API: {url}")

    response = requests.get(
        url,
        timeout=15
    )

    response.raise_for_status()

    print("AI API Status:", response.status_code)
    print("AI API Response:", response.text)

    return response.status_code

# ============================================================
# 6. DAG DEFINITION
# ============================================================

with DAG(
    dag_id=DAG_ID,
    description=(
        "Orchestrate Binance REST ingestion, "
        "Databricks Bronze, Silver, Gold, "
        "data quality and AI API health."
    ),

    start_date=pendulum.datetime(
        2026, 1, 1, tz="UTC"
    ),

    schedule="0 * * * *",
    catchup=False,

    default_args=DEFAULT_ARGS,

    dagrun_timeout=timedelta(hours=9),
    max_active_runs=1,

    tags=[
        "binance",
        "data-engineering",
        "databricks",
        "end-to-end",
    ],

) as dag:

    # ========================================================
    # TASK 1: REST INGESTION -> S3
    # ========================================================

    rest_ingestion = BashOperator(
        task_id="rest_ingestion_to_s3",

        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.pipeline.rest_pipeline"
        ),

        execution_timeout=timedelta(minutes=30),
    )

    # ========================================================
    # TASK 2: BRONZE
    # ========================================================

    bronze = PythonOperator(
        task_id="databricks_bronze",

        python_callable=run_databricks_job,

        op_kwargs={
            "variable_name": "DATABRICKS_BRONZE_JOB_ID"
        },

        execution_timeout=timedelta(hours=2),
    )

    # ========================================================
    # TASK 3: SILVER KL​​INES
    # ========================================================

    silver_klines = PythonOperator(
        task_id="databricks_silver_klines",

        python_callable=run_databricks_job,

        op_kwargs={
            "variable_name": "DATABRICKS_SILVER_KLINES_JOB_ID"
        },

        execution_timeout=timedelta(hours=2),
    )

    # ========================================================
    # TASK 4: SILVER TRADES
    # ========================================================

    silver_trades = PythonOperator(
        task_id="databricks_silver_trades",

        python_callable=run_databricks_job,

        op_kwargs={
            "variable_name": "DATABRICKS_SILVER_TRADES_JOB_ID"
        },

        execution_timeout=timedelta(hours=2),
    )

    # ========================================================
    # TASK 5: GOLD ANALYTICS
    # ========================================================

    gold_analytics = PythonOperator(
        task_id="databricks_gold_analytics",

        python_callable=run_databricks_job,

        op_kwargs={
            "variable_name": "DATABRICKS_GOLD_ANALYTICS_JOB_ID"
        },

        execution_timeout=timedelta(hours=2),
    )

    # ========================================================
    # TASK 6: GOLD AI
    # ========================================================

    gold_ai = PythonOperator(
        task_id="databricks_gold_ai",

        python_callable=run_databricks_job,

        op_kwargs={
            "variable_name": "DATABRICKS_GOLD_AI_JOB_ID"
        },

        execution_timeout=timedelta(hours=2),
    )

    # ========================================================
    # TASK 7: DATA QUALITY
    # ========================================================

    data_quality = PythonOperator(
        task_id="databricks_data_quality",

        python_callable=run_databricks_job,

        op_kwargs={
            "variable_name": "DATABRICKS_QUALITY_JOB_ID"
        },

        execution_timeout=timedelta(hours=2),
    )

    # ========================================================
    # TASK 8: AI API HEALTH CHECK
    # ========================================================

    ai_api_health = PythonOperator(
        task_id="ai_api_health_check",

        python_callable=check_ai_api,

        execution_timeout=timedelta(minutes=2),
    )

    # ========================================================
    # 7. TASK DEPENDENCIES
    # ========================================================

    # REST ingestion must finish before Bronze.
    rest_ingestion >> bronze

    # Bronze must finish before both Silver jobs.
    bronze >> [silver_klines, silver_trades]

    # Both Silver jobs must finish before either Gold job.
    silver_klines >> gold_analytics
    silver_klines >> gold_ai

    silver_trades >> gold_analytics
    silver_trades >> gold_ai

    # Both Gold jobs must finish before data quality.
    [gold_analytics, gold_ai] >> data_quality

    # Data quality must pass before the AI API health check.
    data_quality >> ai_api_health
