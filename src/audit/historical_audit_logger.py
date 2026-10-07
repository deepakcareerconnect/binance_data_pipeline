import logging
import uuid
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


def create_historical_audit_record(
    pipeline_name: str,
    symbol: str,
    interval: str,
    archive_name: str,
    checksum: str,
    records_read: int,
    records_written: int,
    s3_key: str,
    status: str,
    start_time: str,
    error_message: str | None = None,
    pipeline_run_id: str | None = None
) -> dict:
    """
    Create an audit record for one historical ingestion run.

    The audit record captures:
    - Pipeline execution ID
    - Source information
    - Symbol and interval
    - Source archive
    - SHA-256 checksum
    - Records read
    - Records written
    - Destination S3 key
    - Pipeline status
    - Start and end timestamps
    - Optional error information
    """

    if pipeline_run_id is None:
        pipeline_run_id = str(
            uuid.uuid4()
        )

    end_time = datetime.now(
        timezone.utc
    ).isoformat()

    audit_record = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "source": "binance_vision",
        "dataset": "historical_klines",
        "symbol": symbol,
        "interval": interval,
        "archive_name": archive_name,
        "source_checksum_sha256": checksum,
        "records_read": records_read,
        "records_written": records_written,
        "s3_key": s3_key,
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "error_message": error_message
    }

    logger.info(
        "Historical audit record created | "
        "pipeline_run_id=%s | "
        "status=%s",
        pipeline_run_id,
        status
    )

    return audit_record