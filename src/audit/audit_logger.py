import json
import logging

from datetime import datetime, timezone
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


def create_audit_record(
    pipeline_name: str,
    symbol: str,
    status: str,
    records_read: int = 0,
    records_written: int = 0,
    s3_key: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    run_id: Optional[str] = None,
    interval: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a standardized audit record for the Binance ingestion pipeline.
    """

    # Start time
    if start_time is None:
        start_time_value = None
    elif isinstance(start_time, datetime):
        start_time_value = start_time.isoformat()
    else:
        start_time_value = str(start_time)

    # End time
    if end_time is None:
        end_time_value = datetime.now(
            timezone.utc
        ).isoformat()
    elif isinstance(end_time, datetime):
        end_time_value = end_time.isoformat()
    else:
        end_time_value = str(end_time)

    audit_record = {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "source": source,
        "symbol": symbol,
        "interval": interval,
        "status": status,
        "records_read": records_read,
        "records_written": records_written,
        "s3_key": s3_key,
        "start_time": start_time_value,
        "end_time": end_time_value,
        "error_message": error_message,
        "metadata": metadata or {},
    }

    return audit_record


def serialize_audit_record(
    audit_record: Dict[str, Any]
) -> str:
    """
    Serialize audit record to formatted JSON.
    """

    return json.dumps(
        audit_record,
        indent=2,
        default=str
    )


def log_audit_record(
    audit_record: Dict[str, Any]
) -> None:
    """
    Write audit record to application logs.
    """

    logger.info(
        "AUDIT | %s",
        serialize_audit_record(audit_record)
    )