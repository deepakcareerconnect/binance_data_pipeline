import logging
from datetime import datetime, timezone
import uuid

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

    if pipeline_run_id is None:
        pipeline_run_id = str(uuid.uuid4())

    return {
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
        "end_time": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message
    }