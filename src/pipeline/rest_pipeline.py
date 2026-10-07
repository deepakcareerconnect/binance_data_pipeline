import logging
import uuid

from datetime import datetime, timezone

from src.api.binance_rest import BinanceRESTClient
from src.validation.validator import validate_kline_response
from src.storage.s3_client import S3Client
from src.audit.historical_audit_logger import create_audit_record
from src.checkpoint.checkpoint_manager import CheckpointManager
from src.storage.parquet_writer import convert_klines_to_parquet

logger = logging.getLogger(__name__)


def create_metadata(
    symbol: str,
    interval: str,
    records: list,  
    pipeline_run_id: str
) -> dict:

    return {
        "source": "binance",
        "source_type": "rest_api",
        "dataset": "klines",
        "symbol": symbol,
        "interval": interval,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "pipeline_run_id": pipeline_run_id,
        "record_count": len(records),
        "payload": records
    }


def run_pipeline():

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        )
    )

    pipeline_run_id = str(uuid.uuid4())

    binance = BinanceRESTClient(
        base_url="https://api.binance.com",
        max_retries=3,
        backoff_factor=2
    )

    s3 = S3Client(
        bucket_name="binance-market-data-lake",
        region_name="ap-south-2"
    )

    checkpoint_manager = CheckpointManager(
        s3_client=s3
    )

    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT"
    ]

    interval = "1m"

    for symbol in symbols:

        start_time = datetime.now(
            timezone.utc
        ).isoformat()

        records_read = 0
        records_written = 0

        try:

            logger.info(
                "Starting ingestion | symbol=%s",
                symbol
            )

            # ------------------------------------------
            # 1. Extract
            # ------------------------------------------

            data = binance.get_klines(
                symbol=symbol,
                interval=interval,
                limit=100
            )

            records_read = len(data)

            # ------------------------------------------
            # 2. Validate
            # ------------------------------------------

            validate_kline_response(
                data=data,
                symbol=symbol
            )

            # ------------------------------------------
            # 3. Add metadata
            # ------------------------------------------

            parquet_data = convert_klines_to_parquet(
                records=data,
                symbol=symbol,
                interval=interval
            )

            timestamp = datetime.now(timezone.utc)

            date = timestamp.strftime("%Y-%m-%d")
            time_value = timestamp.strftime("%H%M%S")

            s3_key = (
                f"landing/rest/klines/"
                f"symbol={symbol}/"
                f"interval={interval}/"
                f"date={date}/"
                f"klines_{time_value}_"
                f"{pipeline_run_id}.parquet"
            )

            s3.upload_parquet(
                parquet_data=parquet_data,
                key=s3_key
            )

            records_written = len(data)

            # ------------------------------------------
            # 6. Save checkpoint
            # ------------------------------------------

            last_timestamp = data[-1][6]

            checkpoint_manager.save_checkpoint(
                symbol=symbol,
                interval=interval,
                last_processed_timestamp=last_timestamp
            )

            # ------------------------------------------
            # 7. Audit SUCCESS
            # ------------------------------------------

            audit_record = create_audit_record(
                pipeline_name="binance_rest_ingestion",
                symbol=symbol,
                records_read=records_read,
                records_written=records_written,
                status="SUCCESS",
                start_time=start_time
            )

            audit_key = (
                f"metadata/ingestion_audit/"
                f"date={date}/"
                f"{symbol}_{time_value}_"
                f"{pipeline_run_id}.json"
            )

            s3.upload_json(
                data=audit_record,
                key=audit_key
            )

            logger.info(
                "Pipeline successful | symbol=%s",
                symbol
            )

        except Exception as error:

            logger.exception(
                "Pipeline failed | symbol=%s",
                symbol
            )

            # ------------------------------------------
            # Quarantine
            # ------------------------------------------

            try:

                s3.upload_quarantine(
                    data=locals().get(
                        "data",
                        None
                    ),
                    symbol=symbol,
                    error_message=str(error)
                )

            except Exception:

                logger.exception(
                    "Failed to quarantine data | "
                    "symbol=%s",
                    symbol
                )

            # ------------------------------------------
            # Audit FAILURE
            # ------------------------------------------

            try:

                timestamp = datetime.now(
                    timezone.utc
                )

                date = timestamp.strftime(
                    "%Y-%m-%d"
                )

                time_value = timestamp.strftime(
                    "%H%M%S"
                )

                audit_record = create_audit_record(
                    pipeline_name="binance_rest_ingestion",
                    symbol=symbol,
                    records_read=records_read,
                    records_written=records_written,
                    status="FAILED",
                    start_time=start_time,
                    error_message=str(error)
                )

                audit_key = (
                    f"metadata/ingestion_audit/"
                    f"date={date}/"
                    f"{symbol}_{time_value}_"
                    f"{pipeline_run_id}_FAILED.json"
                )

                s3.upload_json(
                    data=audit_record,
                    key=audit_key
                )

            except Exception:

                logger.exception(
                    "Failed to write audit log"
                )


if __name__ == "__main__":
    run_pipeline()