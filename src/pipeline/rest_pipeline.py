import logging
import uuid

from datetime import datetime, timezone

from src.audit.audit_logger import create_audit_record
from src.api.binance_rest import BinanceRESTClient
from src.validation.validator import validate_kline_response
from src.storage.s3_client import S3Client
from src.checkpoint.checkpoint_manager import CheckpointManager
from src.storage.parquet_writer import convert_klines_to_parquet


logger = logging.getLogger(__name__)


def run_pipeline():

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        )
    )

    # ==================================================
    # PIPELINE RUN ID
    # ==================================================

    # One unique ID for the complete pipeline execution
    pipeline_run_id = str(uuid.uuid4())

    logger.info(
        "Pipeline started | run_id=%s",
        pipeline_run_id
    )

    # ==================================================
    # BINANCE REST CLIENT
    # ==================================================

    binance = BinanceRESTClient(
        base_url="https://api.binance.com",
        max_retries=3,
        backoff_factor=2
    )

    # ==================================================
    # S3 CLIENT
    # ==================================================

    s3 = S3Client(
        bucket_name="binance-market-data-lake",
        region_name="ap-south-2"
    )

    # ==================================================
    # CHECKPOINT MANAGER
    # ==================================================

    checkpoint_manager = CheckpointManager(
        s3_client=s3
    )

    # ==================================================
    # PIPELINE CONFIGURATION
    # ==================================================

    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT"
    ]

    interval = "1m"

    # ==================================================
    # PROCESS EACH SYMBOL
    # ==================================================

    for symbol in symbols:

        start_time = datetime.now(
            timezone.utc
        )

        records_read = 0
        records_written = 0

        # Reset data for every symbol
        data = None

        try:

            logger.info(
                "Starting ingestion | symbol=%s",
                symbol
            )

            # ==================================================
            # 1. EXTRACT
            # ==================================================

            data = binance.get_klines(
                symbol=symbol,
                interval=interval,
                limit=100
            )

            records_read = len(data)

            logger.info(
                "Extraction completed | "
                "symbol=%s | records=%s",
                symbol,
                records_read
            )

            # ==================================================
            # 2. VALIDATE
            # ==================================================

            validate_kline_response(
                data=data,
                symbol=symbol
            )

            # ==================================================
            # 3. TRANSFORM
            # ==================================================

            parquet_data = convert_klines_to_parquet(
                records=data,
                symbol=symbol,
                interval=interval
            )

            logger.info(
                "Transformation completed | "
                "symbol=%s",
                symbol
            )

            # ==================================================
            # 4. BUILD S3 KEY
            # ==================================================

            timestamp = datetime.now(
                timezone.utc
            )

            date = timestamp.strftime(
                "%Y-%m-%d"
            )

            time_value = timestamp.strftime(
                "%H%M%S"
            )

            s3_key = (
                f"landing/rest/klines/"
                f"symbol={symbol}/"
                f"interval={interval}/"
                f"date={date}/"
                f"klines_{time_value}_"
                f"{pipeline_run_id}.parquet"
            )

            # ==================================================
            # 5. LOAD PARQUET TO S3
            # ==================================================

            s3.upload_parquet(
                parquet_data=parquet_data,
                key=s3_key
            )

            records_written = len(data)

            logger.info(
                "Data loaded successfully | "
                "symbol=%s | records=%s",
                symbol,
                records_written
            )

            # ==================================================
            # 6. SAVE CHECKPOINT
            # ==================================================

            last_timestamp = data[-1][6]

            checkpoint_manager.save_checkpoint(
                symbol=symbol,
                interval=interval,
                last_processed_timestamp=last_timestamp
            )

            logger.info(
                "Checkpoint saved | "
                "symbol=%s | timestamp=%s",
                symbol,
                last_timestamp
            )

            # ==================================================
            # 7. CREATE SUCCESS AUDIT
            # ==================================================

            audit_record = create_audit_record(
                pipeline_name="binance_rest_ingestion",
                run_id=pipeline_run_id,
                symbol=symbol,
                interval=interval,
                records_read=records_read,
                records_written=records_written,
                status="SUCCESS",
                start_time=start_time,
                end_time=datetime.now(timezone.utc),
                s3_key=s3_key,
                source="binance_rest_api",
                metadata={
                    "dataset": "klines",
                    "limit": 100
                }
            )

            # ==================================================
            # 8. WRITE SUCCESS AUDIT TO S3
            # ==================================================

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
                "Audit record written | "
                "symbol=%s | status=SUCCESS",
                symbol
            )

            logger.info(
                "Pipeline successful | "
                "symbol=%s | run_id=%s",
                symbol,
                pipeline_run_id
            )

        except Exception as error:

            # ==================================================
            # PIPELINE FAILURE
            # ==================================================

            logger.exception(
                "Pipeline failed | "
                "symbol=%s | run_id=%s",
                symbol,
                pipeline_run_id
            )

            # ==================================================
            # 9. QUARANTINE FAILED DATA
            # ==================================================

            try:

                if data is not None:

                    s3.upload_quarantine(
                        data=data,
                        symbol=symbol,
                        error_message=str(error)
                    )

                    logger.warning(
                        "Data moved to quarantine | "
                        "symbol=%s",
                        symbol
                    )

            except Exception:

                logger.exception(
                    "Failed to quarantine data | "
                    "symbol=%s",
                    symbol
                )

            # ==================================================
            # 10. CREATE FAILURE AUDIT
            # ==================================================

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
                    run_id=pipeline_run_id,
                    symbol=symbol,
                    interval=interval,
                    records_read=records_read,
                    records_written=records_written,
                    status="FAILED",
                    start_time=start_time,
                    end_time=datetime.now(timezone.utc),
                    error_message=str(error),
                    source="binance_rest_api",
                    metadata={
                        "dataset": "klines",
                        "limit": 100
                    }
                )

                # ==================================================
                # 11. WRITE FAILURE AUDIT TO S3
                # ==================================================

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

                logger.info(
                    "Failure audit written | "
                    "symbol=%s",
                    symbol
                )

            except Exception:

                logger.exception(
                    "Failed to write audit log | "
                    "symbol=%s",
                    symbol
                )

    # ==================================================
    # PIPELINE COMPLETE
    # ==================================================

    logger.info(
        "Pipeline completed | run_id=%s",
        pipeline_run_id
    )


if __name__ == "__main__":
    run_pipeline()