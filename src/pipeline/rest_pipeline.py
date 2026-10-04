import logging
import uuid

from datetime import datetime, timezone

from src.api.binance_rest import BinanceRESTClient
from src.validation.validator import validate_kline_response
from src.storage.s3_client import S3Client


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
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    binance = BinanceRESTClient(
        base_url="https://api.binance.com"
    )

    s3 = S3Client(
        bucket_name="binance-market-data-lake",
        region_name="ap-south-2"
    )

    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT"
    ]

    interval = "1m"

    pipeline_run_id = str(uuid.uuid4())

    for symbol in symbols:

        try:

            logger.info(
                "Starting ingestion for %s",
                symbol
            )

            # 1. Extract
            data = binance.get_klines(
                symbol=symbol,
                interval=interval,
                limit=100
            )

            # 2. Validate
            validate_kline_response(
                data,
                symbol
            )

            # 3. Add metadata
            payload = create_metadata(
                symbol=symbol,
                interval=interval,
                records=data,
                pipeline_run_id=pipeline_run_id
            )

            # 4. Generate S3 key
            ingestion_date = datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d")

            timestamp = datetime.now(
                timezone.utc
            ).strftime("%H%M%S")

            s3_key = (
                f"landing/rest/klines/"
                f"symbol={symbol}/"
                f"interval={interval}/"
                f"date={ingestion_date}/"
                f"klines_{timestamp}_{pipeline_run_id}.json"
            )

            # 5. Upload to S3
            s3.upload_json(
                data=payload,
                key=s3_key
            )

            logger.info(
                "Successfully completed %s",
                symbol
            )

        except Exception as error:

            logger.exception(
                "Failed ingestion for %s: %s",
                symbol,
                error
            )


if __name__ == "__main__":
    run_pipeline()