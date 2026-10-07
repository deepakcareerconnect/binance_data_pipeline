import logging
import os
from pathlib import Path

from src.historical.historical_pipeline import HistoricalPipeline
from src.storage.s3_client import S3Client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def main():

    # =========================================================
    # Configuration
    # =========================================================

    symbol = "BTCUSDT"
    interval = "1m"

    # We intentionally process ONLY one archive first.
    archive_path = Path(
        "data/historical/BTCUSDT/"
        "BTCUSDT-1m-2025-01.zip"
    )

    s3_bucket = os.getenv("S3_BUCKET")

    aws_region = os.getenv(
        "AWS_REGION",
        "ap-south-1"
    )

    if not s3_bucket:
        raise RuntimeError(
            "S3_BUCKET environment variable is not set. "
            "Set it before running the historical pipeline."
        )

    # =========================================================
    # Validate local archive
    # =========================================================

    if not archive_path.exists():

        raise FileNotFoundError(
            f"Historical archive not found: {archive_path}"
        )

    logger.info(
        "Historical ingestion test started"
    )

    logger.info(
        "Symbol=%s | interval=%s | archive=%s",
        symbol,
        interval,
        archive_path
    )

    # =========================================================
    # Create S3 client
    # =========================================================

    s3_client = S3Client(
        bucket_name=s3_bucket,
        region_name=aws_region
    )

    # =========================================================
    # Create historical pipeline
    # =========================================================

    pipeline = HistoricalPipeline(
        s3_client=s3_client,
        interval=interval,
        cleanup_extracted_files=True
    )

    # =========================================================
    # Process archive
    # =========================================================

    result = pipeline.process_archive(
        archive_path=str(archive_path),
        symbol=symbol
    )

    # =========================================================
    # Final result
    # =========================================================

    logger.info(
        "Historical ingestion completed successfully"
    )

    logger.info(
        "Status=%s",
        result["status"]
    )

    logger.info(
        "Records read=%s",
        result["records_read"]
    )

    logger.info(
        "Records written=%s",
        result["records_written"]
    )

    logger.info(
        "S3 key=%s",
        result["s3_key"]
    )


if __name__ == "__main__":
    main()