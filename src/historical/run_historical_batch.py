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


# =========================================================
# Configuration
# =========================================================

HISTORICAL_ROOT = Path("data/historical")

SYMBOL = "BTCUSDT"
INTERVAL = "1m"


def discover_historical_archives():
    """
    Discover all historical ZIP archives recursively.
    """

    archives = sorted(
        HISTORICAL_ROOT
        .joinpath(SYMBOL)
        .rglob("*.zip")
    )

    logger.info(
        "Historical archives discovered | count=%s",
        len(archives)
    )

    for archive in archives:
        logger.info(
            "Archive discovered | file=%s",
            archive
        )

    return archives


def run_historical_batch():

    logger.info("=" * 70)
    logger.info("HISTORICAL BATCH INGESTION STARTED")
    logger.info("=" * 70)

    # =========================================================
    # Discover archives
    # =========================================================

    archives = discover_historical_archives()

    if not archives:

        logger.warning(
            "No historical ZIP archives found | path=%s",
            HISTORICAL_ROOT
        )

        return

    total = len(archives)

    logger.info(
        "Total archives to process=%s",
        total
    )

    # =========================================================
    # AWS configuration
    # =========================================================

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
    # Create S3 client
    # =========================================================

    s3_client = S3Client(
        bucket_name=s3_bucket,
        region_name=aws_region
    )

    # =========================================================
    # Create pipeline
    # =========================================================

    pipeline = HistoricalPipeline(
        s3_client=s3_client,
        interval=INTERVAL,
        cleanup_extracted_files=True
    )

    # =========================================================
    # Batch counters
    # =========================================================

    successful = 0
    skipped = 0
    failed = 0

    total_records_read = 0
    total_records_written = 0

    # =========================================================
    # Process every archive
    # =========================================================

    for index, archive_path in enumerate(
        archives,
        start=1
    ):

        logger.info("=" * 70)

        logger.info(
            "PROCESSING ARCHIVE %s/%s",
            index,
            total
        )

        logger.info(
            "Archive=%s",
            archive_path
        )

        logger.info("=" * 70)

        try:

            result = pipeline.process_archive(
                archive_path=str(archive_path),
                symbol=SYMBOL
            )

            status = result.get("status")

            records_read = result.get(
                "records_read",
                0
            )

            records_written = result.get(
                "records_written",
                0
            )

            total_records_read += records_read
            total_records_written += records_written

            if status == "SKIPPED_ALREADY_EXISTS":

                skipped += 1

            else:

                successful += 1

            logger.info(
                "Archive completed | "
                "archive=%s | "
                "status=%s | "
                "records_read=%s | "
                "records_written=%s",
                archive_path.name,
                status,
                records_read,
                records_written
            )

        except Exception:

            failed += 1

            logger.exception(
                "Archive ingestion failed | archive=%s",
                archive_path
            )

            # Continue processing the remaining archives
            continue

    # =========================================================
    # Final summary
    # =========================================================

    logger.info("=" * 70)
    logger.info("HISTORICAL BATCH INGESTION COMPLETED")
    logger.info("=" * 70)

    logger.info(
        "Total archives=%s",
        total
    )

    logger.info(
        "Successful=%s",
        successful
    )

    logger.info(
        "Skipped=%s",
        skipped
    )

    logger.info(
        "Failed=%s",
        failed
    )

    logger.info(
        "Total records read=%s",
        total_records_read
    )

    logger.info(
        "Total records written=%s",
        total_records_written
    )

    logger.info("=" * 70)


if __name__ == "__main__":
    run_historical_batch()