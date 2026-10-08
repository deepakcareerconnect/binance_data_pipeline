import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from src.historical.historical_pipeline import HistoricalPipeline
from src.storage.s3_client import S3Client


load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# Configuration
# =========================================================

HISTORICAL_ROOT = Path("data/historical")

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT"
]

INTERVAL = "1m"


# =========================================================
# Discover historical archives
# =========================================================

def discover_historical_archives(symbol: str):
    """
    Discover all historical ZIP archives for a symbol.
    """

    symbol_root = HISTORICAL_ROOT / symbol

    archives = sorted(
        symbol_root.rglob("*.zip")
    )

    logger.info(
        "Historical archives discovered | "
        "symbol=%s | count=%s",
        symbol,
        len(archives)
    )

    for archive in archives:
        logger.info(
            "Archive discovered | "
            "symbol=%s | file=%s",
            symbol,
            archive
        )

    return archives


# =========================================================
# Run historical batch
# =========================================================

def run_historical_batch():

    logger.info("=" * 70)
    logger.info("HISTORICAL BATCH INGESTION STARTED")
    logger.info("=" * 70)

    # =====================================================
    # AWS configuration
    # =====================================================

    s3_bucket = os.getenv("S3_BUCKET")

    aws_region = os.getenv(
        "AWS_REGION",
        "ap-south-2"
    )

    if not s3_bucket:
        raise RuntimeError(
            "S3_BUCKET environment variable is not set. "
            "Set it before running the historical pipeline."
        )

    logger.info(
        "S3 configuration | bucket=%s | region=%s",
        s3_bucket,
        aws_region
    )

    # =====================================================
    # Create S3 client
    # =====================================================

    s3_client = S3Client(
        bucket_name=s3_bucket,
        region_name=aws_region
    )

    # =====================================================
    # Create historical pipeline
    # =====================================================

    pipeline = HistoricalPipeline(
        s3_client=s3_client,
        interval=INTERVAL,
        cleanup_extracted_files=True
    )

    # =====================================================
    # Batch counters
    # =====================================================

    total_archives = 0
    successful = 0
    skipped = 0
    failed = 0

    total_records_read = 0
    total_records_written = 0

    # =====================================================
    # Process each symbol
    # =====================================================

    for symbol in SYMBOLS:

        logger.info("")
        logger.info("#" * 70)
        logger.info(
            "STARTING SYMBOL | %s",
            symbol
        )
        logger.info("#" * 70)

        # -------------------------------------------------
        # Discover archives
        # -------------------------------------------------

        archives = discover_historical_archives(symbol)

        if not archives:

            logger.warning(
                "No historical ZIP archives found | "
                "symbol=%s | path=%s",
                symbol,
                HISTORICAL_ROOT / symbol
            )

            continue

        total = len(archives)

        total_archives += total

        logger.info(
            "Archives to process | "
            "symbol=%s | count=%s",
            symbol,
            total
        )

        # -------------------------------------------------
        # Process archives
        # -------------------------------------------------

        for index, archive_path in enumerate(
            archives,
            start=1
        ):

            logger.info("=" * 70)

            logger.info(
                "PROCESSING ARCHIVE | "
                "symbol=%s | "
                "archive=%s/%s",
                symbol,
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
                    symbol=symbol
                )

                status = result.get(
                    "status"
                )

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

                # -----------------------------------------
                # Result classification
                # -----------------------------------------

                if status == "SKIPPED_ALREADY_EXISTS":

                    skipped += 1

                elif status in (
                    "SUCCESS",
                    "COMPLETED"
                ):

                    successful += 1

                else:

                    # Treat unknown statuses as successful
                    # only if process_archive completed
                    successful += 1

                logger.info(
                    "Archive completed | "
                    "symbol=%s | "
                    "archive=%s | "
                    "status=%s | "
                    "records_read=%s | "
                    "records_written=%s",
                    symbol,
                    archive_path.name,
                    status,
                    records_read,
                    records_written
                )

            except Exception:

                failed += 1

                logger.exception(
                    "Archive ingestion failed | "
                    "symbol=%s | "
                    "archive=%s",
                    symbol,
                    archive_path
                )

                # Continue with the next archive
                continue

        logger.info("")
        logger.info(
            "SYMBOL COMPLETED | "
            "symbol=%s | "
            "archives=%s",
            symbol,
            total
        )

    # =====================================================
    # Final summary
    # =====================================================

    logger.info("")
    logger.info("=" * 70)
    logger.info("HISTORICAL BATCH INGESTION COMPLETED")
    logger.info("=" * 70)

    logger.info(
        "Symbols processed=%s",
        ", ".join(SYMBOLS)
    )

    logger.info(
        "Total archives=%s",
        total_archives
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


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":
    run_historical_batch()