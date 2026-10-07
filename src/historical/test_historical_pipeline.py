import logging
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.audit.historical_audit_logger import (
    create_historical_audit_record
)
from src.historical.archive_extractor import (
    extract_archive
)
from src.historical.checksum import (
    calculate_sha256
)
from src.historical.file_reader import (
    read_historical_file
)
from src.historical.historical_validator import (
    validate_historical_records
)
from src.historical.normalizer import (
    normalize_klines
)
from src.historical.parquet_writer import (
    records_to_parquet
)
from src.storage.s3_client import (
    S3Client
)


logger = logging.getLogger(__name__)


class HistoricalPipeline:
    """
    End-to-end Binance historical ingestion pipeline.

    Flow:

        ZIP
         ↓
        Checksum
         ↓
        Build deterministic S3 key
         ↓
        Idempotency check
         ↓
        Extract
         ↓
        Read CSV
         ↓
        Validate
         ↓
        Normalize
         ↓
        Parquet
         ↓
        Upload S3
         ↓
        Audit
         ↓
        Cleanup
    """

    def __init__(
        self,
        s3_client: S3Client,
        interval: str = "1m",
        cleanup_extracted_files: bool = True
    ):
        self.s3_client = s3_client
        self.interval = interval
        self.cleanup_extracted_files = (
            cleanup_extracted_files
        )

    # =========================================================
    # Find CSV
    # =========================================================

    def find_csv_file(
        self,
        extracted_files: list[Path]
    ) -> Path:
        """
        Find CSV file inside extracted archive.
        """

        csv_files = [
            path
            for path in extracted_files
            if path.is_file()
            and path.suffix.lower() == ".csv"
        ]

        if not csv_files:
            raise FileNotFoundError(
                "No CSV file found inside historical archive"
            )

        if len(csv_files) > 1:

            logger.warning(
                "Multiple CSV files found | "
                "count=%s | using=%s",
                len(csv_files),
                csv_files[0]
            )

        csv_file = csv_files[0]

        logger.info(
            "Historical CSV discovered | path=%s",
            csv_file
        )

        return csv_file

    # =========================================================
    # Build S3 Key
    # =========================================================

    def build_s3_key(
        self,
        symbol: str,
        interval: str,
        archive_path: Path
    ) -> str:
        """
        Build deterministic S3 key.

        Example:

        landing/historical/klines/
        symbol=BTCUSDT/
        interval=1m/
        year=2025/
        month=01/
        BTCUSDT-1m-2025-01.parquet
        """

        archive_stem = (
            archive_path.stem
        )

        parts = archive_stem.split("-")

        if len(parts) < 4:
            raise ValueError(
                f"Unexpected Binance archive filename: "
                f"{archive_path.name}"
            )

        try:

            year = int(
                parts[-2]
            )

            month = int(
                parts[-1]
            )

        except ValueError as error:

            raise ValueError(
                f"Unable to determine year/month "
                f"from archive: {archive_path.name}"
            ) from error

        parquet_filename = (
            f"{archive_stem}.parquet"
        )

        s3_key = (
            f"landing/historical/klines/"
            f"symbol={symbol}/"
            f"interval={interval}/"
            f"year={year:04d}/"
            f"month={month:02d}/"
            f"{parquet_filename}"
        )

        logger.info(
            "S3 key generated | key=%s",
            s3_key
        )

        return s3_key

    # =========================================================
    # Cleanup
    # =========================================================

    def cleanup_extraction(
        self,
        archive_path: Path
    ) -> None:
        """
        Remove temporary extraction directory.

        Original ZIP is retained.
        """

        extraction_directory = (
            archive_path.parent
            / archive_path.stem
        )

        if not extraction_directory.exists():

            logger.info(
                "No extraction directory to clean | "
                "path=%s",
                extraction_directory
            )

            return

        shutil.rmtree(
            extraction_directory
        )

        logger.info(
            "Temporary extraction directory removed | "
            "path=%s",
            extraction_directory
        )

    # =========================================================
    # Process Archive
    # =========================================================

    def process_archive(
        self,
        archive_path: str,
        symbol: str
    ) -> dict:
        """
        Process one Binance historical archive.
        """

        archive = Path(
            archive_path
        )

        # -----------------------------------------------------
        # Validate archive
        # -----------------------------------------------------

        if not archive.exists():

            raise FileNotFoundError(
                f"Historical archive not found: "
                f"{archive}"
            )

        if not archive.is_file():

            raise ValueError(
                f"Historical archive is not a file: "
                f"{archive}"
            )

        if archive.suffix.lower() != ".zip":

            raise ValueError(
                f"Expected ZIP archive | "
                f"received: {archive}"
            )

        # -----------------------------------------------------
        # Pipeline metadata
        # -----------------------------------------------------

        pipeline_run_id = str(
            uuid.uuid4()
        )

        start_time = datetime.now(
            timezone.utc
        ).isoformat()

        # -----------------------------------------------------
        # Source checksum
        # -----------------------------------------------------

        source_checksum = (
            calculate_sha256(
                file_path=str(archive)
            )
        )

        logger.info(
            "Source checksum calculated | "
            "archive=%s | sha256=%s",
            archive.name,
            source_checksum
        )

        logger.info(
            "=================================================="
        )

        logger.info(
            "Historical archive processing started"
        )

        logger.info(
            "Pipeline run ID=%s",
            pipeline_run_id
        )

        logger.info(
            "Symbol=%s | interval=%s | archive=%s",
            symbol,
            self.interval,
            archive
        )

        logger.info(
            "=================================================="
        )

        try:

            # -------------------------------------------------
            # Build deterministic S3 key
            # -------------------------------------------------

            s3_key = self.build_s3_key(
                symbol=symbol,
                interval=self.interval,
                archive_path=archive
            )

            # -------------------------------------------------
            # IDEMPOTENCY CHECK
            # -------------------------------------------------

            logger.info(
                "Checking S3 idempotency | key=%s",
                s3_key
            )

            if self.s3_client.object_exists(
                key=s3_key
            ):

                logger.info(
                    "IDEMPOTENCY CHECK | "
                    "S3 object already exists | "
                    "symbol=%s | archive=%s",
                    symbol,
                    archive.name
                )

                audit_record = (
                    create_historical_audit_record(
                        pipeline_name=(
                            "binance_historical_ingestion"
                        ),
                        symbol=symbol,
                        interval=self.interval,
                        archive_name=archive.name,
                        checksum=source_checksum,
                        records_read=0,
                        records_written=0,
                        s3_key=s3_key,
                        status=(
                            "SKIPPED_ALREADY_EXISTS"
                        ),
                        start_time=start_time,
                        pipeline_run_id=(
                            pipeline_run_id
                        )
                    )
                )

                audit_key = (
                    self.s3_client
                    .upload_historical_audit(
                        audit_record=audit_record,
                        symbol=symbol,
                        interval=self.interval,
                        archive_name=archive.name
                    )
                )

                logger.info(
                    "Skipped ingestion audit uploaded | "
                    "audit_key=%s",
                    audit_key
                )

                return {
                    "status": (
                        "SKIPPED_ALREADY_EXISTS"
                    ),
                    "symbol": symbol,
                    "interval": self.interval,
                    "archive": archive.name,
                    "records_read": 0,
                    "records_written": 0,
                    "s3_key": s3_key,
                    "checksum": source_checksum,
                    "audit_key": audit_key
                }

            # -------------------------------------------------
            # STEP 1 — Extract
            # -------------------------------------------------

            logger.info(
                "STEP 1/6 | "
                "Extracting historical archive"
            )

            extracted_files = (
                extract_archive(
                    archive_path=str(archive)
                )
            )

            logger.info(
                "Archive extraction completed | "
                "files=%s",
                len(extracted_files)
            )

            # -------------------------------------------------
            # STEP 2 — Find CSV
            # -------------------------------------------------

            logger.info(
                "STEP 2/6 | Finding CSV file"
            )

            csv_file = self.find_csv_file(
                extracted_files=extracted_files
            )

            # -------------------------------------------------
            # STEP 3 — Read CSV
            # -------------------------------------------------

            logger.info(
                "STEP 3/6 | Reading historical CSV"
            )

            records = (
                read_historical_file(
                    file_path=str(csv_file)
                )
            )

            records_read = len(
                records
            )

            logger.info(
                "Historical CSV read successfully | "
                "symbol=%s | records=%s",
                symbol,
                records_read
            )

            # -------------------------------------------------
            # STEP 4 — Validate
            # -------------------------------------------------

            logger.info(
                "STEP 4/6 | "
                "Validating historical data"
            )

            validate_historical_records(
                records=records,
                symbol=symbol,
                interval=self.interval
            )

            # -------------------------------------------------
            # STEP 5 — Normalize + Parquet
            # -------------------------------------------------

            logger.info(
                "STEP 5/6 | "
                "Normalizing and converting to Parquet"
            )

            normalized_records = (
                normalize_klines(
                    records=records,
                    symbol=symbol,
                    interval=self.interval
                )
            )

            parquet_data = (
                records_to_parquet(
                    records=normalized_records,
                    symbol=symbol,
                    interval=self.interval,
                    compression="snappy"
                )
            )

            records_written = len(
                normalized_records
            )

            logger.info(
                "Parquet conversion successful | "
                "symbol=%s | records=%s | "
                "size=%s bytes",
                symbol,
                records_written,
                len(parquet_data)
            )

            # -------------------------------------------------
            # STEP 6 — Upload
            # -------------------------------------------------

            logger.info(
                "STEP 6/6 | Uploading Parquet to S3"
            )

            self.s3_client.upload_parquet(
                parquet_data=parquet_data,
                key=s3_key
            )

            logger.info(
                "Historical Parquet uploaded successfully | "
                "symbol=%s | records=%s | s3_key=%s",
                symbol,
                records_written,
                s3_key
            )

            # -------------------------------------------------
            # SUCCESS AUDIT
            # -------------------------------------------------

            audit_record = (
                create_historical_audit_record(
                    pipeline_name=(
                        "binance_historical_ingestion"
                    ),
                    symbol=symbol,
                    interval=self.interval,
                    archive_name=archive.name,
                    checksum=source_checksum,
                    records_read=records_read,
                    records_written=records_written,
                    s3_key=s3_key,
                    status="SUCCESS",
                    start_time=start_time,
                    pipeline_run_id=pipeline_run_id
                )
            )

            audit_key = (
                self.s3_client
                .upload_historical_audit(
                    audit_record=audit_record,
                    symbol=symbol,
                    interval=self.interval,
                    archive_name=archive.name
                )
            )

            logger.info(
                "Historical ingestion audit uploaded | "
                "audit_key=%s",
                audit_key
            )

            # -------------------------------------------------
            # Cleanup
            # -------------------------------------------------

            if self.cleanup_extracted_files:

                logger.info(
                    "Cleaning up temporary "
                    "extracted files"
                )

                self.cleanup_extraction(
                    archive_path=archive
                )

            # -------------------------------------------------
            # Final result
            # -------------------------------------------------

            result = {
                "status": "SUCCESS",
                "symbol": symbol,
                "interval": self.interval,
                "archive": archive.name,
                "records_read": records_read,
                "records_written": records_written,
                "s3_key": s3_key,
                "checksum": source_checksum,
                "audit_key": audit_key
            }

            logger.info(
                "=================================================="
            )

            logger.info(
                "Historical archive processing "
                "completed successfully"
            )

            logger.info(
                "Status=%s | symbol=%s | "
                "records=%s",
                result["status"],
                symbol,
                records_written
            )

            logger.info(
                "S3 key=%s",
                s3_key
            )

            logger.info(
                "Checksum=%s",
                source_checksum
            )

            logger.info(
                "Audit key=%s",
                audit_key
            )

            logger.info(
                "=================================================="
            )

            return result

        except Exception:

            logger.exception(
                "Historical archive processing FAILED | "
                "symbol=%s | archive=%s",
                symbol,
                archive
            )

            # Keep extracted files on failure
            # for debugging.

            raise