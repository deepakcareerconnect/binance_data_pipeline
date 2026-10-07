import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class S3Client:
    """
    AWS S3 client used by the Binance data pipeline.

    Responsibilities:
    - Upload JSON objects
    - Upload Parquet objects
    - Upload quarantine records
    - Check object existence
    - Upload historical ingestion audit records
    - Store source checksum metadata on historical Parquet objects
    """

    def __init__(
        self,
        bucket_name: str,
        region_name: str
    ):
        if not bucket_name:
            raise ValueError(
                "S3 bucket name cannot be empty"
            )

        if not region_name:
            raise ValueError(
                "AWS region cannot be empty"
            )

        self.bucket_name = bucket_name
        self.region_name = region_name

        self.client = boto3.client(
            "s3",
            region_name=region_name
        )

        logger.info(
            "S3 client initialized | "
            "bucket=%s | region=%s",
            self.bucket_name,
            self.region_name
        )

    # =========================================================
    # JSON Upload
    # =========================================================

    def upload_json(
        self,
        data: dict,
        key: str
    ) -> str:
        """
        Upload a dictionary as a JSON object to S3.
        """

        if not key:
            raise ValueError(
                "S3 key cannot be empty"
            )

        body = json.dumps(
            data,
            indent=2,
            default=str
        )

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )

        logger.info(
            "S3 JSON upload successful | "
            "bucket=%s | key=%s",
            self.bucket_name,
            key
        )

        return key

    # =========================================================
    # Parquet Upload
    # =========================================================

    def upload_parquet(
        self,
        parquet_data: bytes,
        key: str,
        metadata: dict | None = None
    ) -> str:
        """
        Upload Parquet data to S3.

        Optional metadata can be attached to the S3 object.

        Example:

            metadata={
                "source-sha256": checksum,
                "pipeline-run-id": pipeline_run_id
            }
        """

        if not parquet_data:
            raise ValueError(
                "Parquet data cannot be empty"
            )

        if not key:
            raise ValueError(
                "S3 key cannot be empty"
            )

        s3_metadata = {}

        if metadata:
            s3_metadata = {
                str(key): str(value)
                for key, value in metadata.items()
                if value is not None
            }

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=parquet_data,
            ContentType="application/vnd.apache.parquet",
            Metadata=s3_metadata
        )

        logger.info(
            "S3 Parquet upload successful | "
            "bucket=%s | key=%s",
            self.bucket_name,
            key
        )

        if s3_metadata:
            logger.info(
                "S3 object metadata stored | "
                "key=%s | metadata=%s",
                key,
                s3_metadata
            )

        return key

    # =========================================================
    # Historical Parquet Upload
    # =========================================================

    def upload_historical_parquet(
        self,
        parquet_data: bytes,
        symbol: str,
        interval: str,
        year: int,
        month: int,
        source_checksum: str | None = None,
        pipeline_run_id: str | None = None
    ) -> str:
        """
        Upload a historical monthly Parquet file using
        a deterministic S3 key.

        Example:

        landing/historical/klines/
        symbol=BTCUSDT/
        interval=1m/
        year=2025/
        month=01/
        BTCUSDT-1m-2025-01.parquet
        """

        if not parquet_data:
            raise ValueError(
                "Historical Parquet data cannot be empty"
            )

        if not symbol:
            raise ValueError(
                "Symbol cannot be empty"
            )

        if not interval:
            raise ValueError(
                "Interval cannot be empty"
            )

        key = (
            f"landing/"
            f"historical/"
            f"klines/"
            f"symbol={symbol}/"
            f"interval={interval}/"
            f"year={year:04d}/"
            f"month={month:02d}/"
            f"{symbol}-{interval}-"
            f"{year:04d}-{month:02d}.parquet"
        )

        metadata = {}

        if source_checksum:
            metadata["source-sha256"] = source_checksum

        if pipeline_run_id:
            metadata["pipeline-run-id"] = pipeline_run_id

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=parquet_data,
            ContentType="application/vnd.apache.parquet",
            Metadata=metadata
        )

        logger.info(
            "Historical Parquet uploaded | "
            "bucket=%s | key=%s | "
            "checksum=%s",
            self.bucket_name,
            key,
            source_checksum
        )

        return key

    # =========================================================
    # Quarantine Upload
    # =========================================================

    def upload_quarantine(
        self,
        data,
        symbol: str,
        error_message: str
    ) -> str:
        """
        Upload failed or invalid data to the quarantine area.
        """

        timestamp = datetime.now(
            timezone.utc
        )

        date = timestamp.strftime(
            "%Y-%m-%d"
        )

        time_value = timestamp.strftime(
            "%H%M%S%f"
        )

        key = (
            f"quarantine/"
            f"rest/klines/"
            f"symbol={symbol}/"
            f"date={date}/"
            f"failed_{time_value}.json"
        )

        payload = {
            "source": "binance",
            "dataset": "klines",
            "symbol": symbol,
            "quarantined_at": timestamp.isoformat(),
            "error": error_message,
            "payload": data
        }

        self.upload_json(
            data=payload,
            key=key
        )

        logger.warning(
            "Data moved to quarantine | "
            "symbol=%s | key=%s",
            symbol,
            key
        )

        return key

    # =========================================================
    # Object Exists
    # =========================================================

    def object_exists(
        self,
        key: str
    ) -> bool:
        """
        Check whether an object already exists in S3.

        Returns:
            True  -> object exists
            False -> object does not exist
        """

        if not key:
            raise ValueError(
                "S3 key cannot be empty"
            )

        try:

            self.client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )

            logger.info(
                "S3 object exists | key=%s",
                key
            )

            return True

        except ClientError as error:

            error_code = (
                error.response
                .get("Error", {})
                .get("Code")
            )

            if error_code in (
                "404",
                "NoSuchKey",
                "NotFound"
            ):

                logger.info(
                    "S3 object does not exist | "
                    "key=%s",
                    key
                )

                return False

            logger.error(
                "S3 object existence check failed | "
                "key=%s | error=%s",
                key,
                error
            )

            raise

    # =========================================================
    # Get Object Metadata
    # =========================================================

    def get_object_metadata(
        self,
        key: str
    ) -> dict:
        """
        Retrieve S3 object metadata.

        Used for checksum-aware idempotency.
        """

        if not key:
            raise ValueError(
                "S3 key cannot be empty"
            )

        try:

            response = self.client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )

            metadata = response.get(
                "Metadata",
                {}
            )

            logger.info(
                "S3 object metadata retrieved | "
                "key=%s | metadata=%s",
                key,
                metadata
            )

            return metadata

        except ClientError as error:

            error_code = (
                error.response
                .get("Error", {})
                .get("Code")
            )

            if error_code in (
                "404",
                "NoSuchKey",
                "NotFound"
            ):

                logger.info(
                    "S3 object not found while "
                    "retrieving metadata | key=%s",
                    key
                )

                return {}

            raise

    # =========================================================
    # Historical Audit Upload
    # =========================================================

    def upload_historical_audit(
        self,
        audit_record: dict,
        symbol: str,
        interval: str,
        archive_name: str
    ) -> str:
        """
        Upload historical ingestion audit information.

        Example:

        metadata/audit/historical/
        symbol=BTCUSDT/
        interval=1m/
        date=2026-10-07/
        BTCUSDT-1m-2025-01_<run-id>.json
        """

        if not audit_record:
            raise ValueError(
                "Audit record cannot be empty"
            )

        if not symbol:
            raise ValueError(
                "Symbol cannot be empty"
            )

        if not interval:
            raise ValueError(
                "Interval cannot be empty"
            )

        if not archive_name:
            raise ValueError(
                "Archive name cannot be empty"
            )

        pipeline_run_id = audit_record.get(
            "pipeline_run_id"
        )

        if not pipeline_run_id:
            raise ValueError(
                "Audit record must contain "
                "pipeline_run_id"
            )

        now = datetime.now(
            timezone.utc
        )

        date = now.strftime(
            "%Y-%m-%d"
        )

        safe_archive_name = archive_name

        if safe_archive_name.lower().endswith(
            ".zip"
        ):
            safe_archive_name = (
                safe_archive_name[:-4]
            )

        key = (
            f"metadata/audit/historical/"
            f"symbol={symbol}/"
            f"interval={interval}/"
            f"date={date}/"
            f"{safe_archive_name}_"
            f"{pipeline_run_id}.json"
        )

        body = json.dumps(
            audit_record,
            indent=2,
            default=str
        )

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )

        logger.info(
            "Historical audit uploaded | "
            "bucket=%s | key=%s | "
            "status=%s",
            self.bucket_name,
            key,
            audit_record.get("status")
        )

        return key