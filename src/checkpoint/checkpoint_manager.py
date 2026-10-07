import json
import logging


logger = logging.getLogger(__name__)


class CheckpointManager:

    def __init__(self, s3_client):
        self.s3_client = s3_client

    def get_checkpoint(
        self,
        symbol: str,
        interval: str
    ):

        key = (
            f"metadata/checkpoints/"
            f"rest/klines/"
            f"{symbol}_{interval}.json"
        )

        try:

            response = self.s3_client.client.get_object(
                Bucket=self.s3_client.bucket_name,
                Key=key
            )

            body = response["Body"].read()

            return json.loads(
                body.decode("utf-8")
            )

        except self.s3_client.client.exceptions.NoSuchKey:

            logger.info(
                "No checkpoint found | symbol=%s",
                symbol
            )

            return None

    def save_checkpoint(
        self,
        symbol: str,
        interval: str,
        last_processed_timestamp: int
    ):

        key = (
            f"metadata/checkpoints/"
            f"rest/klines/"
            f"{symbol}_{interval}.json"
        )

        data = {
            "symbol": symbol,
            "interval": interval,
            "last_processed_timestamp":
                last_processed_timestamp
        }

        self.s3_client.upload_json(
            data=data,
            key=key
        )

        logger.info(
            "Checkpoint saved | "
            "symbol=%s | timestamp=%s",
            symbol,
            last_processed_timestamp
        )