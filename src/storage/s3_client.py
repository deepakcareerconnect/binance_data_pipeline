import json
import boto3
import logging


logger = logging.getLogger(__name__)


class S3Client:

    def __init__(
        self,
        bucket_name: str,
        region_name: str
    ):
        self.bucket_name = bucket_name

        self.client = boto3.client(
            "s3",
            region_name=region_name
        )

    def upload_json(
        self,
        data: dict,
        key: str
    ):

        body = json.dumps(
            data,
            indent=2
        )

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )

        logger.info(
            "Uploaded JSON to s3://%s/%s",
            self.bucket_name,
            key
        )