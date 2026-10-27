from datetime import datetime, timezone
import json
import uuid

from config import AppConfig, BOTO_CONFIG
from logger import setup_logger

logger = setup_logger(__name__)

class S3Client:
    def __init__(self, session):
        self.s3 = session.client(
            "s3",
            endpoint_url = AppConfig.aws_endpoint_url,
            region_name = AppConfig.aws_region,
            config = BOTO_CONFIG
        )
        self.bucket = AppConfig.s3_bucket

    def put_dlq_record(self, raw, error):
        if not isinstance(raw, str):
            raw = json.dumps(raw)

        now = datetime.now(timezone.utc)
        failure_time = now.strftime("%Y%m%dT%H%M%S.%fZ")
        key = f"dlq/{now.strftime("%Y/%m/%d")}/{failure_time}_{uuid.uuid4()}.txt"
        body = "\n".join([
            f"failed_at: {failure_time}",
            f"error_type: {type(error).__name__}",
            f"error_message: {error}",
            f"raw: {raw}",
        ])
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode('utf-8'),
            ContentType="text/plain; charset=utf-8"
        )

        logger.info(f"put_dlq_record: Record is saved to DLQ: {key}")
        return key