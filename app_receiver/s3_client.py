from datetime import datetime, timezone
import json
import uuid

from config import AppConfig
from logger import setup_logger

logger = setup_logger(__name__)

class S3Client:
    def __init__(self, session):
        self.s3 = session.client(
            "s3",
            endpoint_url = AppConfig.aws_endpoint_url,
            region_name = AppConfig.aws_region
        )
        self.bucket = AppConfig.s3_bucket

    def put_dlq_record(self, raw, error):
        if not isinstance(raw, int):
            raw = json.dumps(raw)

        now = datetime.now(timezone.utc)
        failure_time = now.strftime("%Y%m%dT%H%M%S%f")
        key = f"dlq/{now.year}/{now.month}/{now.day}/{failure_time}_{uuid.uuid4()}.txt"
        body = f"failed_at: {failure_time} \nerror: {error} \nraw: {raw}"
        self.s3.put_object(
            self.bucket,
            key,
            body.encode('utf-8')
        )
        return key