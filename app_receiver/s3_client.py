from botocore.exceptions import ClientError, BotoCoreError
from io import BytesIO
import json
import pandas as pd

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

    def upload_obj(self, data):
        logger.info(f"save_record_s3: Saving record to DLQ...")
        key = f"{data['S']}_{data['t']}_{data['i']}.parquet"

        buffer = BytesIO()
        df = pd.DataFrame(list(data.items()), column=["Key", "Value"])
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        try:
            self.s3.upload_fileobj(buffer, self.bucket, key)
            logger.info(f"save_record_s3: Uploaded data to DLQ: s3://{self.bucket}/{key}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
        
            if error_code == 'NoSuchBucket':
                logger.error(f"save_record_s3: Bucket not found: {self.bucket}")
                raise
            else:
                logger.error(f"save_record_s3: S3 upload failed ClientError: {str(e)}")
                raise
        except Exception as e:
            logger.error(
                f"save_record_s3: S3 upload failed: {str(e)}",
                f"save_record_s3: DLQ_WRITE_FAILED: {json.dumps(data)}"
            )