from io import BytesIO
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
        logger.infor(f"save_record_s3: Saving record to DLQ...")
        key = f"{data['S']}_{data['t']}_{data['i']}.parquet"

        buffer = BytesIO()
        df = pd.DataFrame(list(data.items()), column=["Key", "Value"])
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.s3.upload_fileobj(buffer, self.bucket, key)