import json

from config import AppConfig
from logger import setup_logger

logger = setup_logger(__name__)

class KinesisClient:
    def __init__(self, session):
        self.kinesis = session.client(
            "kinesis",
            endpoint_url = AppConfig.aws_endpoint_url,
            region_name = AppConfig.aws_region
        )

    def put_record(self, data):
        logger.info(f"put_record_kinesis: Putting record to Kinesis...")
        response = self.kinesis.put_record(
            StreamName = AppConfig.stream_name,
            Data = json.dumps(data).encode('utf-8'),
            PartitionKey = data['S'],
            StreamARN = AppConfig.kinesis_stream_arn
        )

        logger.info(f"put_record_kinesis: Done putting records to Kinesis: {response}")
        return response