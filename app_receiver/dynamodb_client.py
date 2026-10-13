from botocore.exceptions import ClientError, BotoCoreError

from config import AppConfig
from logger import setup_logger

logger = setup_logger(__name__)

class DynamoDBClient:
    def __init__(self, session):
        self.dynamodb = session.client(
            "dynamodb",
            endpoint_url = AppConfig.aws_endpoint_url,
            region_name = AppConfig.aws_region
        )

    def put_item(self, latest_mark):
        logger.info(f"write_dynamodb: Writing latest_mark to Dynamodb...")
        try:
            for ticker, trade_ts in latest_mark.items():
                if trade_ts:
                    self.dynamodb.put_item(
                        TableName=AppConfig.table_name,
                        Item={
                            "ticker" : {"S": ticker},
                            "trade_ts" : {"S": trade_ts}
                        }
                    )
        except ClientError:
            logger.error("write_dynamodb: Client Error")
            raise
        except BotoCoreError:
            logger.error("write_dynamodb: Network Error")
            raise