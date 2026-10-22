from config import AppConfig, BOTO_CONFIG
from logger import setup_logger

logger = setup_logger(__name__)

class DynamoDBClient:
    def __init__(self, session):
        self.dynamodb = session.client(
            "dynamodb",
            endpoint_url = AppConfig.aws_endpoint_url,
            region_name = AppConfig.aws_region,
            config = BOTO_CONFIG
        )

    def put_item(self, latest_mark):
        # Errors go up to the caller (timer / final save), which logs them with detail.
        logger.debug("write_dynamodb: Writing latest_mark to Dynamodb...")
        for ticker, trade_ts in latest_mark.items():
            if trade_ts:
                self.dynamodb.put_item(
                    TableName=AppConfig.table_name,
                    Item={
                        "ticker" : {"S": ticker},
                        "trade_ts" : {"S": trade_ts}
                    }
                )
