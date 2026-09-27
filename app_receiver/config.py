import os

class AppConfig:
    """Application configuration loaded from env file"""

    # Alaca API credential
    alpaca_api_key: str = os.getenv('ALPACA_API_KEY')
    alpaca_api_secret: str = os.getenv('ALPACA_API_SECRET')
    alpaca_uri: str = "wss://stream.data.alpaca.markets/v2"

    # S3 / Delta Lake — optional; only required for market_data_pipeline write layer
    aws_endpoint_url: str = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    delta_table_uri: str = os.getenv("DELTA_TABLE_URI", "s3://ohlcv/prices")
    kinesis_stream_arn: str = os.getenv("KINESIS_STREAM_ARN")
    stream_name: str = os.getenv("STREAM_NAME")
