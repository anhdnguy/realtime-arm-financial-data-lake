import os

from botocore.config import Config

TICKERS = [
    "FAKEPACA"
]

# Keep every AWS call short so a hung request can't eat Docker's stop window
# (stop_grace_period in docker-compose.yaml). total_max_attempts counts the first
# try, so boto makes at most 1 quick retry of its own; our retry() handles the rest.
BOTO_CONFIG = Config(
    connect_timeout=3,
    read_timeout=5,
    retries={"total_max_attempts": 2, "mode": "standard"},
)

# AWS_ENDPOINT_URL (LocalStack only) and the AWS keys (ECS uses a task role) may be unset.
REQUIRED_ENV = [
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
    "ALPACA_URI",
    "AWS_REGION",
    "KINESIS_STREAM_ARN",
    "STREAM_NAME",
    "DYNAMODB_TABLE_NAME",
    "S3_BUCKET",
]


class ConfigError(Exception):
    """Required configuration is missing. A setup fault: crash at startup."""


class AppConfig:
    """Application configuration loaded from env file"""

    # Alpaca API credential
    alpaca_api_key: str = os.getenv('ALPACA_API_KEY')
    alpaca_api_secret: str = os.getenv('ALPACA_API_SECRET')
    alpaca_uri: str = os.getenv('ALPACA_URI')

    # AWS: Kinesis (stream), DynamoDB (marks), S3 (DLQ)
    aws_endpoint_url: str = os.getenv("AWS_ENDPOINT_URL")
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region: str = os.getenv("AWS_REGION")
    kinesis_stream_arn: str = os.getenv("KINESIS_STREAM_ARN")
    stream_name: str = os.getenv("STREAM_NAME")
    table_name: str = os.getenv("DYNAMODB_TABLE_NAME")
    s3_bucket: str = os.getenv("S3_BUCKET")

    @staticmethod
    def validate(environ=os.environ):
        """Raise ConfigError naming every missing variable, not just the first."""
        missing = [name for name in REQUIRED_ENV if not environ.get(name)]
        if missing:
            raise ConfigError(f"missing required env vars: {', '.join(missing)}")
