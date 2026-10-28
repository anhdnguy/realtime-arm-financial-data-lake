import os

from botocore.config import Config

TICKERS = [
    "AMD", "NVDA", "APP", "AVGO", "MU",
    "GOOGL", "MSFT", "AMZN", "TSM", "META",
    "ASML", "PLTR", "ORCL", "DELL", "PANW",
    "ARM", "CRWD", "KLAC", "ANET", "SAP",
    "MRVL", "IBM", "ISRG", "NOW", "SNOW"
]

# Keep every AWS call short so a hung request can't eat Docker's stop window
# (stop_grace_period in docker-compose.yaml). total_max_attempts counts the first
# try, so boto makes at most 1 quick retry of its own; our retry() handles the rest.
BOTO_CONFIG = Config(
    connect_timeout=3,
    read_timeout=5,
    retries={"total_max_attempts": 2, "mode": "standard"},
)

# Same job as BOTO_CONFIG, for Kafka: one put_record ends within ~10s, so it fits
# main.py's 15s drain and stop_grace_period in docker-compose.yaml.
# - message.timeout.ms: librdkafka retries a record on its own until this runs out
#   (default 300s), then reports _MSG_TIMED_OUT; our retry() handles the rest.
#   FLUSH_TIMEOUT in kafka_client must stay above it, or flush returns before the report.
# - request.timeout.ms / socket.connection.setup.timeout.ms: one hung request or
#   connect can't use up the whole window (same as BOTO_CONFIG's read/connect timeouts).
# - enable.idempotence: the broker drops copies made by librdkafka's own retries.
#   It does not catch resends from our retry() or backfill; container 2 dedupes those.
KAFKA_PRODUCER_CONFIG = {
    "enable.idempotence": True,
    "message.timeout.ms": 10000,
    "request.timeout.ms": 5000,
    "socket.connection.setup.timeout.ms": 3000
}

# AWS_ENDPOINT_URL (LocalStack only) and the AWS keys (ECS uses a task role) may be unset.
REQUIRED_ENV = [
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
    "ALPACA_URI",
    "AWS_REGION",
    "DYNAMODB_TABLE_NAME",
    "S3_BUCKET",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
]


class ConfigError(Exception):
    """Required setup is missing (an env var, the Kafka topic). A setup fault: crash at startup."""


class AppConfig:
    """Application configuration loaded from env file"""

    # Alpaca API credential
    alpaca_api_key: str = os.getenv('ALPACA_API_KEY')
    alpaca_api_secret: str = os.getenv('ALPACA_API_SECRET')
    alpaca_uri: str = os.getenv('ALPACA_URI')

    # AWS: DynamoDB (marks), S3 (DLQ)
    aws_endpoint_url: str = os.getenv("AWS_ENDPOINT_URL")
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region: str = os.getenv("AWS_REGION")
    table_name: str = os.getenv("DYNAMODB_TABLE_NAME")
    s3_bucket: str = os.getenv("S3_BUCKET")

    # Kafka (the trade stream)
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic: str = os.getenv("KAFKA_TOPIC")

    @staticmethod
    def validate(environ=os.environ):
        """Raise ConfigError naming every missing variable, not just the first."""
        missing = [name for name in REQUIRED_ENV if not environ.get(name)]
        if missing:
            raise ConfigError(f"missing required env vars: {', '.join(missing)}")
