import boto3
import pytest

from config import REQUIRED_ENV, AppConfig, ConfigError
from dynamodb_client import DynamoDBClient
from s3_client import S3Client


def full_env():
    return {name: "x" for name in REQUIRED_ENV}


def test_validate_passes_when_everything_is_set():
    AppConfig.validate(full_env())


def test_validate_names_every_missing_var():
    env = full_env()
    del env["KAFKA_TOPIC"]
    env["S3_BUCKET"] = ""  # empty counts as missing

    with pytest.raises(ConfigError) as info:
        AppConfig.validate(env)

    assert "KAFKA_TOPIC" in str(info.value)
    assert "S3_BUCKET" in str(info.value)


def test_kafka_settings_are_required_and_kinesis_ones_are_gone():
    assert "KAFKA_BOOTSTRAP_SERVERS" in REQUIRED_ENV
    assert "KAFKA_TOPIC" in REQUIRED_ENV
    assert "KINESIS_STREAM_ARN" not in REQUIRED_ENV
    assert "STREAM_NAME" not in REQUIRED_ENV


def test_optional_vars_are_not_required():
    for name in ["AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]:
        assert name not in REQUIRED_ENV


@pytest.mark.parametrize("client_class, attr", [
    (DynamoDBClient, "dynamodb"),
    (S3Client, "s3"),
])
def test_aws_clients_use_short_timeouts(monkeypatch, client_class, attr):
    monkeypatch.setattr(AppConfig, "aws_region", "us-east-1")
    session = boto3.session.Session(
        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )

    client = getattr(client_class(session), attr)

    assert client.meta.config.connect_timeout == 3
    assert client.meta.config.read_timeout == 5
    assert client.meta.config.retries["total_max_attempts"] == 2
