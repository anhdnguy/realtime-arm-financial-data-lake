import pytest
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    NoRegionError,
    ParamValidationError,
    ReadTimeoutError,
)

from errors import RecordRejected, TemporaryError
from retry import is_temporary


def client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "PutRecord")


@pytest.mark.parametrize("exc", [
    EndpointConnectionError(endpoint_url="http://localstack:4566"),
    ConnectTimeoutError(endpoint_url="http://localstack:4566"),
    ReadTimeoutError(endpoint_url="http://localstack:4566"),
    ConnectionClosedError(endpoint_url="http://localstack:4566"),
    client_error("ProvisionedThroughputExceededException"),
    client_error("KMSThrottlingException"),
    client_error("InternalFailureException"),
    client_error("SlowDown"),
    client_error("InternalError"),
    client_error("RequestTimeout"),
    client_error("ServiceUnavailable"),
], ids=lambda e: type(e).__name__ + (f"[{e.response['Error']['Code']}]" if isinstance(e, ClientError) else ""))
def test_network_and_throttling_errors_are_temporary(exc):
    assert is_temporary(exc) is True


@pytest.mark.parametrize("exc", [
    ParamValidationError(report="missing StreamName"),
    NoCredentialsError(),
    NoRegionError(),
    client_error("InvalidArgumentException"),
    client_error("AccessDeniedException"),
    client_error("ResourceNotFoundException"),
    ClientError({}, "PutRecord"),  # malformed response must not crash is_temporary
    ValueError("not an AWS error"),
], ids=lambda e: type(e).__name__ + (f"[{e.response.get('Error', {}).get('Code')}]" if isinstance(e, ClientError) else ""))
def test_setup_code_and_data_errors_are_permanent(exc):
    assert is_temporary(exc) is False


def test_app_temporary_error_is_temporary():
    # Clients that sort their own library's errors (kafka_client) raise this,
    # so retry() can retry Kafka faults without importing Kafka.
    assert is_temporary(TemporaryError("broker not reachable")) is True


def test_rejected_record_is_not_temporary():
    # A rejected record goes to the DLQ. Retrying it would only fail again.
    assert is_temporary(RecordRejected("record too large")) is False
