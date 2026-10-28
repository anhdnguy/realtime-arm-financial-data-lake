import asyncio

from botocore.exceptions import ClientError, HTTPClientError
from botocore.exceptions import ConnectionError as BotoConnectionError

from errors import TemporaryError
from logger import setup_logger

logger = setup_logger(__name__)

MAX_WAIT = 30
MAX_ATTEMPT = 10
TEMP_ERROR = set([
    "ThrottlingException", "RequestLimitExceeded", "InternalServerError", # DynamoDB
    "ProvisionedThroughputExceededException", # DynamoDB
    "SlowDown", "InternalError", # S3
    "RequestTimeout",
    "ServiceUnavailable"
])

def is_temporary(exc):
    """
    True when a retry can help: network trouble, or AWS asking us to slow down.
    Non-AWS clients (kafka_client) sort their own errors and raise TemporaryError.

    Every other BotoCoreError (ParamValidationError, NoCredentialsError,
    NoRegionError, ...) is a setup or code fault. Retrying it only delays the crash.
    """
    if isinstance(exc, TemporaryError):
        return True

    if isinstance(exc, (BotoConnectionError, HTTPClientError)):
        return True

    if isinstance(exc, ClientError):
        return exc.response.get('Error', {}).get('Code') in TEMP_ERROR

    return False

class RetryExhausted(Exception):
    """Every try failed with a temporary error. The last error is in __cause__."""


async def retry(func, shutdown_event):
    """
    Run func (a blocking call that takes no arguments) in a thread until it works.

    - Returns True once func succeeds.
    - Returns False if shutdown starts while waiting between tries.
    - Raises a permanent error straight away.
    - Raises RetryExhausted after 1 first try + MAX_ATTEMPT retries.

    The first try always runs, even during shutdown, so the queue can still drain.
    Only the waits between tries listen for shutdown.
    """
    for attempt in range(MAX_ATTEMPT + 1):
        try:
            await asyncio.to_thread(func)
            return True
        except Exception as e:
            if not is_temporary(e):
                raise
            if attempt == MAX_ATTEMPT:
                raise RetryExhausted(f"gave up after {MAX_ATTEMPT + 1} tries") from e

            wait = min(2 ** (attempt + 1), MAX_WAIT)
            log = logger.warning if attempt == 0 else logger.info
            log(f"retry: try {attempt + 1} failed ({type(e).__name__}: {e}), next try in {wait}s")

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=wait)
                logger.warning("retry: shutdown started during the wait, giving up")
                return False
            except asyncio.TimeoutError:
                pass
