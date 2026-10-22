from functools import partial

from logger import setup_logger
from retry import retry

logger = setup_logger(__name__)

async def send_to_dlq(raw, error, shutdown_event, save_record_s3):
    """
    Write one bad record to the DLQ. Returns True/False and never raises.

    On failure it logs exactly one DLQ_WRITE_FAILED line holding the raw record:
    that line is the record's only copy.
    """
    original = f"{type(error).__name__}: {error}"
    try:
        status = await retry(partial(save_record_s3, raw, error), shutdown_event)
    except Exception as e:
        dlq_error = f"{type(e).__name__}: {e}"
        if e.__cause__ is not None:
            dlq_error += f" (last error {type(e.__cause__).__name__}: {e.__cause__})"
        logger.error(f"send_to_dlq: DLQ_WRITE_FAILED | error={original} | dlq_error={dlq_error} | raw={raw}")
        return False

    if not status:
        logger.error(f"send_to_dlq: DLQ_WRITE_FAILED | error={original} | dlq_error=interrupted by shutdown | raw={raw}")
    return status
