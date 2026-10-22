from botocore.exceptions import ClientError
from functools import partial

from dlq import send_to_dlq
from logger import setup_logger
from retry import retry

logger = setup_logger(__name__)

async def consumer(
        queue,
        shutdown_event,
        latest_mark,
        put_record_kinesis,
        save_record_s3
    ):
    """
    Send each queued trade to Kinesis and advance its ticker's mark.
    Returns on None, which main puts after the last record at shutdown.

    If a retry gives up because of shutdown, every later record is discarded
    without a try. The mark must not move past a record that never reached
    Kinesis, or backfill would skip it.
    """
    counter = 0
    discarding = False
    while True:
        data = await queue.get()
        logger.debug(f"consumer: Received message from Queue: {data}")

        if data is None:
            logger.info(f"consumer: queue drained, discarded {counter} records after a retry gave up")
            return

        if discarding:
            counter += 1
            continue

        try:
            logger.debug(f"consumer: Try to put record to Kinesis: {data}")
            status = await retry(partial(put_record_kinesis, data), shutdown_event)
            if status:
                latest_mark[data["S"]] = data["t"]
            else:
                discarding = True

        except Exception as e:
            if isinstance(e, ClientError) and e.response['Error']['Code'] == "InvalidArgumentException":
                logger.warning(f"consumer: Kinesis rejected the record, sending it to the DLQ: {e}")
                await send_to_dlq(data, e, shutdown_event, save_record_s3)
            else:
                raise
