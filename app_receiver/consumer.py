import asyncio
from botocore.exceptions import ClientError, BotoCoreError
from functools import partial

from dlq import send_to_dlq
from logger import setup_logger
from retry import retry

logger = setup_logger(__name__)

async def consumer(
        queue,
        shutdown_event,
        latest_mark,
        write_dynamodb,
        put_record_kinesis,
        save_record_s3
    ):
    counter = 0
    discarding = False
    while True:
        data = await queue.get()
        logger.debug(f"consumer: Received message from Queue: {data}")

        if data is None:
            try:
                logger.info("consumer: Attempt to save before shutdown...")
                await asyncio.to_thread(write_dynamodb, latest_mark.copy())
            except (ClientError, BotoCoreError) as e:
                logger.error(f"consumer: Last save failed: {e}")
            logger.info(f"consumer: Discard count: {counter}")
            break

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
