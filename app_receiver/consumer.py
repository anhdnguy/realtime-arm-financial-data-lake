from functools import partial

from dlq import hand_off
from errors import RecordRejected
from logger import setup_logger
from retry import retry

logger = setup_logger(__name__)

async def consumer(
        queue,
        dlq_queue,
        shutdown_event,
        latest_mark,
        put_record
    ):
    """
    Send each queued trade to Kafka and advance its ticker's mark.
    put_record returns only after the broker's ack, so the mark never moves
    past a trade that isn't safely in Kafka.
    Returns on None, which main puts after the last record at shutdown.

    If a retry gives up because of shutdown, every later record is discarded
    without a try. The mark must not move past a record that never reached
    Kafka, or backfill would skip it.

    A rejected record (data fault) goes to the DLQ; any other error crashes.
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
            logger.debug(f"consumer: Try to put record to Kafka: {data}")
            status = await retry(partial(put_record, data), shutdown_event)
            if status:
                latest_mark[data["S"]] = data["t"]
            else:
                discarding = True

        except RecordRejected as e:
            logger.warning(f"consumer: Kafka rejected the record, sending it to the DLQ: {e}")
            hand_off(dlq_queue, data, e)
