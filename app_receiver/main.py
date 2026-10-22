import asyncio
import boto3
import boto3.session
import signal
import sys

from config import AppConfig, TICKERS
from consumer import consumer
from dynamodb_client import DynamoDBClient
from kinesis_client import KinesisClient
from logger import setup_logger
from producer import producer
from s3_client import S3Client
from timer import timer

logger = setup_logger(__name__)

QUEUE_SIZE = 200
# Time the consumer gets to empty the queue at shutdown. Records still queued after
# this are not lost: the marks don't cover them, so backfill fetches them again.
# Must fit, with the final save, inside stop_grace_period (docker-compose.yaml).
DRAIN_TIMEOUT = 15


class ShutdownSignals:
    """
    SIGTERM (ECS / docker stop) or SIGINT (Ctrl+C).

    First signal: graceful shutdown (set the event; run() does the rest).
    Second signal: forced exit. Cancel main straight away and skip the final
    save. Safe direction: the marks are older, so a restart sends duplicates, never gaps.
    """

    def __init__(self, shutdown_event, main_task):
        self.shutdown_event = shutdown_event
        self.main_task = main_task
        self.forced = False

    def handle(self, signame):
        if not self.shutdown_event.is_set():
            logger.info(f"main: received {signame}, shutting down gracefully (send again to force)")
            self.shutdown_event.set()
        elif not self.forced:
            logger.warning(f"main: received {signame} again, forced exit without the final save")
            self.forced = True
            self.main_task.cancel()

    def install(self, loop):
        for signame in ("SIGINT", "SIGTERM"):
            loop.add_signal_handler(getattr(signal, signame), self.handle, signame)


async def run(shutdown_event, queue, latest_mark, producer, consumer, timer, write_marks,
              drain_timeout=DRAIN_TIMEOUT):
    """
    Run the pipeline until a shutdown signal or until any task ends (a crash),
    then stop everything in order. Returns the exit code: 0 clean, 1 crash.

    Shutdown order, and why:
      1. cancel the producer      - nothing new enters the queue
      2. put None on the queue    - after the last record, so nothing is skipped
      3. wait for the consumer    - drains the queue, at most drain_timeout
      4. wait for the timer       - an in-flight timer write must finish before the
                                    final save, or an older mark could land last
      5. final mark save          - also runs after a crash, marks are still good
    """
    task_producer = asyncio.create_task(producer, name="producer")
    task_consumer = asyncio.create_task(consumer, name="consumer")
    task_timer = asyncio.create_task(timer, name="timer")
    tasks = [task_producer, task_consumer, task_timer]

    task_signal = asyncio.create_task(shutdown_event.wait(), name="shutdown_signal")
    await asyncio.wait([*tasks, task_signal], return_when=asyncio.FIRST_COMPLETED)
    task_signal.cancel()

    if not shutdown_event.is_set():
        ended = ", ".join(t.get_name() for t in tasks if t.done())
        logger.error(f"main: {ended} stopped unexpectedly, shutting down")
    # Also tells retry() and the timer to stop waiting.
    shutdown_event.set()

    # 1. Stop the producer.
    task_producer.cancel()
    await asyncio.wait([task_producer])

    # 2 + 3. Let the consumer drain, within the deadline.
    try:
        async with asyncio.timeout(drain_timeout):
            if not task_consumer.done():
                await queue.put(None)
            await asyncio.wait([task_consumer])
        logger.info("main: consumer finished")
    except TimeoutError:
        logger.error(
            f"main: consumer did not drain within {drain_timeout}s, "
            f"{queue.qsize()} records left in the queue (backfill covers them)"
        )
        task_consumer.cancel()
        await asyncio.wait([task_consumer])

    # 4. Let an in-flight timer write finish before the final save.
    await asyncio.wait([task_timer])

    # 5. Final mark save.
    exit_code = 0
    try:
        await asyncio.to_thread(write_marks, latest_mark.copy())
        logger.info(f"main: final marks saved: {latest_mark}")
    except Exception as e:
        logger.error(f"main: final mark save failed ({type(e).__name__}: {e}), restart will resend from older marks")
        exit_code = 1

    for task in tasks:
        if not task.cancelled() and task.exception() is not None:
            logger.error(f"main: {task.get_name()} crashed", exc_info=task.exception())
            exit_code = 1

    logger.info(f"main: shutdown complete, exit code {exit_code}")
    return exit_code


async def main():
    """
    Main application entry point. Returns the process exit code.
    """
    AppConfig.validate()

    session = boto3.session.Session(
        region_name = AppConfig.aws_region,
        aws_access_key_id = AppConfig.aws_access_key_id,
        aws_secret_access_key = AppConfig.aws_secret_access_key
    )
    dynamodb = DynamoDBClient(session)
    kinesis = KinesisClient(session)
    s3 = S3Client(session)

    shutdown_event = asyncio.Event()
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    latest_mark = dict.fromkeys(TICKERS, "")

    signals = ShutdownSignals(shutdown_event, asyncio.current_task())
    signals.install(asyncio.get_running_loop())

    try:
        return await run(
            shutdown_event,
            queue,
            latest_mark,
            producer=producer(queue),
            consumer=consumer(queue, shutdown_event, latest_mark, kinesis.put_record, s3.put_dlq_record),
            timer=timer(latest_mark, shutdown_event, dynamodb.put_item),
            write_marks=dynamodb.put_item,
        )
    except asyncio.CancelledError:
        if signals.forced:
            return 1
        raise

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
