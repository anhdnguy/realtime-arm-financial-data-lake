import asyncio
import boto3
import boto3.session
import signal
import sys

# Aliased: producer below is the websocket task that feeds the queue.
from confluent_kafka import Producer as KafkaProducer

from config import AppConfig, KAFKA_PRODUCER_CONFIG, TICKERS
from consumer import consumer
from dlq import DLQ_QUEUE_SIZE, dlq_worker, log_unsent
from dynamodb_client import DynamoDBClient
from kafka_client import KafkaClient
from logger import setup_logger
from producer import producer
from s3_client import S3Client
from timer import timer

logger = setup_logger(__name__)

QUEUE_SIZE = 200
# Time the consumer gets to empty the queue at shutdown. Records still queued after
# this are not lost: the marks don't cover them, so backfill fetches them again.
DRAIN_TIMEOUT = 15
# Time the DLQ worker gets to write what's left. Records still queued after this
# are logged as DLQ_WRITE_FAILED (their only copy).
DLQ_DRAIN_TIMEOUT = 5
# Worst case: 15s drain + timer write and final save (each bounded by BOTO_CONFIG,
# ~10s) + 5s DLQ drain. stop_grace_period in docker-compose.yaml (and ECS
# stopTimeout) must stay above this, or SIGKILL lands before the DLQ markers.


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


async def run(shutdown_event, queue, dlq_queue, latest_mark,
              producer, consumer, timer, dlq_worker, write_marks,
              drain_timeout=DRAIN_TIMEOUT, dlq_drain_timeout=DLQ_DRAIN_TIMEOUT):
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
      6. put None on dlq_queue    - only now: producer and consumer are both done,
                                    so no handoff can come after it
      7. wait for the DLQ worker  - at most dlq_drain_timeout; leftovers get markers
    """
    task_producer = asyncio.create_task(producer, name="producer")
    task_consumer = asyncio.create_task(consumer, name="consumer")
    task_timer = asyncio.create_task(timer, name="timer")
    task_dlq = asyncio.create_task(dlq_worker, name="dlq_worker")
    tasks = [task_producer, task_consumer, task_timer, task_dlq]

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

    # 6 + 7. Let the DLQ worker write what's left, within its deadline.
    try:
        async with asyncio.timeout(dlq_drain_timeout):
            if not task_dlq.done():
                await dlq_queue.put(None)
            await asyncio.wait([task_dlq])
        logger.info("main: DLQ worker finished")
    except TimeoutError:
        logger.error(f"main: DLQ worker did not finish within {dlq_drain_timeout}s")
        task_dlq.cancel()
        await asyncio.wait([task_dlq])
    # Normally empty. Non-empty after a DLQ timeout or a worker crash.
    lost = log_unsent(dlq_queue)
    if lost:
        logger.error(f"main: {lost} DLQ records were not written, see DLQ_WRITE_FAILED lines")

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
    s3 = S3Client(session)

    kafka = KafkaClient(
        KafkaProducer({**KAFKA_PRODUCER_CONFIG, "bootstrap.servers": AppConfig.kafka_bootstrap_servers}),
        AppConfig.kafka_topic,
    )
    # Before any task starts: a missing topic would otherwise look like a broker
    # outage and be retried for minutes. Blocks for up to METADATA_TIMEOUT, fine
    # since nothing else is running yet.
    kafka.check_topic()

    shutdown_event = asyncio.Event()
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    dlq_queue = asyncio.Queue(maxsize=DLQ_QUEUE_SIZE)
    latest_mark = dict.fromkeys(TICKERS, "")

    signals = ShutdownSignals(shutdown_event, asyncio.current_task())
    signals.install(asyncio.get_running_loop())

    try:
        return await run(
            shutdown_event,
            queue,
            dlq_queue,
            latest_mark,
            producer=producer(queue, dlq_queue),
            consumer=consumer(queue, dlq_queue, shutdown_event, latest_mark, kafka.put_record),
            timer=timer(latest_mark, shutdown_event, dynamodb.put_item),
            dlq_worker=dlq_worker(dlq_queue, shutdown_event, s3.put_dlq_record),
            write_marks=dynamodb.put_item,
        )
    except asyncio.CancelledError:
        if signals.forced:
            return 1
        raise

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
