import asyncio
from asyncio.exceptions import CancelledError
import boto3
import boto3.session
from botocore.exceptions import ClientError, BotoCoreError
from functools import partial
import json
import signal

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from config import AppConfig
from dynamodb_client import DynamoDBClient
from kinesis_client import KinesisClient
from logger import setup_logger
from s3_client import S3Client

logger = setup_logger(__name__)

LIST_TICKERS = [
    "FAKEPACA"
]
MAX_WAIT = 30
MAX_ATTEMPT = 10
TEMP_ERROR = set([
    "ProvisionedThroughputExceededException",
    "KMSThrottlingException",
    "InternalFailureException",
    "SlowDown",
    "InternalError",
    "RequestTimeout",
    "ServiceUnavailable"
])

def signal_handler(signum, task_producer, shutdown_event):
    """
    Handle shutdown signals (SIGTERM, SIGINT).
    
    ECS sends SIGTERM when stopping a task.
    CTRL+C sends SIGINT during local testing.
    """
    logger.info(f"shutdown_handler: Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()
    task_producer.cancel()

def is_temporary(exc):
    if isinstance(exc, BotoCoreError):
        return True

    if isinstance(exc, ClientError):
        if exc.response['Error']['Code'] in TEMP_ERROR:
            return True
        return False

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

async def producer(queue, shutdown_event):
    async def fetch_data(queue, websocket):
        while True:
            message = await websocket.recv()
            logger.info(f"producer: Recieved {message}")
            json_message = json.loads(message)
            if json_message[0]["T"] == 't':
                logger.info(f"producer: Sending message to Queue: {json_message}")
                for m in json_message:
                    await queue.put(m)

    uri = f"{AppConfig.alpaca_uri}/test"
    headers = {
        "APCA-API-KEY-ID": AppConfig.alpaca_api_key,
        "APCA-API-SECRET-KEY": AppConfig.alpaca_api_secret
    }
    msg = {
        "action": "subscribe",
        "trades": LIST_TICKERS
    }
    str_dict = json.dumps(msg)
    bytestring = bytes(str_dict, 'utf-8')

    try:
        async for websocket in connect(uri=uri, additional_headers=headers):
            try:
                await websocket.send(bytestring)
                await fetch_data(queue, websocket)

            except ConnectionClosed:
                continue
    except CancelledError:
        if shutdown_event.is_set():
            logger.info("producer: Producer received a shutdown signal, attempt to graceful shutdown...")
            await queue.put(None)
        else:
            logger.info("producer: Someone else cancelled, re raise...")
            raise

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
        logger.info(f"consumer: Received message from Queue: {data}")

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
            logger.info(f"consumer: Try to put record to Kinesis: {data}")
            status = await retry(partial(put_record_kinesis, data), shutdown_event)
            if status:
                latest_mark[data["S"]] = data["t"]
            else:
                discarding = True

        except Exception as e:
            if isinstance(e, ClientError) and e.response['Error']['Code'] == "InvalidArgumentException":
                logger.info(f"consumer: Sending data to DLQ because: {e}")
                await send_to_dlq(data, e, shutdown_event, save_record_s3)
            else:
                raise

async def timer(latest_mark, shutdown_event, write_dynamodb):
    while True:
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=5)
            break
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(write_dynamodb, latest_mark.copy())
            except ClientError:
                logger.error("timer: Update to DynamodDB - Client Error")
                continue
            except BotoCoreError:
                logger.error("timer: Update to DynamodDB - Network Error")
                continue

async def main():
    """
    Main application entry point.
    """
    session = boto3.session.Session(
        region_name = AppConfig.aws_region,
        aws_access_key_id = AppConfig.aws_access_key_id,
        aws_secret_access_key = AppConfig.aws_secret_access_key
    )
    dynamodb = DynamoDBClient(session)
    kinesis = KinesisClient(session)
    s3 = S3Client(session)
    shutdown_event = asyncio.Event()

    latest_mark = dict.fromkeys(LIST_TICKERS, "")

    queue = asyncio.Queue(maxsize=200)
    task_producer = asyncio.create_task(producer(queue, shutdown_event))
    task_consumer = asyncio.create_task(consumer(
        queue,
        shutdown_event,
        latest_mark,
        dynamodb.put_item,
        kinesis.put_record,
        s3.put_dlq_record
    ))
    task_timer = asyncio.create_task(timer(
        latest_mark,
        shutdown_event,
        dynamodb.put_item
    ))

    loop = asyncio.get_running_loop()
    
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            signal_handler,
            signame,
            task_producer,
            shutdown_event
        )
    await asyncio.gather(
        task_producer,
        task_consumer,
        task_timer
    )

if __name__ == "__main__":
    asyncio.run(main())