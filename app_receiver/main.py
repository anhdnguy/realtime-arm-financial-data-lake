import asyncio
from asyncio.exceptions import CancelledError
import boto3
import boto3.session
from botocore.exceptions import ClientError, BotoCoreError
import json
from io import BytesIO
import pandas as pd
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
    "InternalFailureException"
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

async def retry(data, shutdown_event, put_record_kinesis):
    attempt = 1
    last_error = None
    while attempt <= MAX_ATTEMPT:
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=min(2 ** attempt, MAX_WAIT))
            return False
        except asyncio.TimeoutError:
            pass

        try:
            logger.info(f"retry: Retry attempt {attempt}...")
            await asyncio.to_thread(put_record_kinesis, data)
            return True
        except Exception as e:
            logger.info(f"retry: Retry attempt {attempt} failed: {e}")
            if is_temporary(e):
                attempt += 1
                last_error = e
            else:
                raise

    raise RuntimeError(f"retry: gave up after {MAX_ATTEMPT} tries") from last_error

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

def save_record_s3(data):
    logger.infor(f"save_record_s3: Saving record to DLQ...")
    s3 = boto3.client(
        "s3",
        endpoint_url = AppConfig.aws_endpoint_url,
        region_name = AppConfig.aws_region
    )
    bucket = AppConfig.s3_bucket
    key = f"{data['S']}_{data['t']}_{data['i']}.parquet"
    buffer = BytesIO()
    df = pd.DataFrame(list(data.items()), column=["Key", "Value"])
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    try:
        s3.upload_fileobj(buffer, bucket, key)
        logger.info(f"save_record_s3: Uploaded data to DLQ: s3://{bucket}/{key}")
    except ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == 'NoSuchBucket':
            logger.error(f"save_record_s3: Bucket not found: {bucket}")
            raise
        else:
            logger.error(f"save_record_s3: S3 upload failed ClientError: {str(e)}")
            raise
    except Exception as e:
        logger.error(
            f"save_record_s3: S3 upload failed: {str(e)}",
            f"save_record_s3: DLQ_WRITE_FAILED: {json.dumps(data)}"
        )


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
            await asyncio.to_thread(put_record_kinesis, data)
            latest_mark[data["S"]] = data["t"]
        except Exception as e:
            logger.warning(f"consumer: Failed to put record to Kinesis: {e}")
            if is_temporary(e):
                status = await retry(data, shutdown_event, put_record_kinesis)
                if status:
                    latest_mark[data["S"]] = data["t"]
                else:
                    discarding = True
            elif isinstance(e, ClientError) and e.response['Error']['Code'] == "InvalidArgumentException":
                save_record_s3(data)
                logger.error(f"consumer: Not a temporary error, logging data to DLQ: {data}")
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
        s3.upload_obj
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

asyncio.run(main())