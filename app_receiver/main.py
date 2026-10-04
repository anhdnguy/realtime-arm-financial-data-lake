import asyncio
from asyncio.exceptions import CancelledError
import boto3
import boto3.session
from botocore.exceptions import ClientError, BotoCoreError
import json
import signal

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from config import AppConfig
from logger import setup_logger

logger = setup_logger(__name__)

LIST_TICKERS = ["FAKEPACA"]

def signal_handler(signum, task_producer, event):
    """
    Handle shutdown signals (SIGTERM, SIGINT).
    
    ECS sends SIGTERM when stopping a task.
    CTRL+C sends SIGINT during local testing.
    """
    logger.info(f"shutdown_handler: Received signal {signum}, initiating graceful shutdown...")
    event.set()
    task_producer.cancel()

async def producer(queue):
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
        logger.info("producer: Producer received a shutdown signal, attempt to graceful shutdown...")
        await queue.put(None)

def put_record_kinesis(data):
    logger.info(f"put_record_kinesis: Putting record to Kinesis...")
    session = boto3.session.Session(
        region_name = AppConfig.aws_region,
        aws_access_key_id = AppConfig.aws_access_key_id,
        aws_secret_access_key = AppConfig.aws_secret_access_key
    )
    kinesis = session.client(
        "kinesis",
        endpoint_url = AppConfig.aws_endpoint_url,
        region_name = AppConfig.aws_region
    )

    response = kinesis.put_record(
        StreamName = AppConfig.stream_name,
        Data = json.dumps(data).encode('utf-8'),
        PartitionKey = data['S'],
        StreamARN = AppConfig.kinesis_stream_arn
    )

    logger.info(f"put_record_kinesis: Done putting records to Kinesis: {response}")
    return response

async def consumer(queue, latest_mark):
    while True:
        data = await queue.get()
        logger.info(f"consumer: Received message from Queue: {data}")
        if data is None:
            await asyncio.to_thread(write_dynamodb, latest_mark.copy())
            break
        try:
            await asyncio.to_thread(put_record_kinesis, data)
            latest_mark[data["S"]] = data["t"]
        except:
            logger.error(f"consumer: Failed to put_record to kinesis: {data}")

def write_dynamodb(latest_mark):
    logger.info(f"write_dynamodb: Writing latest_mark to Dynamodb...")
    dynamodb = boto3.resource(
        'dynamodb',
        endpoint_url = AppConfig.aws_endpoint_url,
        region_name=AppConfig.aws_region)
    table = dynamodb.Table(AppConfig.table_name)
    try:
        for ticker, trade_ts in latest_mark.items():
            if trade_ts:
                table.put_item(
                    Item = {
                        "ticker": ticker,
                        "trade_ts": trade_ts
                    }
                )
    except ClientError:
        logger.error("write_dynamodb: Client Error")
        raise
    except BotoCoreError:
        logger.error("write_dynamodb: Network Error")
        raise

async def timer(latest_mark, event):
    while True:
        try:
            await asyncio.wait_for(event.wait(), timeout=5)
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
    event = asyncio.Event()

    latest_mark = dict.fromkeys(LIST_TICKERS, "")

    queue = asyncio.Queue(maxsize=200)
    task_producer = asyncio.create_task(producer(queue))
    task_consumer = asyncio.create_task(consumer(queue, latest_mark))
    task_timer = asyncio.create_task(timer(latest_mark, event))

    loop = asyncio.get_running_loop()
    
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            signal_handler,
            signame,
            task_producer,
            event
        )
    await asyncio.gather(
        task_producer,
        task_consumer,
        task_timer
    )

asyncio.run(main())