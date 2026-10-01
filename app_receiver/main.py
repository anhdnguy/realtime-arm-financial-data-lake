import asyncio
from asyncio.exceptions import CancelledError
import boto3
import boto3.session
from botocore.exceptions import ClientError
import json
import signal

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from config import AppConfig
from logger import setup_logger

logger = setup_logger(__name__)

LIST_TICKERS = ["FAKEPACA"]

def signal_handler(signum, task_producer):
    """
    Handle shutdown signals (SIGTERM, SIGINT).
    
    ECS sends SIGTERM when stopping a task.
    CTRL+C sends SIGINT during local testing.
    """
    logger.info(f"shutdown_handler: Received signal {signum}, initiating graceful shutdown...")
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

async def consumer(queue):
    latest_mark = dict.fromkeys(LIST_TICKERS, "")
    while True:
        data = await queue.get()
        logger.info(f"consumer: Received message from Queue: {data}")
        if data is None:
            await write_dynamodb(latest_mark)
            break
        try:
            await asyncio.to_thread(put_record_kinesis, data)
            latest_mark[data["S"]] = data["t"]
        except:
            logger.error(f"consumer: Failed to put_record to kinesis: {data}")

async def write_dynamodb(latest_mark = None):
    dynamodb = boto3.client('dynamodb', region_name=AppConfig.aws_region)
    try:
        dynamodb.update_item(
            TableName=AppConfig.table_name,
            Key={
                "id": AppConfig.mark_id
            },
            UpdateExpression="SET dict = :m",
            ExpressionAttributeValues={":m": latest_mark}
        )
    except ClientError:
        raise ClientError(f"Dynamodb Update Failed")

async def main():
    """
    Main application entry point.
    """
    queue = asyncio.Queue(maxsize=200)
    task_producer = asyncio.create_task(producer(queue))
    task_consumer = asyncio.create_task(consumer(queue))
    task_write_dynamodb = asyncio.create_task()

    loop = asyncio.get_running_loop()
    
    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            signal_handler,
            signame,
            task_producer
        )
    await asyncio.gather(
        task_producer,
        task_consumer,
        task_write_dynamodb
    )

asyncio.run(main())