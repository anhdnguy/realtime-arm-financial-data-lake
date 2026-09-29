import asyncio
from asyncio.exceptions import CancelledError
import boto3
import boto3.session
import json
import signal

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from config import AppConfig
from logger import setup_logger

logger = setup_logger(__name__)

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
                logger.info(f"producer: Sending message to Queue: {json_message[0]}")
                await queue.put(json_message[0])            

    uri = f"{AppConfig.alpaca_uri}/test"
    headers = {
        "APCA-API-KEY-ID": AppConfig.alpaca_api_key,
        "APCA-API-SECRET-KEY": AppConfig.alpaca_api_secret
    }
    msg = {
        "action": "subscribe",
        "trades": [
            "FAKEPACA"
        ]
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
    while True:
        data = await queue.get()
        logger.info(f"consumer: Received message from Queue: {data}")
        if data is None:
            break
        await asyncio.to_thread(put_record_kinesis, data)
        await asyncio.sleep(10)

async def main():
    """
    Main application entry point.
    """
    queue = asyncio.Queue()
    task_producer = asyncio.create_task(producer(queue))
    task_consumer = asyncio.create_task(consumer(queue))

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
        task_consumer
    )

asyncio.run(main())