import asyncio
import boto3
import boto3.session
import functools
import json
import signal

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from app_receiver.config import AppConfig
from app_receiver.logger import setup_logger

logger = setup_logger(__name__)

def signal_handler(signum, loop):
    """
    Handle shutdown signals (SIGTERM, SIGINT).
    
    ECS sends SIGTERM when stopping a task.
    CTRL+C sends SIGINT during local testing.
    """
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")

    loop.stop()
    # Note: Don't sys.exit() here - let the main loop finish current work

async def producer(queue):
    async def fetch_data(queue, websocket):
        while True:
            message = await websocket.recv()
            print(f"producer: Recieved {message}")
            json_message = json.loads(message)
            if json_message[0]["T"] == 't':
                print(f"producer: Sending message to Queue: {json_message[0]}")
                await queue.put(json_message[0])            

    uri = "wss://stream.data.alpaca.markets/v2/test"
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
    async for websocket in connect(uri=uri, additional_headers=headers):
        try:
            await websocket.send(bytestring)
            await fetch_data(queue, websocket)
        except ConnectionClosed:
            continue

def put_record_kinesis(data):
    print(f"put_record_kinesis: Putting record to Kinesis...")
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

    print(f"put_record_kinesis: Done putting records to Kinesis: {response}")
    return response

async def consumer(queue):
    while True:
        data = await queue.get()
        print(f"consumer: Received message from Queue: {data}")
        if data is None:
            continue
        await asyncio.to_thread(put_record_kinesis, data)

async def start_container():
    """
    Start the main processing loop with shutdown support.
    """
    queue = asyncio.Queue()
    await asyncio.gather(
        producer(queue),
        consumer(queue)
    )

async def main():
    """
    Main application entry point.
    """
    loop = asyncio.get_running_loop()

    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            functools.partial(signal_handler, signame, loop))

    await start_container()

asyncio.run(main())