import os
import asyncio
import json
import boto3
import boto3.session
import json
from dotenv import load_dotenv

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

load_dotenv()

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
        "APCA-API-KEY-ID": os.getenv('ALPACA_API_KEY'),
        "APCA-API-SECRET-KEY": os.getenv('ALPACA_API_SECRET')
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
        region_name = os.getenv('AWS_REGION'),
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    kinesis = session.client(
        "kinesis",
        endpoint_url = os.getenv('AWS_ENDPOINT_URL'),
        region_name = os.getenv('AWS_REGION')
    )

    response = kinesis.put_record(
        StreamName = 'test-stream',
        Data = json.dumps(data).encode('utf-8'),
        PartitionKey = data['S'],
        StreamARN = os.getenv('KINESIS_STREAM_ARN')
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
        

async def main():
    queue = asyncio.Queue()
    await asyncio.gather(
        producer(queue),
        consumer(queue)
    )

if __name__ == "__main__":
    asyncio.run(main())