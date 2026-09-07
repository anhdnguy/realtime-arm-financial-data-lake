import os
import asyncio
import json
from dotenv import load_dotenv

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

load_dotenv()

async def producer(queue):
    async def fetch_data(queue, websocket):
        while True:
            message = await websocket.recv()
            print(f"fetch_data: Received: {message}")
            await queue.put(message)
            print(f"fetch_data: Done sending {message} to Queue")
            

    uri = "wss://stream.data.alpaca.markets/v2/test"
    headers = {
        "APCA-API-KEY-ID": os.getenv('API_KEY'),
        "APCA-API-SECRET-KEY": os.getenv('API_SECRET')
    }
    msg = {
        "action": "subscribe",
        "trades": [
            "FAKEALPACA"
        ]
    }
    str_dict = json.dumps(msg)
    bytestring = bytes(str_dict, 'utf-8')
    async for websocket in connect(uri=uri, additional_headers=headers):
        try:
            print("producer: Sending message to Alpaca...")
            await websocket.send(bytestring)
            print("producer: Done sending message to Alpaca...")
            await fetch_data(queue, websocket)
        except ConnectionClosed:
            continue

def _write_record(data):
    with open("data.txt", "a") as f:
        f.write(data)

async def consumer(queue):
    while True:
        data = await queue.get()
        if data is None:
            continue
        _write_record(data)
        # print(f"consumer: Get from queue: {data}")


async def main():
    queue = asyncio.Queue()
    await asyncio.gather(
        producer(queue),
        consumer(queue)
    )

if __name__ == "__main__":
    asyncio.run(main())