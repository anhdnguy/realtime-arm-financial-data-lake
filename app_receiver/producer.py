import json

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from config import AppConfig, TICKERS
from logger import setup_logger

logger = setup_logger(__name__)

async def producer(queue):
    """
    Stream trades from Alpaca into the queue until cancelled.
    main cancels it at shutdown and then puts the None that stops the consumer.
    """
    async def fetch_data(queue, websocket):
        while True:
            message = await websocket.recv()
            logger.debug(f"producer: Received {message}")
            json_message = json.loads(message)
            if json_message[0]["T"] == 't':
                logger.debug(f"producer: Sending message to Queue: {json_message}")
                for m in json_message:
                    await queue.put(m)

    uri = f"{AppConfig.alpaca_uri}/test"
    headers = {
        "APCA-API-KEY-ID": AppConfig.alpaca_api_key,
        "APCA-API-SECRET-KEY": AppConfig.alpaca_api_secret
    }
    msg = {
        "action": "subscribe",
        "trades": TICKERS
    }
    str_dict = json.dumps(msg)
    bytestring = bytes(str_dict, 'utf-8')

    async for websocket in connect(uri=uri, additional_headers=headers):
        try:
            await websocket.send(bytestring)
            await fetch_data(queue, websocket)

        except ConnectionClosed:
            continue
