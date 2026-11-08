import asyncio
import json

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from config import AppConfig, TICKERS
from dlq import hand_off
from logger import setup_logger

logger = setup_logger(__name__)


class AlpacaError(Exception):
    """
    Alpaca sent an error message (auth failed 402, connection limit 406, ...).
    A setup fault, so the producer crashes instead of reconnecting forever.
    The producer waits before every reconnect, so Alpaca has released the old
    connection: a 406 means a second receiver is running with the same keys.
    """

    def __init__(self, code, msg):
        super().__init__(f"Alpaca error {code}: {msg}")
        self.code = code
        self.msg = msg


class Backoff:
    """
    How long to wait before reconnecting: 2s, 4s, 8s, ... up to 60s.
    Reconnecting at once got a 406 (Alpaca still counted the old connection).
    The cap keeps a long Alpaca outage from costing more than a minute of trades
    once it's back. reset() after Alpaca says "authenticated", so each new
    run of closes starts from 2s.
    """

    def __init__(self):
        self.attempt = 0
        self.cap = 60

    def next_wait(self):
        self.attempt += 1
        return min(2**self.attempt, self.cap)

    def reset(self):
        self.attempt = 0


def trade_problem(message):
    """Why a trade can't be streamed, or None if it's fine. S and t drive the mark and partition key."""
    for field in ("S", "t"):
        value = message.get(field)
        if not isinstance(value, str) or not value:
            return f"trade has missing or invalid '{field}': {value!r}"
    return None


def parse_frame(raw):
    """
    Split one websocket frame into good trades and bad records.

    Returns (trades, bad, authenticated). bad is a list of (raw record, error)
    for the DLQ. authenticated is True if any message in the frame is Alpaca's
    "authenticated" success: the connection is good, so the backoff can reset.
    - Frame is not valid JSON, or not a list: the whole frame is bad.
    - A bad trade inside a good frame: only that message is bad.
    - T == "error": raises AlpacaError.
    - success / subscription: logged. Other types (quotes, bars, ...): ignored.
    """
    if isinstance(raw, bytes):
        # The DLQ stores text; keep invalid bytes visible instead of failing on them.
        raw = raw.decode("utf-8", errors="replace")

    try:
        messages = json.loads(raw)
    except ValueError as e:
        return [], [(raw, e)], False
    if not isinstance(messages, list):
        return [], [(raw, ValueError("frame is not a JSON list"))], False

    trades, bad = [], []
    authenticated = False
    for message in messages:
        if not isinstance(message, dict) or "T" not in message:
            bad.append((message, ValueError("message is not a JSON object with a 'T' field")))
            continue

        kind = message["T"]
        if kind == "t":
            problem = trade_problem(message)
            if problem:
                bad.append((message, ValueError(problem)))
            else:
                trades.append(message)
        elif kind == "error":
            raise AlpacaError(message.get("code"), message.get("msg"))
        elif kind in ("success", "subscription"):
            logger.info(f"producer: Alpaca {kind}: {message}")
            if message.get("msg", None) == "authenticated":
                authenticated = True
        else:
            logger.debug(f"producer: ignoring message type {kind!r}")

    return trades, bad, authenticated


async def forward_frames(websocket, queue, dlq_queue, backoff):
    """
    Read frames until the connection closes: trades to the queue, bad records to the DLQ.
    Resets the backoff on "authenticated". A 406 arrives before it, so a refused
    connection never resets the wait.
    """
    while True:
        frame = await websocket.recv()
        logger.debug(f"producer: Received {frame}")
        trades, bad, authenticated = parse_frame(frame)
        if authenticated:
            backoff.reset()
        for record, error in bad:
            logger.warning(f"producer: bad record sent to the DLQ: {error}")
            hand_off(dlq_queue, record, error)
        for trade in trades:
            await queue.put(trade)


async def producer(queue, dlq_queue):
    """
    Stream trades from Alpaca into the queue until cancelled.
    main cancels it at shutdown and then puts the None that stops the consumer.
    Reconnects when the connection drops, after a wait from Backoff;
    crashes on an Alpaca error message.
    """
    uri = f"{AppConfig.alpaca_uri}/iex"
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

    backoff = Backoff()

    async for websocket in connect(uri=uri, additional_headers=headers):
        try:
            await websocket.send(bytestring)
            await forward_frames(websocket, queue, dlq_queue, backoff)

        except ConnectionClosed:
            wait_time = backoff.next_wait()
            logger.warning(f"producer: connection closed, reconnecting in {wait_time}s")
            await asyncio.sleep(wait_time)
            continue
