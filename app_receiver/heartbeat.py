"""
Heartbeat: one small record to the heartbeat topic every HEARTBEAT_INTERVAL
seconds, so the watcher can tell the receiver is alive and reaching Kafka,
even when the market is closed and no trades flow.

  started_at              changes on every restart: the watcher counts restarts
  sent_at                 for a human reading the topic (the watcher uses arrival time)
  last_alpaca_message_at  any frame from Alpaca; shown, never alerted on (stale every weekend)
"""
import asyncio
from datetime import datetime, timezone

from errors import TemporaryError
from logger import setup_logger

logger = setup_logger(__name__)

HEARTBEAT_INTERVAL = 60
# Names the sender, like the ticker for trades.
HEARTBEAT_KEY = "receiver"


def utc_text(moment):
    """A UTC datetime as text in Alpaca's style (2026-11-15T01:02:03Z), None stays None."""
    if moment is None:
        return None
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


async def heartbeat(started_at, alpaca_status, put_record):
    """
    Send a beat at once, then every HEARTBEAT_INTERVAL seconds, until main cancels it.

    started_at:    the receiver's start time, already as text
    alpaca_status: dict shared with the producer, which sets "last_message_at"
                   (a UTC datetime) on every frame
    put_record:    a KafkaClient.put_record on the heartbeat's own producer

    - TemporaryError (Kafka down or slow): drop the beat, log a WARNING, keep going.
      No retry: a late beat is useless, and the watcher should see the gap.
    - Anything else: raise and crash. The topic is checked at startup, so this
      is a setup fault or a bug.
    """
    while True:
        beat = {
            "started_at": started_at,
            "sent_at": utc_text(datetime.now(timezone.utc)),
            "last_alpaca_message_at": utc_text(alpaca_status["last_message_at"]),
        }
        try:
            # Blocks up to FLUSH_TIMEOUT (12s) when Kafka is down, so off the event loop.
            await asyncio.to_thread(put_record, HEARTBEAT_KEY, beat)
            logger.debug(f"heartbeat: sent {beat}")
        except TemporaryError as e:
            logger.warning(f"heartbeat: heartbeat dropped, Kafka unavailable: {e}")

        await asyncio.sleep(HEARTBEAT_INTERVAL)
