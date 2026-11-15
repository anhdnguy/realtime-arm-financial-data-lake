import asyncio
from datetime import datetime, timezone

import pytest
from confluent_kafka import KafkaError, KafkaException

import heartbeat as heartbeat_module
from errors import TemporaryError
from heartbeat import HEARTBEAT_KEY, heartbeat

STARTED_AT = "2026-11-15T01:00:00Z"


class Sender:
    """Fake put_record: records each beat, raises the given errors in order."""

    def __init__(self, *errors):
        self.errors = list(errors)
        self.beats = []
        self.keys = []

    def __call__(self, key, data):
        self.keys.append(key)
        self.beats.append(data)
        if self.errors:
            raise self.errors.pop(0)


@pytest.fixture(autouse=True)
def fast_beats(monkeypatch):
    monkeypatch.setattr(heartbeat_module, "HEARTBEAT_INTERVAL", 0.01)


async def run_for(seconds, *args):
    """Run the heartbeat for a while, then cancel it like main does at shutdown."""
    task = asyncio.create_task(heartbeat(*args))
    await asyncio.sleep(seconds)
    task.cancel()
    await asyncio.wait([task])
    return task


async def test_first_beat_goes_out_at_startup(monkeypatch):
    # A crash loop that dies within a minute must still leave a beat behind.
    monkeypatch.setattr(heartbeat_module, "HEARTBEAT_INTERVAL", 60)
    sender = Sender()

    await run_for(0.05, STARTED_AT, {"last_message_at": None}, sender)

    [beat] = sender.beats
    assert sender.keys == [HEARTBEAT_KEY] == ["receiver"]
    assert beat["started_at"] == STARTED_AT
    assert beat["last_alpaca_message_at"] is None  # nothing from Alpaca yet
    assert beat["sent_at"].endswith("Z")


async def test_beats_repeat_and_carry_the_latest_alpaca_time():
    status = {"last_message_at": None}
    sender = Sender()
    task = asyncio.create_task(heartbeat(STARTED_AT, status, sender))
    await asyncio.sleep(0.005)

    status["last_message_at"] = datetime(2026, 11, 15, 1, 2, 3, tzinfo=timezone.utc)
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.wait([task])

    assert len(sender.beats) >= 3
    assert sender.beats[0]["last_alpaca_message_at"] is None
    assert sender.beats[-1]["last_alpaca_message_at"] == "2026-11-15T01:02:03Z"
    assert all(b["started_at"] == STARTED_AT for b in sender.beats)


async def test_temporary_error_drops_the_beat_and_keeps_going(caplog):
    # Kafka down: no retry (a late beat is useless), no crash. The watcher sees the gap.
    sender = Sender(TemporaryError("Kafka send failed: Local: Message timed out"))

    task = await run_for(0.05, STARTED_AT, {"last_message_at": None}, sender)

    assert task.cancelled()  # still running when main stopped it
    assert len(sender.beats) >= 2  # the beat after the failure still went out
    assert "heartbeat dropped" in caplog.text


async def test_other_errors_crash():
    # Topic is checked at startup, so anything else is a setup fault or a bug.
    error = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    sender = Sender(error)

    with pytest.raises(KafkaException):
        await asyncio.wait_for(heartbeat(STARTED_AT, {"last_message_at": None}, sender), timeout=1)

    assert len(sender.beats) == 1
