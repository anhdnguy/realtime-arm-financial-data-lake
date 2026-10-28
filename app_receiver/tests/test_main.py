import asyncio
import threading
import time

import pytest
from botocore.exceptions import ClientError
from confluent_kafka import KafkaError, KafkaException

import timer as timer_module
from consumer import consumer
from dlq import dlq_worker
from errors import RecordRejected
from main import ShutdownSignals, run
from timer import timer


def client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "PutRecord")


def trade(n):
    return {"T": "t", "S": "FAKEPACA", "t": f"2026-10-22T01:00:0{n}Z", "p": 100 + n}


class Recorder:
    """Shared, thread-safe event log so tests can check the order of side effects."""

    def __init__(self):
        self.events = []
        self.lock = threading.Lock()

    def add(self, *event):
        with self.lock:
            self.events.append(event)


class Pipeline:
    """Builds run() with the real consumer and timer, and fake Kafka / AWS calls."""

    def __init__(self, send_delay=0.0, send_errors=None, timer_write_delay=0.0, s3_delay=0.0):
        self.shutdown = asyncio.Event()
        self.queue = asyncio.Queue(maxsize=200)
        self.dlq_queue = asyncio.Queue(maxsize=100)
        self.s3_delay = s3_delay
        self.marks = {"FAKEPACA": ""}
        self.log = Recorder()
        self.send_delay = send_delay
        self.send_errors = send_errors or {}
        self.timer_write_delay = timer_write_delay
        self.send_calls = 0

    def put_record(self, data):
        self.send_calls += 1
        time.sleep(self.send_delay)
        if self.send_calls in self.send_errors:
            raise self.send_errors[self.send_calls]
        self.log.add("kafka", data["t"])

    def timer_write(self, marks):
        self.log.add("timer_write_start", marks["FAKEPACA"])
        time.sleep(self.timer_write_delay)
        self.log.add("timer_write_end", marks["FAKEPACA"])

    def final_write(self, marks):
        self.log.add("final_save", marks["FAKEPACA"])

    def s3_write(self, raw, error):
        time.sleep(self.s3_delay)
        self.log.add("dlq", raw["t"])

    async def run(self, producer, drain_timeout=5, dlq_drain_timeout=5):
        return await run(
            self.shutdown,
            self.queue,
            self.dlq_queue,
            self.marks,
            producer=producer,
            consumer=consumer(self.queue, self.dlq_queue, self.shutdown, self.marks, self.put_record),
            timer=timer(self.marks, self.shutdown, self.timer_write),
            dlq_worker=dlq_worker(self.dlq_queue, self.shutdown, self.s3_write),
            write_marks=self.final_write,
            drain_timeout=drain_timeout,
            dlq_drain_timeout=dlq_drain_timeout,
        )


async def feed(queue, records):
    """Fake producer: queue the records, then stream nothing until cancelled."""
    for record in records:
        await queue.put(record)
    await asyncio.Event().wait()


def stop_after(shutdown, seconds):
    asyncio.get_running_loop().call_later(seconds, shutdown.set)


@pytest.fixture(autouse=True)
def slow_timer(monkeypatch):
    # Keep the timer out of the way unless a test shrinks it again.
    monkeypatch.setattr(timer_module, "SAVE_INTERVAL", 60)


async def test_graceful_stop_drains_queue_then_saves_last():
    p = Pipeline(send_delay=0.02)
    trades = [trade(n) for n in range(5)]
    # Shutdown starts while most trades are still queued.
    stop_after(p.shutdown, 0.03)

    code = await asyncio.wait_for(p.run(feed(p.queue, trades)), timeout=5)

    assert code == 0
    assert [e for e in p.log.events if e[0] == "kafka"] == [("kafka", t["t"]) for t in trades]
    assert p.log.events[-1] == ("final_save", trades[-1]["t"])


async def test_consumer_crash_still_saves_marks_and_exits_1():
    p = Pipeline(send_errors={2: KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))})

    code = await asyncio.wait_for(p.run(feed(p.queue, [trade(0), trade(1), trade(2)])), timeout=5)

    assert code == 1
    # Mark stops at the last record that reached Kafka.
    assert p.log.events[-1] == ("final_save", trade(0)["t"])


async def test_producer_crash_still_saves_marks_and_exits_1():
    p = Pipeline()

    async def broken_producer():
        await p.queue.put(trade(0))
        await asyncio.sleep(0.05)
        raise ValueError("bad frame")

    code = await asyncio.wait_for(p.run(broken_producer()), timeout=5)

    assert code == 1
    assert ("kafka", trade(0)["t"]) in p.log.events
    assert p.log.events[-1] == ("final_save", trade(0)["t"])


async def test_drain_deadline_cancels_consumer_and_still_saves(caplog):
    p = Pipeline(send_delay=0.2)
    stop_after(p.shutdown, 0.01)
    started = time.monotonic()

    code = await asyncio.wait_for(p.run(feed(p.queue, [trade(n) for n in range(5)]), drain_timeout=0.1), timeout=5)

    assert code == 0
    assert time.monotonic() - started < 1
    assert "did not drain" in caplog.text
    assert p.log.events[-1][0] == "final_save"


async def test_in_flight_timer_write_finishes_before_final_save(monkeypatch):
    monkeypatch.setattr(timer_module, "SAVE_INTERVAL", 0.01)
    p = Pipeline(timer_write_delay=0.2)
    # Shutdown lands while the timer's first write is still running.
    stop_after(p.shutdown, 0.05)

    code = await asyncio.wait_for(p.run(feed(p.queue, [])), timeout=5)

    assert code == 0
    names = [e[0] for e in p.log.events]
    assert names == ["timer_write_start", "timer_write_end", "final_save"]


async def test_failed_final_save_exits_1():
    p = Pipeline()

    def broken_final_write(marks):
        raise client_error("AccessDeniedException")

    p.final_write = broken_final_write
    stop_after(p.shutdown, 0.01)

    code = await asyncio.wait_for(p.run(feed(p.queue, [])), timeout=5)

    assert code == 1


# --- DLQ at shutdown ---

async def test_record_rejected_during_drain_still_reaches_the_dlq():
    # The last trade is rejected while the queue drains, after shutdown started.
    # The DLQ None must come after that handoff, or the record is silently lost.
    p = Pipeline(send_delay=0.02, send_errors={5: RecordRejected("record too large")})
    trades = [trade(n) for n in range(5)]
    stop_after(p.shutdown, 0.01)

    code = await asyncio.wait_for(p.run(feed(p.queue, trades)), timeout=5)

    assert code == 0
    assert ("dlq", trades[-1]["t"]) in p.log.events
    # The rejected trade never reached Kafka, so the mark stays on the one before it.
    assert ("final_save", trades[-2]["t"]) in p.log.events


async def test_dlq_drain_deadline_logs_a_marker_for_every_unwritten_record(caplog):
    p = Pipeline(send_errors={n: RecordRejected("record too large") for n in range(1, 4)}, s3_delay=0.3)
    stop_after(p.shutdown, 0.05)

    code = await asyncio.wait_for(p.run(feed(p.queue, [trade(n) for n in range(3)]), dlq_drain_timeout=0.1), timeout=5)

    assert code == 0
    written = {e[1] for e in p.log.events if e[0] == "dlq"}
    markers = [r.getMessage() for r in caplog.records if "DLQ_WRITE_FAILED" in r.getMessage()]
    # Every rejected record is in S3 or has its marker line, never neither.
    for t in [trade(n)["t"] for n in range(3)]:
        assert t in written or any(t in m for m in markers), f"{t} silently lost"
    assert any("cancelled at shutdown" in m for m in markers)
    assert any("not written before shutdown" in m for m in markers)


# --- signals ---

async def test_first_signal_is_graceful_second_forces_exit():
    shutdown = asyncio.Event()
    main_task = asyncio.create_task(asyncio.Event().wait())
    signals = ShutdownSignals(shutdown, main_task)

    signals.handle("SIGTERM")
    await asyncio.sleep(0)
    assert shutdown.is_set()
    assert not main_task.cancelled()
    assert signals.forced is False

    signals.handle("SIGTERM")
    with pytest.raises(asyncio.CancelledError):
        await main_task
    assert signals.forced is True
