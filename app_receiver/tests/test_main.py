import asyncio
import threading
import time

import pytest
from botocore.exceptions import ClientError

import timer as timer_module
from consumer import consumer
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
    """Builds run() with the real consumer and timer and fake AWS calls."""

    def __init__(self, kinesis_delay=0.0, kinesis_errors=None, timer_write_delay=0.0):
        self.shutdown = asyncio.Event()
        self.queue = asyncio.Queue(maxsize=200)
        self.marks = {"FAKEPACA": ""}
        self.log = Recorder()
        self.kinesis_delay = kinesis_delay
        self.kinesis_errors = kinesis_errors or {}
        self.timer_write_delay = timer_write_delay
        self.kinesis_calls = 0

    def put_record(self, data):
        self.kinesis_calls += 1
        time.sleep(self.kinesis_delay)
        if self.kinesis_calls in self.kinesis_errors:
            raise self.kinesis_errors[self.kinesis_calls]
        self.log.add("kinesis", data["t"])

    def timer_write(self, marks):
        self.log.add("timer_write_start", marks["FAKEPACA"])
        time.sleep(self.timer_write_delay)
        self.log.add("timer_write_end", marks["FAKEPACA"])

    def final_write(self, marks):
        self.log.add("final_save", marks["FAKEPACA"])

    async def run(self, producer, drain_timeout=5):
        return await run(
            self.shutdown,
            self.queue,
            self.marks,
            producer=producer,
            consumer=consumer(self.queue, self.shutdown, self.marks, self.put_record, lambda raw, err: None),
            timer=timer(self.marks, self.shutdown, self.timer_write),
            write_marks=self.final_write,
            drain_timeout=drain_timeout,
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
    p = Pipeline(kinesis_delay=0.02)
    trades = [trade(n) for n in range(5)]
    # Shutdown starts while most trades are still queued.
    stop_after(p.shutdown, 0.03)

    code = await asyncio.wait_for(p.run(feed(p.queue, trades)), timeout=5)

    assert code == 0
    assert [e for e in p.log.events if e[0] == "kinesis"] == [("kinesis", t["t"]) for t in trades]
    assert p.log.events[-1] == ("final_save", trades[-1]["t"])


async def test_consumer_crash_still_saves_marks_and_exits_1():
    p = Pipeline(kinesis_errors={2: client_error("AccessDeniedException")})

    code = await asyncio.wait_for(p.run(feed(p.queue, [trade(0), trade(1), trade(2)])), timeout=5)

    assert code == 1
    # Mark stops at the last record that reached Kinesis.
    assert p.log.events[-1] == ("final_save", trade(0)["t"])


async def test_producer_crash_still_saves_marks_and_exits_1():
    p = Pipeline()

    async def broken_producer():
        await p.queue.put(trade(0))
        await asyncio.sleep(0.05)
        raise ValueError("bad frame")

    code = await asyncio.wait_for(p.run(broken_producer()), timeout=5)

    assert code == 1
    assert ("kinesis", trade(0)["t"]) in p.log.events
    assert p.log.events[-1] == ("final_save", trade(0)["t"])


async def test_drain_deadline_cancels_consumer_and_still_saves(caplog):
    p = Pipeline(kinesis_delay=0.2)
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
