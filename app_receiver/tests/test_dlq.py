import asyncio
import time

import pytest

from dlq import dlq_worker, hand_off, log_unsent


def markers(caplog):
    return [r.getMessage() for r in caplog.records if "DLQ_WRITE_FAILED" in r.getMessage()]


def test_hand_off_queues_raw_and_error():
    dlq_queue = asyncio.Queue(maxsize=2)
    error = ValueError("bad trade")

    hand_off(dlq_queue, "raw-1", error)

    assert dlq_queue.get_nowait() == ("raw-1", error)


def test_hand_off_never_waits_when_full_and_logs_one_marker(caplog):
    dlq_queue = asyncio.Queue(maxsize=1)
    hand_off(dlq_queue, "raw-1", ValueError("first"))

    hand_off(dlq_queue, "raw-2", ValueError("second"))  # sync call: returning at all proves it didn't wait

    [marker] = markers(caplog)
    assert "DLQ queue full" in marker
    assert "raw=raw-2" in marker
    assert dlq_queue.qsize() == 1


async def test_worker_writes_each_record_then_stops_on_none():
    dlq_queue = asyncio.Queue()
    written = []
    for n in range(3):
        hand_off(dlq_queue, f"raw-{n}", ValueError(str(n)))
    dlq_queue.put_nowait(None)

    await asyncio.wait_for(
        dlq_worker(dlq_queue, asyncio.Event(), lambda raw, err: written.append(raw)), timeout=1
    )

    assert written == ["raw-0", "raw-1", "raw-2"]


async def test_worker_cancelled_mid_write_logs_marker_for_current_record(caplog):
    dlq_queue = asyncio.Queue()
    hand_off(dlq_queue, "raw-slow", ValueError("bad"))
    worker = asyncio.create_task(
        dlq_worker(dlq_queue, asyncio.Event(), lambda raw, err: time.sleep(0.3))
    )
    await asyncio.sleep(0.05)  # worker is now inside the S3 write

    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker

    [marker] = markers(caplog)
    assert "cancelled at shutdown" in marker
    assert "raw=raw-slow" in marker


def test_log_unsent_logs_one_marker_per_leftover_and_skips_none(caplog):
    dlq_queue = asyncio.Queue()
    hand_off(dlq_queue, "raw-1", ValueError("a"))
    hand_off(dlq_queue, "raw-2", ValueError("b"))
    dlq_queue.put_nowait(None)

    assert log_unsent(dlq_queue) == 2
    assert len(markers(caplog)) == 2
    assert dlq_queue.empty()
