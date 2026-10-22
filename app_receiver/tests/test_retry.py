import asyncio

import pytest
from botocore.exceptions import ClientError

import retry as retry_module
from dlq import send_to_dlq
from retry import MAX_ATTEMPT, RetryExhausted, retry


def client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "PutRecord")


TEMP = "ProvisionedThroughputExceededException"
PERMANENT = "InvalidArgumentException"


class Flaky:
    """Blocking callable that raises the given errors in order, then succeeds."""

    def __init__(self, *errors, forever=None):
        self.errors = list(errors)
        self.forever = forever
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        if self.forever is not None:
            raise self.forever


@pytest.fixture
def waits(monkeypatch):
    """
    Replace the backoff wait with an instant one and record each timeout.
    A set shutdown event ends the wait the same way the real one does.
    """
    timeouts = []

    async def fake_wait_for(aw, timeout):
        timeouts.append(timeout)
        aw.close()
        if shutdown.is_set():
            return True
        raise asyncio.TimeoutError

    monkeypatch.setattr(retry_module.asyncio, "wait_for", fake_wait_for)
    shutdown = asyncio.Event()
    return timeouts, shutdown


async def test_success_on_first_try_does_not_wait(waits):
    timeouts, shutdown = waits
    func = Flaky()

    assert await retry(func, shutdown) is True
    assert func.calls == 1
    assert timeouts == []


async def test_temporary_error_then_success(waits):
    timeouts, shutdown = waits
    func = Flaky(client_error(TEMP))

    assert await retry(func, shutdown) is True
    assert func.calls == 2
    assert timeouts == [2]


async def test_gives_up_after_first_try_plus_max_retries_without_final_wait(waits):
    timeouts, shutdown = waits
    error = client_error(TEMP)
    func = Flaky(forever=error)

    with pytest.raises(RetryExhausted) as info:
        await retry(func, shutdown)

    assert func.calls == MAX_ATTEMPT + 1
    assert timeouts == [2, 4, 8, 16, 30, 30, 30, 30, 30, 30]
    assert info.value.__cause__ is error


async def test_permanent_error_raises_at_once(waits):
    timeouts, shutdown = waits
    func = Flaky(client_error(PERMANENT))

    with pytest.raises(ClientError) as info:
        await retry(func, shutdown)

    assert info.value.response["Error"]["Code"] == PERMANENT
    assert func.calls == 1
    assert timeouts == []


async def test_first_try_still_runs_when_shutdown_already_started(waits):
    # Draining the queue at shutdown depends on this.
    timeouts, shutdown = waits
    shutdown.set()
    func = Flaky()

    assert await retry(func, shutdown) is True
    assert func.calls == 1


async def test_shutdown_before_a_wait_gives_up_after_one_try(waits):
    timeouts, shutdown = waits
    shutdown.set()
    func = Flaky(forever=client_error(TEMP))

    assert await retry(func, shutdown) is False
    assert func.calls == 1


async def test_shutdown_during_a_real_wait_returns_false_quickly():
    # No fake here: the real 2s wait must end as soon as shutdown starts.
    shutdown = asyncio.Event()
    func = Flaky(forever=client_error(TEMP))
    asyncio.get_running_loop().call_later(0.05, shutdown.set)

    result = await asyncio.wait_for(retry(func, shutdown), timeout=1)

    assert result is False
    assert func.calls == 1


# --- send_to_dlq: returns True/False, never raises, one marker per lost record ---

def markers(caplog):
    return [r for r in caplog.records if "DLQ_WRITE_FAILED" in r.getMessage()]


async def test_send_to_dlq_success_passes_raw_and_error(waits):
    timeouts, shutdown = waits
    saved = []
    error = ValueError("bad trade")

    assert await send_to_dlq("raw-frame", error, shutdown, lambda raw, err: saved.append((raw, err))) is True
    assert saved == [("raw-frame", error)]


async def test_send_to_dlq_permanent_error_logs_one_marker(waits, caplog):
    timeouts, shutdown = waits
    save = Flaky(client_error("AccessDenied"))

    result = await send_to_dlq("raw-frame", ValueError("bad trade"), shutdown, lambda raw, err: save())

    assert result is False
    [marker] = markers(caplog)
    assert "AccessDenied" in marker.getMessage()
    assert "raw=raw-frame" in marker.getMessage()


async def test_send_to_dlq_exhausted_marker_names_last_error(waits, caplog):
    timeouts, shutdown = waits
    save = Flaky(forever=client_error("SlowDown"))

    result = await send_to_dlq("raw-frame", ValueError("bad trade"), shutdown, lambda raw, err: save())

    assert result is False
    [marker] = markers(caplog)
    assert "RetryExhausted" in marker.getMessage()
    assert "SlowDown" in marker.getMessage()


async def test_send_to_dlq_shutdown_logs_one_marker(waits, caplog):
    timeouts, shutdown = waits
    shutdown.set()
    save = Flaky(forever=client_error("SlowDown"))

    result = await send_to_dlq("raw-frame", ValueError("bad trade"), shutdown, lambda raw, err: save())

    assert result is False
    [marker] = markers(caplog)
    assert "interrupted by shutdown" in marker.getMessage()
