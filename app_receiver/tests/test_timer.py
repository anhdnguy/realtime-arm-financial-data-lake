import asyncio

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

import main
from main import timer


def client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "PutItem")


class Writer:
    """Fake write_dynamodb: records each snapshot, raises the given errors in order."""

    def __init__(self, *errors):
        self.errors = list(errors)
        self.saved = []

    def __call__(self, marks):
        self.saved.append(marks)
        if self.errors:
            raise self.errors.pop(0)


@pytest.fixture(autouse=True)
def fast_ticks(monkeypatch):
    monkeypatch.setattr(main, "SAVE_INTERVAL", 0.01)


def stop_after(shutdown, seconds):
    asyncio.get_running_loop().call_later(seconds, shutdown.set)


async def test_saves_snapshots_until_shutdown():
    shutdown = asyncio.Event()
    marks = {"FAKEPACA": "2026-10-22T01:00:00Z"}
    writer = Writer()
    stop_after(shutdown, 0.1)

    await asyncio.wait_for(timer(marks, shutdown, writer), timeout=1)

    assert len(writer.saved) >= 2
    assert writer.saved[0] == marks
    assert writer.saved[0] is not marks  # a copy, so later updates can't change a write in flight


async def test_returns_at_once_when_shutdown_already_started():
    shutdown = asyncio.Event()
    shutdown.set()
    writer = Writer()

    await asyncio.wait_for(timer({}, shutdown, writer), timeout=1)

    assert writer.saved == []


@pytest.mark.parametrize("error", [
    client_error("ProvisionedThroughputExceededException"),
    EndpointConnectionError(endpoint_url="http://localstack:4566"),
])
async def test_temporary_error_keeps_going(error):
    shutdown = asyncio.Event()
    writer = Writer(error)
    stop_after(shutdown, 0.1)

    await asyncio.wait_for(timer({"FAKEPACA": "t"}, shutdown, writer), timeout=1)

    assert len(writer.saved) >= 2  # the tick after the failure still saved


@pytest.mark.parametrize("code", ["AccessDeniedException", "ResourceNotFoundException"])
async def test_permanent_error_crashes(code):
    shutdown = asyncio.Event()
    writer = Writer(client_error(code))

    with pytest.raises(ClientError) as info:
        await asyncio.wait_for(timer({"FAKEPACA": "t"}, shutdown, writer), timeout=1)

    assert info.value.response["Error"]["Code"] == code
    assert len(writer.saved) == 1
