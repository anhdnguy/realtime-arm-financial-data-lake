import asyncio
import json

import pytest
from websockets.exceptions import ConnectionClosedOK

from producer import AlpacaError, forward_frames, parse_frame, Backoff


def trade(n, **overrides):
    t = {"T": "t", "S": "FAKEPACA", "t": f"2026-10-22T01:00:0{n}Z", "p": 100 + n, "s": 1}
    t.update(overrides)
    return t


def frame(*messages):
    return json.dumps(list(messages))


# --- Backoff -----

def test_backoff_doubles_then_stops_at_the_cap():
    backoff = Backoff()
    results = []
    for _ in range(7):
        results.append(backoff.next_wait())

    assert results == [2, 4, 8, 16, 32, 60, 60]


def test_backoff_reset_starts_from_the_first_wait():
    backoff = Backoff()
    for _ in range(4):
        backoff.next_wait()
    backoff.reset()

    assert backoff.next_wait() == 2


# --- parse_frame ---

def test_good_trades_pass_through_in_order():
    trades, bad, _ = parse_frame(frame(trade(0), trade(1)))

    assert trades == [trade(0), trade(1)]
    assert bad == []


def test_invalid_json_sends_the_whole_frame_to_dlq():
    raw = '[{"T": "t", "S": "FAKE'

    trades, [(record, error)], _ = parse_frame(raw)

    assert trades == []
    assert record == raw
    assert isinstance(error, json.JSONDecodeError)


def test_invalid_utf8_bytes_become_text_for_the_dlq():
    trades, [(record, error)], _ = parse_frame(b"\xff\xfe not json")

    assert trades == []
    assert isinstance(record, str)


@pytest.mark.parametrize("raw", ['{"T": "t"}', '"hello"', "42", "null"])
def test_frame_that_is_not_a_list_is_bad(raw):
    trades, [(record, error)], _ = parse_frame(raw)

    assert trades == []
    assert record == raw


@pytest.mark.parametrize("bad_trade, field", [
    ({"T": "t", "t": "2026-10-22T01:00:00Z"}, "S"),
    ({"T": "t", "S": "FAKEPACA"}, "t"),
    (trade(0, S=""), "S"),
    (trade(0, S=None), "S"),
    (trade(0, t=12345), "t"),
])
def test_bad_trade_in_good_frame_only_that_message_is_bad(bad_trade, field):
    trades, [(record, error)], _ = parse_frame(frame(trade(0), bad_trade, trade(1)))

    assert trades == [trade(0), trade(1)]
    assert record == bad_trade
    assert f"'{field}'" in str(error)


@pytest.mark.parametrize("message", ["just a string", 7, ["nested"], {"S": "FAKEPACA"}])
def test_message_without_a_type_is_bad(message):
    trades, [(record, error)], _ = parse_frame(frame(trade(0), message))

    assert trades == [trade(0)]
    assert record == message


def test_control_and_other_messages_are_not_trades_and_not_bad():
    trades, bad, _ = parse_frame(frame(
        {"T": "success", "msg": "connected"},
        {"T": "subscription", "trades": ["FAKEPACA"]},
        {"T": "q", "S": "FAKEPACA"},
        trade(0),
    ))

    assert trades == [trade(0)]
    assert bad == []


@pytest.mark.parametrize("code, msg", [(402, "auth failed"), (406, "connection limit exceeded")])
def test_error_message_raises(code, msg):
    with pytest.raises(AlpacaError) as info:
        parse_frame(frame({"T": "error", "code": code, "msg": msg}))

    assert info.value.code == code
    assert msg in str(info.value)


@pytest.mark.parametrize("message", [{"T": "success", "msg": "connected"}, {"T":"subscription","trades":["AAPL"]}])
def test_reconnection_without_authenticated(message):
    _, _, authenticated = parse_frame(frame(message))

    assert authenticated is False


def test_reconnection_with_authenticated():
    raw = {"T": "success", "msg": "authenticated"}

    _, _, authenticated = parse_frame(frame(raw))

    assert authenticated is True


# --- forward_frames ---

class FakeWebsocket:
    """Returns the given frames from recv(), then closes like a dropped connection."""

    def __init__(self, *frames):
        self.frames = list(frames)

    async def recv(self):
        if not self.frames:
            raise ConnectionClosedOK(None, None)
        return self.frames.pop(0)


async def test_forward_frames_routes_trades_and_bad_records():
    queue, dlq_queue, backoff = asyncio.Queue(), asyncio.Queue(), Backoff()
    ws = FakeWebsocket(
        frame({"T": "success", "msg": "authenticated"}),
        frame(trade(0), trade(0, S=None)),
        "not json",
        frame(trade(1)),
    )

    with pytest.raises(ConnectionClosedOK):
        await forward_frames(ws, queue, dlq_queue, backoff)

    assert [queue.get_nowait() for _ in range(queue.qsize())] == [trade(0), trade(1)]
    assert [r for r, _ in [dlq_queue.get_nowait() for _ in range(dlq_queue.qsize())]] == [trade(0, S=None), "not json"]


async def test_forward_frames_crashes_on_alpaca_error():
    queue, dlq_queue, backoff = asyncio.Queue(), asyncio.Queue(), Backoff()
    ws = FakeWebsocket(frame({"T": "error", "code": 402, "msg": "auth failed"}), frame(trade(0)))

    with pytest.raises(AlpacaError):
        await forward_frames(ws, queue, dlq_queue, backoff)

    assert queue.empty()


async def test_forward_frames_reset_backoff_when_reconnect_successfully():
    queue, dlq_queue, backoff = asyncio.Queue(), asyncio.Queue(), Backoff()
    ws = FakeWebsocket(
        frame({"T": "success", "msg": "connected"}),
        frame({"T": "success", "msg": "authenticated"}),
        frame({"T":"subscription","trades":["AAPL"]})
    )

    backoff.next_wait()

    with pytest.raises(ConnectionClosedOK):
        await forward_frames(ws, queue, dlq_queue, backoff)

    assert backoff.next_wait() == 2


async def test_backoff_authenticated_and_subscription_arrives_in_same_frame():
    queue, dlq_queue, backoff = asyncio.Queue(), asyncio.Queue(), Backoff()
    ws = FakeWebsocket(
        frame({"T": "success", "msg": "authenticated"}, {"T":"subscription","trades":["AAPL"]})
    )
    
    backoff.next_wait()
    
    with pytest.raises(ConnectionClosedOK):
        await forward_frames(ws, queue, dlq_queue, backoff)

    assert backoff.next_wait() == 2
