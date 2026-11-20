import json
import logging
from types import SimpleNamespace

import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME, KafkaError, KafkaException, TopicPartition

import watcher
from rules import CRASH_LOOP_WINDOW, WatcherState
from watcher import (CHECK_INTERVAL, ConfigError, broker_reachable, check_topic,
                     handle_message, run, start_position)

TOPIC = "heartbeat"
T0 = 1_800_000_000.0


def beat_bytes(started="2026-11-18T02:00:00Z"):
    return json.dumps({"started_at": started, "sent_at": "x",
                       "last_alpaca_message_at": "2026-11-18T02:00:05Z"}).encode()


class FakeMessage:
    def __init__(self, offset, value, create_time_ms=None, error=None):
        self._offset, self._value, self._error = offset, value, error
        self._ts = (TIMESTAMP_CREATE_TIME, create_time_ms if create_time_ms is not None else 0)

    def offset(self): return self._offset
    def value(self): return self._value
    def timestamp(self): return self._ts
    def error(self): return self._error


class Clock:
    """Fake time: each poll() moves it forward by the poll timeout."""
    def __init__(self, now=T0): self.now = now
    def __call__(self): return self.now


class FakeConsumer:
    """
    Stands in for confluent_kafka.Consumer.
    reachable: list of answers for list_topics, used in order (last one repeats)
    """

    def __init__(self, clock, reachable=(True,), topics=(TOPIC,), end=10, offset_for_time=4, messages=()):
        self.clock = clock
        self.reachable = list(reachable)
        self.topics = topics
        self.end = end
        self.offset_for_time = offset_for_time
        self.messages = list(messages)
        self.assigned = None
        self.asked_time_ms = None
        self.closed = False

    def list_topics(self, topic=None, timeout=-1):
        ok = self.reachable.pop(0) if len(self.reachable) > 1 else self.reachable[0]
        if not ok:
            self.clock.now += timeout  # a failed check waits out the whole timeout
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        names = [topic] if topic else list(self.topics)
        return SimpleNamespace(topics={
            n: SimpleNamespace(error=None if n in self.topics else KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
            for n in names
        })

    def get_watermark_offsets(self, tp, timeout=None):
        return 0, self.end

    def offsets_for_times(self, tps, timeout=None):
        [tp] = tps
        self.asked_time_ms = tp.offset
        return [TopicPartition(tp.topic, tp.partition, self.offset_for_time)]

    def assign(self, tps):
        self.assigned = tps

    def poll(self, timeout=None):
        self.clock.now += timeout
        return self.messages.pop(0) if self.messages else None


# --- startup -------------------------------------------------------------------

def test_start_position_reads_from_15_minutes_back_and_splits_at_the_end_offset():
    consumer = FakeConsumer(Clock(), end=10, offset_for_time=4)

    start, end = start_position(consumer, TOPIC, T0)

    assert (start, end) == (4, 10)
    assert consumer.asked_time_ms == int((T0 - CRASH_LOOP_WINDOW) * 1000)


def test_start_position_with_nothing_newer_starts_at_the_end():
    # offsets_for_times answers -1 when no record is that new: just wait for live beats.
    consumer = FakeConsumer(Clock(), end=10, offset_for_time=-1)

    assert start_position(consumer, TOPIC, T0) == (10, 10)


def test_missing_topic_at_startup_crashes():
    with pytest.raises(ConfigError, match=TOPIC):
        check_topic(FakeConsumer(Clock(), topics=()), TOPIC)


def test_broker_reachable_is_false_on_a_kafka_error():
    assert broker_reachable(FakeConsumer(Clock(), reachable=[True])) is True
    assert broker_reachable(FakeConsumer(Clock(), reachable=[False])) is False


# --- one message ------------------------------------------------------------------

def test_replayed_message_counts_at_its_create_time():
    state = WatcherState()
    msg = FakeMessage(offset=3, value=beat_bytes(), create_time_ms=int((T0 - 600) * 1000))

    handle_message(msg, state, end_offset=10, now=T0)

    assert state.last_beat[0] == T0 - 600


def test_live_message_counts_at_arrival_time():
    state = WatcherState()
    msg = FakeMessage(offset=10, value=beat_bytes(), create_time_ms=int((T0 - 600) * 1000))

    handle_message(msg, state, end_offset=10, now=T0)

    assert state.last_beat[0] == T0


@pytest.mark.parametrize("value", [b"not json", b'{"sent_at": "x"}', b"[1, 2]"],
                         ids=["bad_json", "no_started_at", "not_an_object"])
def test_bad_record_is_skipped_with_a_warning(value, caplog):
    state = WatcherState()

    events = handle_message(FakeMessage(offset=10, value=value), state, end_offset=10, now=T0)

    assert events == []
    assert state.last_beat is None
    assert "skipped" in caplog.text


# --- the loop ----------------------------------------------------------------------

def stop_at(clock, t):
    return lambda: clock.now >= t


def alerts(caplog):
    return [r.getMessage().split(":")[0] for r in caplog.records if r.getMessage().startswith(("ALERT", "CLEARED"))]


def test_run_waits_for_the_broker_then_assigns_and_reads(caplog):
    caplog.set_level(logging.INFO)
    clock = Clock()
    consumer = FakeConsumer(
        clock,
        reachable=[False, False, True],
        end=10, offset_for_time=9,
        messages=[FakeMessage(9, beat_bytes(), int((T0 - 60) * 1000)), FakeMessage(10, beat_bytes())],
    )

    run(consumer, TOPIC, WatcherState(), clock=clock, sleep=lambda s: setattr(clock, "now", clock.now + s),
        stop=stop_at(clock, T0 + 120))

    assert consumer.assigned == [TopicPartition(TOPIC, 0, 9)]
    assert alerts(caplog)[:2] == ["ALERT broker unreachable", "CLEARED broker unreachable"]
    assert any("receiver alive" in r.getMessage() for r in caplog.records)  # the live one


def test_run_checks_the_broker_every_interval_and_reports_silence(caplog):
    clock = Clock()
    consumer = FakeConsumer(clock, reachable=[True], end=0, offset_for_time=-1)

    run(consumer, TOPIC, WatcherState(), clock=clock, sleep=lambda s: None,
        stop=stop_at(clock, T0 + 180 + 2 * CHECK_INTERVAL))

    # No beats at all: reported 3 min after the first successful check.
    assert alerts(caplog) == ["ALERT receiver down"]


def test_run_stops_while_waiting_for_the_broker():
    clock = Clock()
    consumer = FakeConsumer(clock, reachable=[False])

    run(consumer, TOPIC, WatcherState(), clock=clock, sleep=lambda s: setattr(clock, "now", clock.now + s),
        stop=stop_at(clock, T0 + 100))

    assert consumer.assigned is None


def test_main_imports_without_starting():
    assert callable(watcher.main)
