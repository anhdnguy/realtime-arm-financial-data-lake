import json
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from config import KAFKA_PRODUCER_CONFIG, ConfigError
from errors import RecordRejected, TemporaryError
from kafka_client import FLUSH_TIMEOUT, KafkaClient
from retry import is_temporary

TOPIC = "trades"


def trade():
    return {"T": "t", "S": "FAKEPACA", "t": "2026-10-22T01:00:00Z", "p": 100.5, "s": 10}


class FakeProducer:
    """
    Stands in for confluent_kafka.Producer. Like the real one, produce() only
    queues the record; the delivery callback runs inside flush().

    produce_error:  raised by produce() itself (client-side checks, e.g. too large)
    delivery_error: the KafkaError flush() hands to the callback (None = delivered)
    unconfirmed:    flush() returns without calling back, record still queued
    topics:         what list_topics() reports, {name: KafkaError or None}
    """

    def __init__(self, produce_error=None, delivery_error=None, unconfirmed=False, topics=None):
        self.produce_error = produce_error
        self.delivery_error = delivery_error
        self.unconfirmed = unconfirmed
        self.topics = topics or {}
        self.sent = []
        self.flush_timeouts = []
        self.pending = []

    def produce(self, topic, value=None, key=None, on_delivery=None):
        if self.produce_error is not None:
            raise self.produce_error
        self.sent.append(SimpleNamespace(topic=topic, key=key, value=value))
        self.pending.append(on_delivery)

    def flush(self, timeout=None):
        self.flush_timeouts.append(timeout)
        if self.unconfirmed:
            return len(self.pending)
        for callback in self.pending:
            callback(self.delivery_error, None)
        self.pending = []
        return 0

    def list_topics(self, topic=None, timeout=-1):
        names = [topic] if topic else list(self.topics)
        return SimpleNamespace(topics={
            name: SimpleNamespace(error=self.topics[name]) for name in names if name in self.topics
        })


def kafka_code(exc):
    """The KafkaError code inside a KafkaException."""
    return exc.args[0].code()


# --- put_record: success --------------------------------------------------

def test_good_trade_goes_to_the_topic_keyed_by_ticker():
    producer = FakeProducer()

    KafkaClient(producer, TOPIC).put_record(trade())

    [record] = producer.sent
    assert record.topic == TOPIC
    # Key = ticker, so each ticker stays on one partition, in order.
    key = record.key.decode() if isinstance(record.key, bytes) else record.key
    assert key == "FAKEPACA"
    assert json.loads(record.value) == trade()


def test_flush_waits_longer_than_the_message_timeout():
    # librdkafka reports every record by message.timeout.ms. Waiting longer means
    # flush never returns with a record still in the buffer (unless the setup is wrong).
    producer = FakeProducer()

    KafkaClient(producer, TOPIC).put_record(trade())

    assert producer.flush_timeouts == [FLUSH_TIMEOUT]
    assert FLUSH_TIMEOUT * 1000 > KAFKA_PRODUCER_CONFIG["message.timeout.ms"]


# --- put_record: data fault -> RecordRejected (DLQ) -----------------------

def test_too_large_at_produce_is_rejected():
    # librdkafka checks the size itself, so produce() raises before anything is sent.
    original = KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
    producer = FakeProducer(produce_error=original)

    with pytest.raises(RecordRejected) as info:
        KafkaClient(producer, TOPIC).put_record(trade())

    assert info.value.__cause__ is original


def test_too_large_in_delivery_report_is_rejected():
    # The broker's limit can be lower than the client's, so the broker refuses it.
    producer = FakeProducer(delivery_error=KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))

    with pytest.raises(RecordRejected) as info:
        KafkaClient(producer, TOPIC).put_record(trade())

    assert kafka_code(info.value.__cause__) == KafkaError.MSG_SIZE_TOO_LARGE


# --- put_record: network / broker busy -> TemporaryError (retry) ----------

@pytest.mark.parametrize("error", [
    # Not flagged retriable by librdkafka, but it is what a broker outage looks like.
    KafkaError(KafkaError._MSG_TIMED_OUT),
    KafkaError(KafkaError.REQUEST_TIMED_OUT, retriable=True),
], ids=lambda e: e.name())
def test_timeouts_and_retriable_errors_are_temporary(error):
    producer = FakeProducer(delivery_error=error)

    with pytest.raises(TemporaryError) as info:
        KafkaClient(producer, TOPIC).put_record(trade())

    assert kafka_code(info.value.__cause__) == error.code()
    assert is_temporary(info.value)


# --- put_record: setup fault -> crash -------------------------------------

def test_other_errors_come_through_unchanged():
    producer = FakeProducer(delivery_error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))

    with pytest.raises(KafkaException) as info:
        KafkaClient(producer, TOPIC).put_record(trade())

    assert kafka_code(info.value) == KafkaError.TOPIC_AUTHORIZATION_FAILED
    assert not is_temporary(info.value)


def test_record_still_unconfirmed_after_flush_crashes():
    # Should be impossible (flush waits past message.timeout.ms). If it happens the
    # timeouts are wrong: crash. Retrying would produce a second copy of a record
    # that is still in the buffer.
    producer = FakeProducer(unconfirmed=True)

    with pytest.raises(Exception) as info:
        KafkaClient(producer, TOPIC).put_record(trade())

    assert not isinstance(info.value, (TemporaryError, RecordRejected))
    assert not is_temporary(info.value)
    assert "message.timeout.ms" in str(info.value)


# --- check_topic: startup check ------------------------------------------

def test_check_topic_passes_when_the_topic_exists():
    producer = FakeProducer(topics={TOPIC: None})

    KafkaClient(producer, TOPIC).check_topic()


@pytest.mark.parametrize("topics", [
    {TOPIC: KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)},  # what the broker returns
    {},                                                     # topic missing from the reply
], ids=["error_on_topic", "topic_absent"])
def test_check_topic_fails_fast_when_the_topic_is_missing(topics):
    # At runtime a missing topic looks like a broker outage (_MSG_TIMED_OUT, retried
    # for minutes). Checking at startup turns it into a clear setup error.
    producer = FakeProducer(topics=topics)

    with pytest.raises(ConfigError, match=TOPIC):
        KafkaClient(producer, TOPIC).check_topic()
