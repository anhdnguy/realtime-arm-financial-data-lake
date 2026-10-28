import json

from confluent_kafka import KafkaError, KafkaException

from config import ConfigError
from errors import RecordRejected, TemporaryError
from logger import setup_logger

logger = setup_logger(__name__)

# Must stay above KAFKA_PRODUCER_CONFIG["message.timeout.ms"] (10s). librdkafka reports
# every record by then, so flush never returns with the record still in its buffer.
FLUSH_TIMEOUT = 12
# How long the startup topic check waits for the broker's reply.
METADATA_TIMEOUT = 10


def raise_sorted(original):
    """
    Raise what the app should see for a KafkaException, by whose fault it is:

      MSG_SIZE_TOO_LARGE             -> RecordRejected (data fault: DLQ)
      _MSG_TIMED_OUT or retriable()  -> TemporaryError (network / broker busy: retry)
      anything else                  -> the KafkaException unchanged (setup fault: crash)

    _MSG_TIMED_OUT is not flagged retriable by librdkafka, but it is what a broker
    outage looks like, so it is listed by name. The original is chained in __cause__.
    """
    error = original.args[0]
    if error.code() == KafkaError.MSG_SIZE_TOO_LARGE:
        raise RecordRejected(f"Kafka refused the record: {error.str()}") from original
    if error.code() == KafkaError._MSG_TIMED_OUT or error.retriable():
        raise TemporaryError(f"Kafka send failed: {error.str()}") from original
    raise original


class KafkaClient:
    """
    Sends one trade at a time and waits for the broker's ack before returning, so
    the consumer only moves a ticker's mark after the trade is safely in Kafka.

    Takes a ready-made producer (a confluent_kafka.Producer built with
    KAFKA_PRODUCER_CONFIG), so tests can pass a fake.
    """

    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def put_record(self, data):
        """
        Blocking: runs in a thread via retry(). Returns once the broker has the record.

        The delivery callback runs inside flush(), on this same thread, so a plain
        local list is enough to carry the result out.
        """
        reports = []
        try:
            self.producer.produce(
                self.topic,
                key=data["S"],  # one ticker -> one partition, kept in order
                value=json.dumps(data).encode("utf-8"),
                on_delivery=lambda error, msg: reports.append(error),
            )
        except KafkaException as e:
            raise_sorted(e)

        self.producer.flush(FLUSH_TIMEOUT)

        if not reports:
            # Should be impossible. Retrying would queue a second copy while the first
            # is still in librdkafka's buffer, so treat it as a setup fault and crash.
            raise RuntimeError(
                f"record still unconfirmed after a {FLUSH_TIMEOUT}s flush: "
                f"message.timeout.ms must stay below FLUSH_TIMEOUT"
            )
        if reports[0] is not None:
            raise_sorted(KafkaException(reports[0]))

        logger.debug(f"put_record: delivered to Kafka: {data}")

    def check_topic(self):
        """
        Crash at startup if the topic doesn't exist. At runtime a missing topic looks
        exactly like a broker outage (_MSG_TIMED_OUT), and would be retried for minutes.
        If the broker can't be reached at all, list_topics raises: also a crash.
        """
        metadata = self.producer.list_topics(topic=self.topic, timeout=METADATA_TIMEOUT)
        topic = metadata.topics.get(self.topic)
        if topic is None or topic.error is not None:
            reason = topic.error.str() if topic is not None else "not in the broker's reply"
            raise ConfigError(
                f"Kafka topic '{self.topic}' not found ({reason}); "
                f"kafka-init in docker-compose.yaml creates it"
            )
        logger.info(f"check_topic: Kafka topic '{self.topic}' found")
