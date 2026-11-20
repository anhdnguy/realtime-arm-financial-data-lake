"""
Watcher: reads the receiver's heartbeats and logs ALERT lines when the broker is
unreachable, the receiver is down, or it keeps restarting. The rules are in
rules.py; this file is only Kafka and the loop.

No consumer group: assign() to partition 0 (one partition, one reader). In the
step 4 probe, a group took the partition away ~47s into a broker outage, gave it
back only ~65s after the broker returned, and skipped a beat on the way.

Startup: wait until the broker answers -> check the topic exists -> read the end
offset -> find the offset from CRASH_LOOP_WINDOW ago -> assign. Records below the
end offset are replay (placed by their CreateTime); the rest are live (placed by
arrival time, the watcher's own clock).
"""
import json
import os
import signal
import sys
import time

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Consumer, KafkaException, TopicPartition

from logger import setup_logger
from rules import CRASH_LOOP_WINDOW, WatcherState

logger = setup_logger("watcher")

CHECK_INTERVAL = 30   # broker check + rules every 30s
METADATA_TIMEOUT = 5  # a failed list_topics takes this long
POLL_TIMEOUT = 1.0    # also how fast a stop request is noticed
PARTITION = 0         # the heartbeat topic has one partition (kafka-init)
REQUIRED_ENV = ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_HEARTBEAT_TOPIC"]


class ConfigError(Exception):
    """Required setup is missing (an env var, the topic). Crash at startup."""


def consumer_config(bootstrap):
    return {
        "bootstrap.servers": bootstrap,
        # confluent-kafka refuses to build a Consumer without one, even for assign().
        # With assign() and no commits, the consumer never joins this group.
        "group.id": "watcher-unused",
        "enable.auto.commit": False,
        # ~20 events/s during an outage (step 4 probe), and none when the broker is
        # back, so they decide nothing: list_topics does. Kept at DEBUG.
        "error_cb": lambda err: logger.debug(f"librdkafka: {err.name()}: {err.str()}"),
        # librdkafka's own log lines (~3/s during an outage): the watcher reports
        # broker trouble itself. 2 = critical only.
        "log_level": 2,
    }


def log_events(events):
    for level, message in events:
        logger.log(level, message)


def broker_reachable(consumer):
    """One metadata request. Fails after METADATA_TIMEOUT when the broker is down."""
    try:
        consumer.list_topics(timeout=METADATA_TIMEOUT)
        return True
    except KafkaException:
        return False


def check_topic(consumer, topic):
    """A missing topic is a setup fault: crash, like the receiver's startup check."""
    metadata = consumer.list_topics(topic=topic, timeout=METADATA_TIMEOUT)
    found = metadata.topics.get(topic)
    if found is None or found.error is not None:
        raise ConfigError(f"Kafka topic '{topic}' not found; kafka-init in docker-compose.yaml creates it")


def start_position(consumer, topic, now):
    """
    (start, end): start = first offset from CRASH_LOOP_WINDOW ago, so the
    crash-loop rule has its history after a watcher restart; end = the offset the
    next record will get, so everything below it is replay.
    """
    _, end = consumer.get_watermark_offsets(TopicPartition(topic, PARTITION), timeout=METADATA_TIMEOUT)
    since_ms = int((now - CRASH_LOOP_WINDOW) * 1000)
    [found] = consumer.offsets_for_times([TopicPartition(topic, PARTITION, since_ms)], timeout=METADATA_TIMEOUT)
    if getattr(found, "error", None):
        raise KafkaException(found.error)
    # -1: no record that new; start at the end and wait for live beats.
    start = found.offset if found.offset >= 0 else end
    return start, end


def handle_message(msg, state, end_offset, now):
    """One heartbeat record -> the rules. A bad record is logged and skipped (data fault)."""
    try:
        beat = json.loads(msg.value())
        if not isinstance(beat, dict) or "started_at" not in beat:
            raise ValueError("not a heartbeat (no started_at)")
    except (ValueError, TypeError) as e:
        logger.warning(f"record skipped at offset {msg.offset()}: {e}: {msg.value()!r:.200}")
        return []

    live = msg.offset() >= end_offset
    ts_type, ts_ms = msg.timestamp()
    at = now if live or ts_type == TIMESTAMP_NOT_AVAILABLE else ts_ms / 1000
    return state.on_heartbeat(beat, at, live)


def pause(seconds, sleep, stop):
    """Sleep in 1s steps so a stop request is noticed quickly."""
    for _ in range(int(seconds)):
        if stop():
            return
        sleep(1)


def wait_for_broker(consumer, state, clock, sleep, stop):
    """True once list_topics works; False if asked to stop first."""
    while not stop():
        ok = broker_reachable(consumer)
        log_events(state.on_broker_check(ok, clock()))
        if ok:
            return True
        pause(CHECK_INTERVAL, sleep, stop)
    return False


def run(consumer, topic, state, clock=time.time, sleep=time.sleep, stop=lambda: False):
    if not wait_for_broker(consumer, state, clock, sleep, stop):
        return
    check_topic(consumer, topic)
    start, end = start_position(consumer, topic, clock())
    consumer.assign([TopicPartition(topic, PARTITION, start)])
    logger.info(f"reading '{topic}' from offset {start}; offsets below {end} are replay")

    next_check = clock() + CHECK_INTERVAL
    while not stop():
        msg = consumer.poll(POLL_TIMEOUT)
        if msg is not None:
            if msg.error():
                logger.warning(f"poll error: {msg.error()}")
            else:
                log_events(handle_message(msg, state, end, clock()))

        if clock() >= next_check:
            log_events(state.on_broker_check(broker_reachable(consumer), clock()))
            log_events(state.on_tick(clock()))
            next_check = clock() + CHECK_INTERVAL


def main():
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        raise ConfigError(f"missing required env vars: {', '.join(missing)}")
    topic = os.environ["KAFKA_HEARTBEAT_TOPIC"]

    stopping = []
    for signame in ("SIGINT", "SIGTERM"):
        signal.signal(getattr(signal, signame), lambda signum, frame: stopping.append(signum))

    consumer = Consumer(consumer_config(os.environ["KAFKA_BOOTSTRAP_SERVERS"]))
    logger.info(f"watcher started, topic '{topic}'")
    try:
        # Stops within ~POLL_TIMEOUT, or METADATA_TIMEOUT if a broker check is running:
        # well inside Docker's default 10s stop timeout.
        run(consumer, topic, WatcherState(), stop=lambda: bool(stopping))
    finally:
        consumer.close()
    logger.info("watcher stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
