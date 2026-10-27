"""
App-level errors for clients that sort their own library's errors.

kafka_client turns each Kafka error into one of these (or lets it through,
which crashes the container), so consumer.py and retry.py never import Kafka.
The original error is always chained in __cause__.
"""


class TemporaryError(Exception):
    """A fault that can clear on its own (network, broker busy). retry() tries again."""


class RecordRejected(Exception):
    """The sink refused this record because of its content. Retrying won't help: DLQ it."""
