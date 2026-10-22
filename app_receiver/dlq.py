"""
Dead-letter queue (DLQ): bad records go to S3 as text, written by one worker.

Producer and consumer only hand records off (hand_off never waits), so a slow
or failing S3 can never stall the trade stream. The worker is the only caller
of send_to_dlq.

Every record that does not reach S3 gets exactly one DLQ_WRITE_FAILED log line
holding the raw record. That line is its only copy: grep for it to replay.
"""
import asyncio
from functools import partial

from logger import setup_logger
from retry import retry

logger = setup_logger(__name__)

DLQ_QUEUE_SIZE = 100


def log_lost(source, raw, error, reason):
    """The one place that writes the DLQ_WRITE_FAILED marker line."""
    logger.error(
        f"{source}: DLQ_WRITE_FAILED | error={type(error).__name__}: {error} "
        f"| dlq_error={reason} | raw={raw}"
    )


def hand_off(dlq_queue, raw, error):
    """
    Queue a bad record for the DLQ worker. Never waits, never raises.
    If the DLQ queue is full the record is logged as lost instead: waiting here
    would stall the producer or consumer, which is worse than losing a bad record.
    """
    try:
        dlq_queue.put_nowait((raw, error))
    except asyncio.QueueFull:
        log_lost("hand_off", raw, error, "DLQ queue full")


async def send_to_dlq(raw, error, shutdown_event, save_record_s3):
    """
    Write one bad record to the DLQ. Returns True/False and never raises.

    On failure it logs exactly one DLQ_WRITE_FAILED line holding the raw record:
    that line is the record's only copy.
    """
    try:
        status = await retry(partial(save_record_s3, raw, error), shutdown_event)
    except Exception as e:
        dlq_error = f"{type(e).__name__}: {e}"
        if e.__cause__ is not None:
            dlq_error += f" (last error {type(e.__cause__).__name__}: {e.__cause__})"
        log_lost("send_to_dlq", raw, error, dlq_error)
        return False

    if not status:
        log_lost("send_to_dlq", raw, error, "interrupted by shutdown")
    return status


async def dlq_worker(dlq_queue, shutdown_event, save_record_s3):
    """
    Write handed-off records to S3 one at a time. Returns on None, which main
    puts after the consumer has finished (no handoff can come after it).

    If cancelled mid-write (DLQ drain deadline), the current record still gets
    its marker. CancelledError is not an Exception, so send_to_dlq can't catch it.
    The S3 write may still finish in its thread, so this can mean a duplicate,
    never a silent loss.
    """
    while True:
        item = await dlq_queue.get()
        if item is None:
            logger.info("dlq_worker: DLQ queue drained")
            return

        raw, error = item
        try:
            await send_to_dlq(raw, error, shutdown_event, save_record_s3)
        except asyncio.CancelledError:
            log_lost("dlq_worker", raw, error, "cancelled at shutdown")
            raise


def log_unsent(dlq_queue):
    """After the worker has stopped, log a marker for every record still queued."""
    count = 0
    while True:
        try:
            item = dlq_queue.get_nowait()
        except asyncio.QueueEmpty:
            return count
        if item is None:
            continue
        raw, error = item
        log_lost("dlq", raw, error, "not written before shutdown")
        count += 1
