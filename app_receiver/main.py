import asyncio
import boto3
import boto3.session
import signal

from config import AppConfig, TICKERS
from consumer import consumer
from dynamodb_client import DynamoDBClient
from kinesis_client import KinesisClient
from logger import setup_logger
from producer import producer
from s3_client import S3Client
from timer import timer

logger = setup_logger(__name__)

def signal_handler(signum, task_producer, shutdown_event):
    """
    Handle shutdown signals (SIGTERM, SIGINT).
    
    ECS sends SIGTERM when stopping a task.
    CTRL+C sends SIGINT during local testing.
    """
    logger.info(f"shutdown_handler: Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()
    task_producer.cancel()

async def main():
    """
    Main application entry point.
    """
    AppConfig.validate()

    session = boto3.session.Session(
        region_name = AppConfig.aws_region,
        aws_access_key_id = AppConfig.aws_access_key_id,
        aws_secret_access_key = AppConfig.aws_secret_access_key
    )
    dynamodb = DynamoDBClient(session)
    kinesis = KinesisClient(session)
    s3 = S3Client(session)
    shutdown_event = asyncio.Event()

    latest_mark = dict.fromkeys(TICKERS, "")

    queue = asyncio.Queue(maxsize=200)
    task_producer = asyncio.create_task(producer(queue, shutdown_event))
    task_consumer = asyncio.create_task(consumer(
        queue,
        shutdown_event,
        latest_mark,
        dynamodb.put_item,
        kinesis.put_record,
        s3.put_dlq_record
    ))
    task_timer = asyncio.create_task(timer(
        latest_mark,
        shutdown_event,
        dynamodb.put_item
    ))

    loop = asyncio.get_running_loop()

    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            signal_handler,
            signame,
            task_producer,
            shutdown_event
        )
    await asyncio.gather(
        task_producer,
        task_consumer,
        task_timer
    )

if __name__ == "__main__":
    asyncio.run(main())
