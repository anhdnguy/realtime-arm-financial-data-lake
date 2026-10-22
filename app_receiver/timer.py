import asyncio

from logger import setup_logger
from retry import is_temporary

logger = setup_logger(__name__)

SAVE_INTERVAL = 5

async def timer(latest_mark, shutdown_event, write_dynamodb):
    """
    Save the marks every SAVE_INTERVAL seconds until shutdown.

    - Temporary error: log and keep going. The next tick (or the final save)
      catches up; a stale mark only means more duplicates, never a gap.
    - Permanent error (AccessDenied, ResourceNotFound, ...): raise and crash.
      A container that streams without saving marks breaks the backfill promise.
    """
    while True:
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=SAVE_INTERVAL)
            return
        except asyncio.TimeoutError:
            pass

        try:
            await asyncio.to_thread(write_dynamodb, latest_mark.copy())
        except Exception as e:
            if not is_temporary(e):
                logger.error(f"timer: permanent error saving marks, crashing: {type(e).__name__}: {e}")
                raise
            logger.error(f"timer: temporary error saving marks, trying again next tick: {type(e).__name__}: {e}")
