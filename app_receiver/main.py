import signal
import sys
from app.logger import setup_logger

logger = setup_logger(__name__)

# Global falg for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """
    Handle shutdown signals (SIGTERM, SIGINT).
    
    ECS sends SIGTERM when stopping a task.
    CTRL+C sends SIGINT during local testing.
    """
    global shutdown_requested

    signal_name = signal.Signals(signum).name
    logger.info(f"Received signal {signal_name} ({signum}), initiating graceful shutdown...")

    shutdown_requested = True
    # Note: Don't sys.exit() here - let the main loop finish current work

def start_container_1():
    """
    Start the main processing loop with shutdown support.

    This wraps signal_catch to support graceful shutdown.
    """
    global shutdown_requested

    try:
        # Modify 

def main():
    """
    Main application entry point.
    """
    pass