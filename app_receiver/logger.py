import logging
import os
import sys

# INFO by default; set LOG_LEVEL=DEBUG to see every trade.
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

def setup_logger(name: str) -> logging.Logger:
    """
    Create a logger with consistent formatting.
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(LOG_LEVEL)

    # Formatter - structured format for easy parsing
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
