import logging
import sys

def setup_logger(name: str) -> logging.Logger:
    """
    Create a logger with consistent formatting.
    """
    logger = logging.getLogger(name)
    logger.setLevel('INFO')

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel('INFO')

    # Formatter - structured format for easy parsing
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger