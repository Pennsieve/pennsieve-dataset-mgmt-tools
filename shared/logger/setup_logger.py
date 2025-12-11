"""
Logger setup utilities for Pennsieve tools.
"""

import logging
import os
from pathlib import Path


def setup_logger(
    name: str,
    log_dir: str = "output/logs",
    level: int = logging.INFO,
    console: bool = True
) -> logging.Logger:
    """
    Set up a logger with file and optional console handlers.

    Args:
        name: Logger name (used for both logger and log file)
        log_dir: Directory to store log files
        level: Logging level (default: INFO)
        console: Whether to also log to console (default: True)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Don't add handlers if they already exist
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Create log directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    # File handler
    log_file = os.path.join(log_dir, f"{name}.log")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)

    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    if console:
        console_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(file_handler)
    if console:
        logger.addHandler(console_handler)

    return logger
