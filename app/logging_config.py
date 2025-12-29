"""Logging configuration for the application."""

import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the root logger with a standard format.

    Args:
        level: Logging level (default: logging.INFO).
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
