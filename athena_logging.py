"""
Athena Cognitive Engine — Structured Logging Configuration

Replaces stdlib logging with loguru for structured JSON output,
request ID tracing, and automatic log rotation.

Usage in any Athena service:
    from athena_logging import setup_logging, get_logger
    setup_logging("chat-api")
    log = get_logger()
    log.info("Starting service", port=9350)

This also patches stdlib logging so existing `logging.getLogger()` calls
route through loguru automatically.
"""

import logging
import os
import sys
from pathlib import Path

from loguru import logger

LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/athena"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_configured = False


class InterceptHandler(logging.Handler):
    """Route stdlib logging through loguru."""

    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging(service_name: str, level: str = "INFO", json_output: bool = True):
    """Configure loguru logging for an Athena service.

    Args:
        service_name: Name of the service (e.g. "chat-api", "mcp-server")
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        json_output: If True, log as structured JSON to file
    """
    global _configured
    if _configured:
        return
    _configured = True

    # Remove default loguru handler
    logger.remove()

    # Console output — human-readable
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> <level>{level: <8}</level> <cyan>{name}</cyan>: {message}",
        colorize=True,
    )

    # File output — structured JSON with rotation
    if json_output:
        logger.add(
            str(LOG_DIR / f"{service_name}.json"),
            level=level,
            format="{message}",
            serialize=True,
            rotation="100 MB",
            retention="30 days",
            compression="gz",
            enqueue=True,  # Thread-safe
        )

    # Also log plain text for easy grep
    logger.add(
        str(LOG_DIR / f"{service_name}.log"),
        level=level,
        format="{time:YYYY-MM-DD HH:mm:ss} [{level}] {name}: {message}",
        rotation="100 MB",
        retention="30 days",
        compression="gz",
        enqueue=True,
    )

    # Intercept stdlib logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Suppress noisy libraries
    for name in ("uvicorn.access", "httpx", "httpcore", "asyncpg"):
        logging.getLogger(name).setLevel(logging.WARNING)

    logger.info("Logging initialized for {}", service_name)


def get_logger():
    """Get the loguru logger instance."""
    return logger
