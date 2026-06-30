"""CLI logging setup for processing commands (ingest, reset).

Main entry points:
    configure_logging: Attach handlers to the shared processing logger.
    processing_logger: Return the configured logger instance.
    LogMode: Where log output is written (off, console, file, or both).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path


class LogMode(StrEnum):
    """Destination for processing command log output.

    Members:
        OFF: Disable all log output.
        CONSOLE: Write to stderr only.
        FILE: Write to a timestamped file under the pipeline logs directory.
        BOTH: Write to console and file.
    """

    OFF = "off"
    CONSOLE = "console"
    FILE = "file"
    BOTH = "both"


# Map of uppercase level names to ``logging`` module constants.
LOG_LEVELS: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}

LOGGER_NAME = "audit_service.processing"
"""Canonical logger name used by ingest and reset processing commands."""


def configure_logging(
    *,
    logs_dir: Path,
    pipeline_name: str,
    command: str,
    mode: LogMode = LogMode.BOTH,
    level_name: str = "INFO",
) -> logging.Logger:
    """Configure the shared processing logger for a CLI invocation.

    Args:
        logs_dir: Root logs directory from :class:`~audit_service.backend.config.AppConfig`.
        pipeline_name: Pipeline subdirectory name under *logs_dir*.
        command: CLI command name used in log filenames (e.g. ``ingest``).
        mode: Where to emit log records.
        level_name: Verbosity level key from :data:`LOG_LEVELS`.

    Returns:
        Configured :class:`logging.Logger` (may be effectively disabled for
        :attr:`LogMode.OFF`).
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()
    level = LOG_LEVELS.get(level_name.upper(), logging.INFO)
    logger.setLevel(level if mode != LogMode.OFF else logging.CRITICAL + 1)
    logger.propagate = False

    if mode == LogMode.OFF:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if mode in (LogMode.CONSOLE, LogMode.BOTH):
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    if mode in (LogMode.FILE, LogMode.BOTH):
        log_dir = logs_dir / pipeline_name
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_file = log_dir / f"{command}_{timestamp}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info("Log file: %s", log_file)

    return logger


def processing_logger() -> logging.Logger:
    """Return the shared processing logger by :data:`LOGGER_NAME`.

    Returns:
        Logger instance (handlers may be unset until :func:`configure_logging` runs).
    """
    return logging.getLogger(LOGGER_NAME)
