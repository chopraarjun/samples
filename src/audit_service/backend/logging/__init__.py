"""Processing command logging configuration.

Main entry points:
    configure_logging: Set up console and/or file handlers for ingest and reset CLIs.
    processing_logger: Retrieve the shared processing logger by name.
    LogMode: Enum of logging output destinations.
    LOG_LEVELS: Map of level name strings to ``logging`` constants.
    LOGGER_NAME: Canonical logger name for processing commands.
"""

from audit_service.backend.logging.setup import (
    LOG_LEVELS,
    LOGGER_NAME,
    LogMode,
    configure_logging,
    processing_logger,
)

__all__ = [
    "LOG_LEVELS",
    "LOGGER_NAME",
    "LogMode",
    "configure_logging",
    "processing_logger",
]
