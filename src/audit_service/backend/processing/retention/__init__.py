"""Retention CLI re-exports.

Main entry points:
    run_retention: Delete partitions older than configured retention days.
    main: ``audit-retention`` CLI entry point.
"""

from audit_service.backend.processing.retention.retention import main, run_retention

__all__ = ["main", "run_retention"]
