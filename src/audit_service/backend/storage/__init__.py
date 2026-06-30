"""Storage abstractions and Parquet path helpers.

Main entry points:
    BaseTable: Marker base class for pipeline-specific Parquet tables.
"""

from audit_service.backend.storage.base import BaseTable

__all__ = ["BaseTable"]
