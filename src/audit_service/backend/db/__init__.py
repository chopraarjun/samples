"""DuckDB client package.

Main entry points:
    DuckDBClient: Ephemeral in-memory SQL engine for Parquet I/O.
"""

from audit_service.backend.db.client import DuckDBClient

__all__ = ["DuckDBClient"]
