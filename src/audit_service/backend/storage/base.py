"""Abstract base for pipeline-specific Parquet storage tables.

Main entry points:
    BaseTable: Protocol-like marker base requiring a ``RECORD_MODEL`` class attribute.
"""

from pydantic import BaseModel

from audit_service.backend.config import AppConfig, PipelineConfig

if False:  # TYPE_CHECKING stand-in for EventsTable
    from audit_service.backend.db import DuckDBClient


class BaseTable:
    """Marker base for pipeline storage classes.

    Subclasses must define :attr:`RECORD_MODEL` and implement Parquet I/O.

    Class attributes:
        RECORD_MODEL: Pydantic model class for validated storage rows.
    """

    RECORD_MODEL: type[BaseModel]

    def __init__(self, db: "DuckDBClient", app: AppConfig, cfg: PipelineConfig) -> None:
        """Subclasses must override initialization and storage methods.

        Args:
            db: DuckDB client for SQL operations.
            app: Application filesystem configuration.
            cfg: Pipeline-specific settings.

        Raises:
            NotImplementedError: Always raised on the base class.
        """
        raise NotImplementedError
