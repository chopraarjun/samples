"""Pipeline storage registry mapping pipeline names to table classes.

Main entry points:
    storage_table_for: Resolve the Parquet storage implementation for a pipeline.
"""

from typing import TYPE_CHECKING

from audit_service.backend.config import PipelineConfig

if TYPE_CHECKING:
    from audit_service.backend.storage.base import BaseTable


def storage_table_for(cfg: PipelineConfig) -> type["BaseTable"]:
    """Return the primary storage class for a pipeline.

    Args:
        cfg: Pipeline configuration whose ``name`` selects the storage class.

    Returns:
        Storage table class (e.g. :class:`~audit_service.backend.models.EventsTable`).

    Raises:
        ValueError: When no storage is registered for ``cfg.name``.
    """
    if cfg.name == "events":
        from audit_service.backend.models.events.table import EventsTable

        return EventsTable
    raise ValueError(f"No storage registered for pipeline {cfg.name!r}")
