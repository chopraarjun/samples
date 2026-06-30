"""Event domain models and Parquet table implementation.

Main entry points:
    EventRecord: Validated audit event row.
    EventQueryParams: Tenant-scoped query filters.
    EventsResponse: Paginated API response wrapper.
    EventsTable: Hive-partitioned Parquet storage for events.
"""

from audit_service.backend.models.events.query import EventQueryParams
from audit_service.backend.models.events.record import EventRecord
from audit_service.backend.models.events.response import EventsResponse
from audit_service.backend.models.events.table import EventsTable

__all__ = ["EventQueryParams", "EventRecord", "EventsResponse", "EventsTable"]
