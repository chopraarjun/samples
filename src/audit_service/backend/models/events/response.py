"""Paginated API response wrapper for event queries.

Main entry points:
    EventsResponse: Page of events plus total count for pagination.
"""

from pydantic import BaseModel

from audit_service.backend.models.events.record import EventRecord


class EventsResponse(BaseModel):
    """Events page plus total count for pagination headers.

    Attributes:
        events: Matching :class:`EventRecord` rows for the current page.
        total: Total number of events matching filters (ignoring limit/offset).
        limit: Page size echoed from the request.
        offset: Skip count echoed from the request.
    """

    events: list[EventRecord]
    total: int
    limit: int
    offset: int
