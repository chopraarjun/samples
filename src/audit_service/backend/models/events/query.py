"""HTTP query parameter model for listing tenant events.

Main entry points:
    EventQueryParams: Validated filters for tenant-scoped event listing.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from audit_service.backend.models.action import ActionType
from audit_service.backend.models.validators import validate_tenant_id


class EventQueryParams(BaseModel):
    """Query filters for tenant-scoped event listing.

    ``tenant_id`` is required; all other filters are optional.

    Attributes:
        tenant_id: Tenant whose events are listed (required).
        start_time: Inclusive lower bound on ``timestamp``.
        end_time: Inclusive upper bound on ``timestamp``.
        action: Filter by artifact access action.
        package: Filter by package name (exact match).
        limit: Maximum events per page (1–1000).
        offset: Number of matching events to skip before the page.
    """

    tenant_id: str = Field(min_length=1)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    action: Optional[ActionType] = None
    package: Optional[str] = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)

    @field_validator("tenant_id")
    @classmethod
    def tenant_id_format(cls, value: str) -> str:
        """Validate ``tenant_id`` against :func:`~validators.validate_tenant_id`.

        Args:
            value: Raw tenant ID from the query path or body.

        Returns:
            Validated tenant ID.

        Raises:
            ValueError: When the tenant ID format is invalid.
        """
        return validate_tenant_id(value)
