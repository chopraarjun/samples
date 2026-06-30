"""Canonical validated event row — shared by JSONL ingest and API responses.

Main entry points:
    EventRecord: Pydantic model for a single artifact access audit event.
"""

from datetime import date, datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from audit_service.backend.models.action import ActionType
from audit_service.backend.models.validators import validate_tenant_id


class EventRecord(BaseModel):
    """One artifact access audit event after JSON validation.

    Attributes:
        event_id: Unique event identifier within a tenant.
        tenant_id: Owning tenant (alphanumeric and underscore).
        action: Download, upload, or delete.
        package: Package or artifact name accessed.
        timestamp: Event time (stored as naive UTC).
        actor: User or service principal that performed the action.
        version: Optional artifact version string.
    """

    event_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    action: ActionType
    package: str = Field(min_length=1)
    timestamp: datetime
    actor: str = Field(min_length=1)
    version: Optional[str] = None

    @field_validator("tenant_id")
    @classmethod
    def tenant_id_format(cls, value: str) -> str:
        """Validate ``tenant_id`` against :func:`~validators.validate_tenant_id`.

        Args:
            value: Raw tenant ID from JSON input.

        Returns:
            Validated tenant ID.

        Raises:
            ValueError: When the tenant ID format is invalid.
        """
        return validate_tenant_id(value)

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value: object) -> datetime:
        """Parse ISO-8601 timestamps and normalize to naive UTC.

        Args:
            value: Datetime instance or ISO-8601 string (``Z`` suffix allowed).

        Returns:
            Naive UTC :class:`datetime`.

        Raises:
            ValueError: When *value* cannot be parsed as a datetime.
        """
        if isinstance(value, datetime):
            dt = value
        else:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    @property
    def event_date(self) -> date:
        """Return the calendar date of :attr:`timestamp` for Hive partitioning.

        Returns:
            Date portion of the event timestamp.
        """
        return self.timestamp.date()

    @property
    def dt(self) -> str:
        """Return ``event_date`` as an ISO-8601 date string.

        Returns:
            ``YYYY-MM-DD`` partition key string.
        """
        return self.event_date.isoformat()

    def to_db_params(self) -> list[object]:
        """Serialize the record to positional SQL insert parameters.

        Returns:
            List of values in storage column order (excluding ``event_date``).
        """
        return [
            self.event_id,
            self.tenant_id,
            self.action.value,
            self.package,
            self.version,
            self.timestamp,
            self.actor,
        ]

    @classmethod
    def from_db_row(cls, row: tuple[object, ...]) -> "EventRecord":
        """Construct a record from a DuckDB query result row.

        Args:
            row: Tuple of ``event_id, tenant_id, action, package, version,
                timestamp, actor``.

        Returns:
            Validated :class:`EventRecord` instance.
        """
        return cls(
            event_id=str(row[0]),
            tenant_id=str(row[1]),
            action=row[2],  # type: ignore[arg-type]
            package=str(row[3]),
            version=row[4] if row[4] is not None else None,
            timestamp=row[5],  # type: ignore[arg-type]
            actor=str(row[6]),
        )
