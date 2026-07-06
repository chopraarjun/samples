"""FastAPI read-only query service for tenant-scoped audit events.

Main entry points:
    create_app: Build the FastAPI application with health and event listing routes.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Optional

from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import ValidationError

from audit_service.backend.config import (
    AppConfig,
    PipelineConfig,
    load_app_config,
    load_pipeline_config,
)
from audit_service.backend.db import DuckDBClient
from audit_service.backend.db.tables import storage_table_for
from audit_service.backend.models import (
    ActionType,
    EventQueryParams,
    EventsResponse,
    format_validation_error,
)


def _parse_optional_datetime(value: Optional[str], field_name: str) -> Optional[datetime]:
    """Parse an optional ISO-8601 datetime query parameter to naive UTC.

    Args:
        value: Raw query string or ``None`` when the parameter is omitted.
        field_name: Parameter name used in 422 error messages.

    Returns:
        Parsed naive UTC datetime, or ``None`` when *value* is ``None``.

    Raises:
        HTTPException: When *value* is present but not a valid ISO-8601 datetime.
    """
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid {field_name}: must be ISO-8601 datetime",
        ) from exc
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


TenantIdPath = Annotated[
    str,
    Path(
        description="Tenant identifier (alphanumeric and underscore). Try `acme_corp`.",
        example="acme_corp",
        examples={
            "acme_corp": {"summary": "Acme Corp (default)", "value": "acme_corp"},
            "globex_inc": {"summary": "Globex Inc", "value": "globex_inc"},
            "initech": {"summary": "Initech", "value": "initech"},
        },
    ),
]

StartTimeQuery = Annotated[
    Optional[str],
    Query(
        description=(
            "Inclusive lower bound on event timestamp (ISO-8601). "
            "Leave empty for default: no lower bound."
        ),
        examples={
            "day_start": {
                "summary": "Start of day (UTC)",
                "value": "2025-03-15T00:00:00+00:00",
            },
        },
    ),
]

EndTimeQuery = Annotated[
    Optional[str],
    Query(
        description=(
            "Inclusive upper bound on event timestamp (ISO-8601). "
            "Leave empty for default: no upper bound."
        ),
        examples={
            "day_end": {
                "summary": "End of day (UTC)",
                "value": "2025-03-15T23:59:59+00:00",
            },
        },
    ),
]

ActionQuery = Annotated[
    Optional[ActionType],
    Query(
        description=(
            "Filter by artifact access action. "
            "Leave empty for default: all actions."
        ),
        examples={
            "download": {"summary": "Downloads only", "value": "download"},
            "upload": {"summary": "Uploads only", "value": "upload"},
            "delete": {"summary": "Deletes only", "value": "delete"},
        },
    ),
]

PackageQuery = Annotated[
    Optional[str],
    Query(
        description=(
            "Filter by package name (exact match). "
            "Leave empty for default: all packages."
        ),
        examples={
            "requests": {"summary": "Python requests", "value": "requests"},
            "globex_api": {"summary": "Globex API", "value": "globex-api"},
        },
    ),
]

ActorQuery = Annotated[
    Optional[str],
    Query(
        description=(
            "Filter by actor name (partial match). "
            "Leave empty for default: all actors."
        ),
        examples={
            "runner": {"summary": "CI runners", "value": "runner"},
            "admin": {"summary": "Admin users", "value": "admin"},
        },
    ),
]

LimitQuery = Annotated[
    int,
    Query(
        ge=1,
        le=1000,
        description="Maximum events per page (default `100`, allowed 1–1000).",
        examples={
            "default": {"summary": "Default page size", "value": 100},
            "ten": {"summary": "10 events", "value": 10},
        },
    ),
]

OffsetQuery = Annotated[
    int,
    Query(
        ge=0,
        description="Pagination offset (default `0`).",
        examples={
            "first_page": {"summary": "First page", "value": 0},
            "second_page": {"summary": "Second page (after 10)", "value": 10},
        },
    ),
]


def create_app(
    app_config: AppConfig | None = None,
    pipeline_config: PipelineConfig | None = None,
) -> FastAPI:
    """Build the FastAPI app with health check and tenant event listing routes.

    Wires DuckDB-backed Parquet storage for the configured pipeline and closes
    the database connection on application shutdown.

    Args:
        app_config: Filesystem roots and paths; defaults to ``config/app.config``.
        pipeline_config: Pipeline settings; defaults to ``config/events.config``.

    Returns:
        Configured :class:`fastapi.FastAPI` instance with ``/health`` and
        ``/tenants/{tenant_id}/events`` routes.
    """
    app_cfg = app_config or load_app_config()
    pipeline_cfg = pipeline_config or load_pipeline_config("events")
    db = DuckDBClient.connect()
    storage = storage_table_for(pipeline_cfg)(db, app_cfg, pipeline_cfg)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        db.close()

    app = FastAPI(
        title="Artifact Access Audit Service",
        description=(
            "Read-only query API over ingested audit events. "
            "Use **Try it out** on `/tenants/{tenant_id}/events` — parameters include "
            "sample values for tenants in `events.jsonl` (`acme_corp`, `globex_inc`, `initech`)."
        ),
        lifespan=lifespan,
    )
    app.state.app_config = app_cfg
    app.state.pipeline_config = pipeline_cfg
    app.state.db = db

    @app.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        """Liveness probe returning a static OK payload.

        Returns:
            Mapping with ``status`` set to ``"ok"``.
        """
        return {"status": "ok"}

    @app.get(
        "/tenants/{tenant_id}/events",
        response_model=EventsResponse,
        tags=["events"],
        summary="List events for one tenant",
        responses={
            200: {
                "description": "Paginated event list",
                "content": {
                    "application/json": {
                        "example": {
                            "events": [
                                {
                                    "event_id": "evt_1",
                                    "tenant_id": "acme_corp",
                                    "action": "download",
                                    "package": "requests",
                                    "version": "2.32.0",
                                    "timestamp": "2025-03-15T10:00:00+00:00",
                                    "actor": "ci-runner-03",
                                }
                            ],
                            "total": 1,
                            "limit": 10,
                            "offset": 0,
                        }
                    }
                },
            }
        },
    )
    def list_tenant_events(
        tenant_id: TenantIdPath,
        start_time: StartTimeQuery = None,
        end_time: EndTimeQuery = None,
        action: ActionQuery = None,
        package: PackageQuery = None,
        actor: ActorQuery = None,
        limit: LimitQuery = 100,
        offset: OffsetQuery = 0,
    ) -> EventsResponse:
        """List paginated audit events for a single tenant from Parquet storage.

        **Parameter defaults** (also shown in each field's description in Swagger):

        | Parameter | Default if omitted | Effect |
        |-----------|-------------------|--------|
        | `start_time` | *(none)* | No lower time bound |
        | `end_time` | *(none)* | No upper time bound |
        | `action` | *(none)* | All actions |
        | `package` | *(none)* | All packages |
        | `limit` | `100` | Page size (max 1000) |
        | `offset` | `0` | Start at first matching event |

        Clear optional query fields (start/end/action/package) to use defaults.
        Use each field's **Examples** dropdown to pick a sample value (same as `tenant_id`).
        """
        try:
            params = EventQueryParams(
                tenant_id=tenant_id,
                start_time=_parse_optional_datetime(start_time, "start_time"),
                end_time=_parse_optional_datetime(end_time, "end_time"),
                action=action,
                package=package,
                actor=actor,
                limit=limit,
                offset=offset,
            )
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=format_validation_error(exc)) from exc

        events, total = storage.list_events(params)
        return EventsResponse(events=events, total=total, limit=limit, offset=offset)

    return app
