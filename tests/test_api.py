"""Tests for the FastAPI HTTP layer and ASGI entry points."""

from fastapi.testclient import TestClient

from audit_service.backend.db import DuckDBClient
from tests.conftest import seed_event


def test_documented_asgi_app_imports() -> None:
    """Verify ``audit_service.main.app`` is importable for deployment."""
    from audit_service.main import app

    assert app is not None


def test_list_events_with_filters(
    client: TestClient, app_config, db_client: DuckDBClient, pipeline_config
) -> None:
    """Verify time, action, and package filters narrow tenant event results."""
    seed_event(
        db_client,
        app_config,
        pipeline_config,
        "acme_corp",
        "evt_1",
        action="download",
        package="django",
        timestamp="2025-03-14T10:00:00+00:00",
    )
    seed_event(
        db_client,
        app_config,
        pipeline_config,
        "acme_corp",
        "evt_2",
        action="upload",
        package="numpy",
        timestamp="2025-03-15T10:00:00+00:00",
    )

    response = client.get(
        "/tenants/acme_corp/events",
        params={
            "start_time": "2025-03-15T00:00:00+00:00",
            "end_time": "2025-03-15T23:59:59+00:00",
            "action": "upload",
            "package": "numpy",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["events"][0]["event_id"] == "evt_2"


def test_datetime_filters_normalize_offsets(
    client: TestClient,
    app_config,
    db_client: DuckDBClient,
    pipeline_config,
) -> None:
    """Verify offset datetime query params are normalized to UTC for filtering."""
    seed_event(
        db_client,
        app_config,
        pipeline_config,
        "acme_corp",
        "evt_offset",
        timestamp="2025-03-15T10:00:00+00:00",
    )

    response = client.get(
        "/tenants/acme_corp/events",
        params={
            "start_time": "2025-03-15T05:00:00-05:00",
            "end_time": "2025-03-15T05:00:00-05:00",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["events"][0]["event_id"] == "evt_offset"


def test_pagination(
    client: TestClient, app_config, db_client: DuckDBClient, pipeline_config
) -> None:
    """Verify limit and offset return the correct page and total count."""
    for i in range(3):
        seed_event(
            db_client,
            app_config,
            pipeline_config,
            "acme_corp",
            f"evt_{i}",
            timestamp=f"2025-03-15T1{i}:00:00+00:00",
        )

    response = client.get("/tenants/acme_corp/events", params={"limit": 2, "offset": 1})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert len(body["events"]) == 2


def test_invalid_datetime_returns_422(client: TestClient) -> None:
    """Verify malformed datetime query strings return HTTP 422."""
    response = client.get(
        "/tenants/acme_corp/events",
        params={"start_time": "not-a-date"},
    )
    assert response.status_code == 422


def test_invalid_tenant_id_returns_422_with_field_detail(client: TestClient) -> None:
    """Verify invalid tenant IDs return 422 with a field-scoped error message."""
    response = client.get("/tenants/bad-tenant!/events")
    assert response.status_code == 422
    assert "tenant_id:" in response.json()["detail"]


def test_health(client: TestClient) -> None:
    """Verify the health endpoint returns a static OK payload."""
    assert client.get("/health").json() == {"status": "ok"}


def test_actor_filter(
    client: TestClient, app_config, db_client: DuckDBClient, pipeline_config
) -> None:
    """Verify actor filter parameter is accepted and returns results."""
    seed_event(
        db_client,
        app_config,
        pipeline_config,
        "acme_corp",
        "evt_actor_1",
        actor="ci-runner-03",
        timestamp="2025-03-15T10:00:00+00:00",
    )

    response = client.get(
        "/tenants/acme_corp/events",
        params={"actor": "runner"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] >= 1
