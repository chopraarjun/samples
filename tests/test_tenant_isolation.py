"""Tests for tenant isolation in the events listing API."""

from fastapi.testclient import TestClient

from audit_service.backend.db import DuckDBClient
from tests.conftest import seed_event


def test_tenant_isolation_same_package_name(
    client: TestClient, app_config, db_client: DuckDBClient, pipeline_config
) -> None:
    """Verify listing events for one tenant never returns another tenant's rows."""
    seed_event(db_client, app_config, pipeline_config, "tenant_a", "evt_a", package="shared-pkg")
    seed_event(db_client, app_config, pipeline_config, "tenant_b", "evt_b", package="shared-pkg")

    response = client.get("/tenants/tenant_a/events")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["events"][0]["tenant_id"] == "tenant_a"
    assert all(e["tenant_id"] == "tenant_a" for e in body["events"])


def test_invalid_tenant_id_rejected(client: TestClient) -> None:
    """Verify invalid tenant path segments return HTTP 422."""
    response = client.get("/tenants/bad-tenant!/events")
    assert response.status_code == 422


def test_unknown_tenant_returns_empty_list(client: TestClient) -> None:
    """Verify a valid but unknown tenant returns an empty result set."""
    response = client.get("/tenants/unknown_tenant/events")
    assert response.status_code == 200
    assert response.json()["total"] == 0
    assert response.json()["events"] == []
