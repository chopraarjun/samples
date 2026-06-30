"""Shared pytest fixtures for API and storage integration tests.

Fixtures:
    app_config: Temporary filesystem roots for pipeline data.
    pipeline_config: Default events pipeline configuration.
    db_client: Ephemeral DuckDB client.
    client: FastAPI TestClient wired to temporary storage.

Helpers:
    seed_event: Insert a single event into Parquet storage for test setup.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from audit_service.api.app import create_app
from audit_service.backend.config import AppConfig, PipelineConfig
from audit_service.backend.db import DuckDBClient
from audit_service.backend.models import EventRecord, EventsTable
from tests.helpers import event_pipeline_config


@pytest.fixture
def app_config(tmp_path: Path) -> AppConfig:
    """Provide an :class:`AppConfig` rooted under pytest's ``tmp_path``.

    Args:
        tmp_path: Pytest temporary directory fixture.

    Returns:
        Application config with all data dirs under *tmp_path*.
    """
    return AppConfig(
        data_lake_dir=tmp_path / "data_lake",
        dlq_dir=tmp_path / "dlq",
        landing_dir=tmp_path / "landing",
        tracking_dir=tmp_path / "tracking",
        logs_dir=tmp_path / "logs",
    )


@pytest.fixture
def pipeline_config() -> PipelineConfig:
    """Return the default events pipeline config for tests.

    Returns:
        :class:`PipelineConfig` from :func:`tests.helpers.event_pipeline_config`.
    """
    return event_pipeline_config()


@pytest.fixture
def db_client() -> DuckDBClient:
    """Provide a connected DuckDB client for direct storage tests.

    Returns:
        New :class:`DuckDBClient` instance.
    """
    return DuckDBClient.connect()


@pytest.fixture
def client(app_config: AppConfig, pipeline_config: PipelineConfig) -> TestClient:
    """Provide a FastAPI test client backed by temporary Parquet storage.

    Args:
        app_config: Temporary application paths fixture.
        pipeline_config: Pipeline configuration fixture.

    Returns:
        :class:`fastapi.testclient.TestClient` for the audit API.
    """
    app = create_app(app_config, pipeline_config)
    return TestClient(app)


def seed_event(
    db: DuckDBClient,
    app: AppConfig,
    pipeline: PipelineConfig,
    tenant_id: str,
    event_id: str,
    *,
    package: str = "pkg-a",
    action: str = "download",
    timestamp: str = "2025-03-15T10:00:00+00:00",
    actor: str = "actor-1",
    version: str = "1.0.0",
) -> EventRecord:
    """Insert a single validated event into Parquet storage for test setup.

    Args:
        db: DuckDB client for storage writes.
        app: Application filesystem configuration.
        pipeline: Pipeline configuration.
        tenant_id: Tenant partition key.
        event_id: Unique event identifier.
        package: Package name on the event.
        action: Action string (download, upload, delete).
        timestamp: ISO-8601 timestamp string.
        actor: Actor identifier on the event.
        version: Optional artifact version.

    Returns:
        The ingested :class:`EventRecord`.
    """
    record = EventRecord.model_validate(
        {
            "event_id": event_id,
            "tenant_id": tenant_id,
            "action": action,
            "package": package,
            "version": version,
            "timestamp": timestamp,
            "actor": actor,
        }
    )
    EventsTable(db, app, pipeline).ingest_records([record])
    return record
