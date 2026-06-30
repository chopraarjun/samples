"""Tests for DuckDB client behavior and Parquet partition creation."""

from pathlib import Path

import duckdb
import pytest

from audit_service.backend.config import AppConfig, PipelineConfig
from audit_service.backend.db import DuckDBClient
from audit_service.backend.models import EventRecord
from audit_service.backend.models.events.table import EventsTable
from audit_service.backend.storage.parquet_paths import partition_dir
from tests.helpers import event_pipeline_config


def _configs(tmp_path: Path) -> tuple[AppConfig, PipelineConfig]:
    """Build temporary app and pipeline configs for storage tests.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Tuple of ``(AppConfig, PipelineConfig)``.
    """
    app = AppConfig(
        data_lake_dir=tmp_path / "data_lake",
        dlq_dir=tmp_path / "dlq",
        landing_dir=tmp_path / "landing",
        tracking_dir=tmp_path / "tracking",
    )
    pipeline = event_pipeline_config()
    return app, pipeline


def test_ingest_creates_parquet_partition(tmp_path: Path) -> None:
    """Verify ingesting a record creates a hive-partitioned Parquet directory."""
    app, pipeline = _configs(tmp_path)
    record = EventRecord.model_validate(
        {
            "event_id": "evt_1",
            "tenant_id": "acme_corp",
            "action": "download",
            "package": "pkg",
            "timestamp": "2025-03-15T10:00:00+00:00",
            "actor": "a",
        }
    )
    with DuckDBClient.connect() as db:
        EventsTable(db, app, pipeline).ingest_records([record])

    part = partition_dir(app.pipeline_data_lake_dir(pipeline), "acme_corp", "2025-03-15")
    assert part.is_dir()
    assert list(part.glob("*.parquet"))


def test_duckdb_client_transaction_rollback() -> None:
    """Verify transaction context manager rolls back on exception."""
    db = DuckDBClient.connect()
    try:
        with pytest.raises(RuntimeError):
            with db.transaction():
                db.execute("CREATE TEMP TABLE t AS SELECT 1 AS id")
                raise RuntimeError("force rollback")
        with pytest.raises(duckdb.CatalogException):
            db.fetchone("SELECT COUNT(*) FROM t")
    finally:
        db.close()
