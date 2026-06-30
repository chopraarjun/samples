"""Tests for partition-level retention cleanup."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from audit_service.backend.config import AppConfig, PipelineConfig
from audit_service.backend.db import DuckDBClient
from audit_service.backend.models import EventRecord, EventsTable
from audit_service.backend.processing.retention import run_retention
from audit_service.backend.storage.parquet_paths import partition_dir, sql_path
from tests.helpers import event_pipeline_config


def _seed_event(
    db: DuckDBClient,
    app: AppConfig,
    pipeline: PipelineConfig,
    tenant_id: str,
    event_id: str,
    timestamp: str,
) -> None:
    """Insert one event at the given timestamp for retention tests.

    Args:
        db: DuckDB client for storage writes.
        app: Application filesystem configuration.
        pipeline: Pipeline configuration.
        tenant_id: Tenant partition key.
        event_id: Unique event identifier.
        timestamp: ISO-8601 timestamp string.
    """
    record = EventRecord.model_validate(
        {
            "event_id": event_id,
            "tenant_id": tenant_id,
            "action": "download",
            "package": "pkg",
            "timestamp": timestamp,
            "actor": "actor",
        }
    )
    EventsTable(db, app, pipeline).ingest_records([record])


def test_retention_deletes_old_events_and_partitions(tmp_path: Path) -> None:
    """Verify retention removes old partitions while keeping recent events."""
    app = AppConfig(
        data_lake_dir=tmp_path / "data_lake",
        dlq_dir=tmp_path / "dlq",
        landing_dir=tmp_path / "landing",
        tracking_dir=tmp_path / "tracking",
    )
    pipeline = event_pipeline_config(retention_days=7)
    db = DuckDBClient.connect()

    old_ts = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    recent_ts = datetime.now(timezone.utc).isoformat()
    old_date = datetime.fromisoformat(old_ts.replace("Z", "+00:00")).date().isoformat()
    recent_date = datetime.fromisoformat(recent_ts.replace("Z", "+00:00")).date().isoformat()

    _seed_event(db, app, pipeline, "acme_corp", "evt_old", old_ts)
    _seed_event(db, app, pipeline, "acme_corp", "evt_new", recent_ts)

    summary = run_retention(db, app, pipeline)

    assert summary.rows_deleted >= 1
    assert summary.partitions_deleted >= 1
    glob_path = sql_path(
        app.pipeline_data_lake_dir(pipeline) / "tenant_id=acme_corp" / "**" / "*.parquet"
    )
    remaining = db.fetchall(f"""
        SELECT event_id
        FROM read_parquet('{glob_path}', hive_partitioning=true)
        ORDER BY event_id
        """)
    assert remaining == [("evt_new",)]
    assert not partition_dir(app.pipeline_data_lake_dir(pipeline), "acme_corp", old_date).exists()
    assert partition_dir(app.pipeline_data_lake_dir(pipeline), "acme_corp", recent_date).exists()
    db.close()
