"""Tests for application and pipeline configuration loading."""

import pytest

from audit_service.backend.config import PipelineConfig, load_app_config, load_pipeline_config


def test_load_app_config() -> None:
    """Verify default app config paths end with expected data directory names."""
    app = load_app_config()
    assert str(app.data_lake_dir).replace("\\", "/").endswith("data/data_lake")
    assert str(app.dlq_dir).replace("\\", "/").endswith("data/dlq")
    assert str(app.landing_dir).replace("\\", "/").endswith("data/landing")
    assert str(app.tracking_dir).replace("\\", "/").endswith("data/tracking")
    assert str(app.logs_dir).replace("\\", "/").endswith("data/logs")


def test_load_events_config() -> None:
    """Verify the bundled events pipeline config loads with expected defaults."""
    cfg = load_pipeline_config("events")
    assert cfg.name == "events"
    assert cfg.ingest_batch_size == 5
    assert cfg.retention_days == 90
    assert cfg.watch_interval_seconds == 30


def test_pipeline_config_requires_pipeline_contract() -> None:
    """Verify PipelineConfig rejects construction without a name field."""
    with pytest.raises(ValueError):
        PipelineConfig()
