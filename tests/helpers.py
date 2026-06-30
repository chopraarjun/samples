"""Test helpers for constructing pipeline configuration objects.

Main entry points:
    event_pipeline_config: Build a :class:`PipelineConfig` for the events pipeline.
"""

from audit_service.backend.config import PipelineConfig


def event_pipeline_config(**overrides: object) -> PipelineConfig:
    """Build a default events :class:`PipelineConfig` with optional overrides.

    Args:
        **overrides: Field values to merge over the defaults.

    Returns:
        Validated pipeline config suitable for unit and integration tests.
    """
    data = {
        "name": "events",
        "ingest_batch_size": 5,
        "retention_days": 90,
        "watch_interval_seconds": 30,
    }
    data.update(overrides)
    return PipelineConfig.model_validate(data)
