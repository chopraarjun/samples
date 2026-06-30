"""CLI entry point for partition-level retention cleanup.

Main entry points:
    run_retention: Delete event partitions older than ``retention_days``.
    main: ``audit-retention`` command-line entry point.
"""

from datetime import datetime, timedelta, timezone

from audit_service.backend.config import (
    AppConfig,
    PipelineConfig,
    load_app_config,
    load_pipeline_config,
    parse_pipeline_cli,
)
from audit_service.backend.db import DuckDBClient
from audit_service.backend.db.tables import storage_table_for
from audit_service.backend.models import RetentionSummary


def run_retention(db: DuckDBClient, app: AppConfig, pipeline: PipelineConfig) -> RetentionSummary:
    """Delete ``event_date`` partitions older than ``pipeline.retention_days`` (UTC).

    Args:
        db: DuckDB client for partition row counts.
        app: Application filesystem configuration.
        pipeline: Pipeline settings including ``retention_days``.

    Returns:
        :class:`~audit_service.backend.models.RetentionSummary` with deletion counts.
    """
    summary = RetentionSummary()
    cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=pipeline.retention_days)
    cutoff_dt = datetime.combine(cutoff_date, datetime.min.time())
    storage = storage_table_for(pipeline)(db, app, pipeline)

    rows_deleted, partitions_deleted = storage.delete_events_before(cutoff_dt)
    summary.rows_deleted = rows_deleted
    summary.partitions_deleted = partitions_deleted
    return summary


def main() -> None:
    """Run retention for the pipeline named on the CLI and print JSON summary."""
    pipeline_name = parse_pipeline_cli()
    app = load_app_config()
    pipeline = load_pipeline_config(pipeline_name)
    with DuckDBClient.connect() as db:
        summary = run_retention(db, app, pipeline)
    print(summary.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
