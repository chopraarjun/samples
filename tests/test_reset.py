"""Tests for pipeline reset: lake, DLQ, tracking markers, and landing cleanup."""

from audit_service.backend.db import DuckDBClient
from audit_service.backend.models import EventQueryParams
from audit_service.backend.models.events.table import EventsTable
from audit_service.backend.processing.ingestion import PROCESSED_SUFFIX, ingest_landing_dir
from audit_service.backend.processing.reset import reset_pipeline_data
from tests.conftest import seed_event
from tests.helpers import event_pipeline_config

VALID_EVENT_LINE = (
    '{"event_id":"evt_t","tenant_id":"acme_corp","action":"download",'
    '"package":"pkg","timestamp":"2025-03-15T10:00:00+00:00","actor":"a"}'
)


def _event_count(db: DuckDBClient, app, pipeline) -> int:
    """Return total stored events for acme_corp in the test data lake.

    Args:
        db: DuckDB client for queries.
        app: Application filesystem configuration.
        pipeline: Pipeline configuration.

    Returns:
        Total event count for tenant ``acme_corp``.
    """
    storage = EventsTable(db, app, pipeline)
    _, total = storage.list_events(EventQueryParams(tenant_id="acme_corp"))
    return total


def test_reset_on_empty_lake_dirs(app_config, pipeline_config, db_client: DuckDBClient) -> None:
    """Verify reset succeeds when lake folders exist but contain no Parquet files."""
    lake = app_config.pipeline_data_lake_dir(pipeline_config)
    lake.mkdir(parents=True, exist_ok=True)
    (lake / "tenant_id=acme_corp" / "event_date=2025-03-15").mkdir(parents=True)

    summary = reset_pipeline_data(db_client, app_config, pipeline_config)

    assert summary.rows_deleted == 0
    assert lake.is_dir()


def test_reset_clears_lake_dlq_and_all_markers(
    app_config, pipeline_config, db_client: DuckDBClient
) -> None:
    """Verify reset removes lake rows, DLQ files, and tracking markers."""
    seed_event(db_client, app_config, pipeline_config, "acme_corp", "evt_1")
    landing = app_config.pipeline_landing_dir(pipeline_config)
    landing.mkdir(parents=True, exist_ok=True)
    (landing / "batch.jsonl").write_text(VALID_EVENT_LINE + "\n", encoding="utf-8")
    ingest_landing_dir(db_client, app_config, pipeline_config)

    summary = reset_pipeline_data(db_client, app_config, pipeline_config)

    assert _event_count(db_client, app_config, pipeline_config) == 0
    assert summary.rows_deleted >= 1
    assert summary.tracking_markers_removed >= 1
    assert not list(app_config.pipeline_dlq_dir(pipeline_config).rglob("*.parquet"))
    assert not list(app_config.pipeline_tracking_dir(pipeline_config).glob(f"*{PROCESSED_SUFFIX}"))
    assert (landing / "batch.jsonl").exists()


def test_reset_allows_reingest(app_config, pipeline_config, db_client: DuckDBClient) -> None:
    """Verify reset clears tracking markers so the same landing file can be re-ingested."""
    landing_file = app_config.pipeline_landing_dir(pipeline_config) / "batch.jsonl"
    landing_file.parent.mkdir(parents=True, exist_ok=True)
    landing_file.write_text(VALID_EVENT_LINE + "\n", encoding="utf-8")

    ingest_landing_dir(db_client, app_config, pipeline_config)
    assert (
        app_config.pipeline_tracking_dir(pipeline_config) / f"batch.jsonl{PROCESSED_SUFFIX}"
    ).exists()

    reset_pipeline_data(db_client, app_config, pipeline_config)

    batch = ingest_landing_dir(db_client, app_config, pipeline_config)
    assert batch.files_succeeded == 1
    assert _event_count(db_client, app_config, pipeline_config) == 1


def test_reset_with_landing_removes_batch_files(
    app_config, pipeline_config, db_client: DuckDBClient
) -> None:
    """Verify reset with clear_landing deletes batch_*.jsonl landing files."""
    landing = app_config.pipeline_landing_dir(pipeline_config)
    landing.mkdir(parents=True, exist_ok=True)
    (landing / "batch_0001.jsonl").write_text("{}\n", encoding="utf-8")
    tracking = app_config.pipeline_tracking_dir(pipeline_config)
    tracking.mkdir(parents=True, exist_ok=True)
    (tracking / f"batch_0001.jsonl{PROCESSED_SUFFIX}").touch()

    summary = reset_pipeline_data(
        db_client,
        app_config,
        pipeline_config,
        clear_landing=True,
    )

    assert summary.landing_batches_removed == 1
    assert not (landing / "batch_0001.jsonl").exists()
    assert not (tracking / f"batch_0001.jsonl{PROCESSED_SUFFIX}").exists()


def test_reset_main_aborts_when_not_confirmed(monkeypatch) -> None:
    """Verify reset CLI exits when the user declines confirmation."""
    import pytest

    from audit_service.backend.processing.reset.cli import main

    monkeypatch.setattr(
        "audit_service.backend.processing.reset.cli._confirm_reset",
        lambda _app, _pipeline, *, clear_landing=False: False,
    )

    with pytest.raises(SystemExit, match="Aborted"):
        main(["events"])


def test_reset_main_wipes_with_yes(app_config, pipeline_config, tmp_path, monkeypatch) -> None:
    """Verify reset CLI with -y clears ingested data without prompting."""
    from audit_service.backend.config import AppConfig
    from audit_service.backend.processing.ingestion.ingest import ingest_landing_dir
    from audit_service.backend.processing.reset.cli import main

    app = AppConfig(
        data_lake_dir=tmp_path / "data_lake",
        dlq_dir=tmp_path / "dlq",
        landing_dir=tmp_path / "landing",
        tracking_dir=tmp_path / "tracking",
    )
    pipeline = event_pipeline_config()
    landing_dir = app.pipeline_landing_dir(pipeline)
    landing_dir.mkdir(parents=True, exist_ok=True)
    (landing_dir / "batch.jsonl").write_text(VALID_EVENT_LINE + "\n", encoding="utf-8")

    monkeypatch.setattr("audit_service.backend.processing.reset.cli.load_app_config", lambda: app)
    monkeypatch.setattr(
        "audit_service.backend.processing.reset.cli.load_pipeline_config",
        lambda _name: pipeline,
    )

    with DuckDBClient.connect() as db:
        ingest_landing_dir(db, app, pipeline)
        assert _event_count(db, app, pipeline) == 1

    main(["events", "-y"])

    with DuckDBClient.connect() as db:
        assert _event_count(db, app, pipeline) == 0
