"""Tests for ingest watch mode and watch interval resolution."""

from pathlib import Path

from audit_service.backend.config import AppConfig
from audit_service.backend.db import DuckDBClient
from audit_service.backend.processing.ingestion.ingest import resolve_watch_interval, run_ingest
from tests.helpers import event_pipeline_config


def _write_landing(path: Path, *lines: str) -> None:
    """Write JSONL lines to a landing file, creating parent directories.

    Args:
        path: Destination landing file path.
        *lines: Line strings to join with newlines.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


VALID_EVENT = (
    '{"event_id":"evt_w","tenant_id":"acme_corp","action":"download",'
    '"package":"pkg","timestamp":"2025-03-15T10:00:00+00:00","actor":"a"}'
)


def test_resolve_watch_interval_prefers_cli() -> None:
    """Verify CLI --interval overrides pipeline watch_interval_seconds."""
    pipeline = event_pipeline_config(watch_interval_seconds=60)
    assert resolve_watch_interval(10, pipeline) == 10
    assert resolve_watch_interval(None, pipeline) == 60


def test_run_ingest_watch_sleeps_after_each_batch(tmp_path: Path) -> None:
    """Verify watch mode sleeps after every batch, not only when the backlog is empty."""
    app = AppConfig(
        data_lake_dir=tmp_path / "data_lake",
        dlq_dir=tmp_path / "dlq",
        landing_dir=tmp_path / "landing",
        tracking_dir=tmp_path / "tracking",
        logs_dir=tmp_path / "logs",
    )
    pipeline = event_pipeline_config(ingest_batch_size=1)
    landing = app.pipeline_landing_dir(pipeline)
    _write_landing(landing / "batch_0001.jsonl", VALID_EVENT)
    _write_landing(landing / "batch_0002.jsonl", VALID_EVENT)

    sleep_calls: list[int] = []

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)
        raise KeyboardInterrupt

    with DuckDBClient.connect() as db:
        run_ingest(
            db,
            app,
            pipeline,
            watch=True,
            watch_interval=5,
            sleep=fake_sleep,
        )

    assert sleep_calls == [5]


def test_run_ingest_watch_sleeps_between_batch_limit_cycles(tmp_path: Path) -> None:
    """Verify watch sleeps after each ingest_batch_size chunk when backlog remains."""
    app = AppConfig(
        data_lake_dir=tmp_path / "data_lake",
        dlq_dir=tmp_path / "dlq",
        landing_dir=tmp_path / "landing",
        tracking_dir=tmp_path / "tracking",
        logs_dir=tmp_path / "logs",
    )
    pipeline = event_pipeline_config(ingest_batch_size=2)
    landing = app.pipeline_landing_dir(pipeline)
    for index in range(3):
        _write_landing(landing / f"batch_{index:04d}.jsonl", VALID_EVENT)

    sleep_calls: list[int] = []

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) == 2:
            raise KeyboardInterrupt

    with DuckDBClient.connect() as db:
        summary = run_ingest(
            db,
            app,
            pipeline,
            watch=True,
            watch_interval=30,
            sleep=fake_sleep,
        )

    assert sleep_calls == [30, 30]
    assert summary.files_attempted == 1
    assert summary.inventory_after.pending == 0


def test_run_ingest_watch_uses_config_interval(tmp_path: Path) -> None:
    """Verify watch interval falls back to pipeline config when CLI omits --interval."""
    pipeline = event_pipeline_config(watch_interval_seconds=42)
    assert resolve_watch_interval(None, pipeline) == 42
