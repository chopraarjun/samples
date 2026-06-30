"""Tests for processing command logging and landing inventory reporting."""

from pathlib import Path

from audit_service.backend.config import AppConfig
from audit_service.backend.logging import LogMode, configure_logging
from audit_service.backend.processing.ingestion.ingest import (
    PROCESSED_SUFFIX,
    PROCESSING_SUFFIX,
    landing_inventory,
)
from tests.helpers import event_pipeline_config


def test_configure_logging_writes_file(tmp_path: Path) -> None:
    """Verify FILE log mode creates a timestamped log file with messages."""
    logger = configure_logging(
        logs_dir=tmp_path / "logs",
        pipeline_name="events",
        command="ingest",
        mode=LogMode.FILE,
        level_name="INFO",
    )
    logger.info("test message")

    log_files = list((tmp_path / "logs" / "events").glob("ingest_*.log"))
    assert len(log_files) == 1
    assert "test message" in log_files[0].read_text(encoding="utf-8")


def test_configure_logging_off_disables_output(tmp_path: Path, capsys) -> None:
    """Verify OFF log mode suppresses console output and file creation."""
    logger = configure_logging(
        logs_dir=tmp_path / "logs",
        pipeline_name="events",
        command="ingest",
        mode=LogMode.OFF,
    )
    logger.info("hidden")
    assert capsys.readouterr().out == ""
    assert not list((tmp_path / "logs").rglob("*.log"))


def test_landing_inventory_reports_statuses(tmp_path: Path) -> None:
    """Verify landing inventory counts processed, pending, and processing files."""
    app = AppConfig(
        data_lake_dir=tmp_path / "data_lake",
        dlq_dir=tmp_path / "dlq",
        landing_dir=tmp_path / "landing",
        tracking_dir=tmp_path / "tracking",
        logs_dir=tmp_path / "logs",
    )
    pipeline = event_pipeline_config()
    landing_dir = app.pipeline_landing_dir(pipeline)
    tracking_dir = app.pipeline_tracking_dir(pipeline)
    landing_dir.mkdir(parents=True)
    tracking_dir.mkdir(parents=True)

    (landing_dir / "done.jsonl").write_text("{}\n", encoding="utf-8")
    (landing_dir / "waiting.jsonl").write_text("{}\n", encoding="utf-8")
    (landing_dir / "active.jsonl").write_text("{}\n", encoding="utf-8")
    (tracking_dir / f"done.jsonl{PROCESSED_SUFFIX}").touch()
    (tracking_dir / f"active.jsonl{PROCESSING_SUFFIX}").touch()

    inventory = landing_inventory(landing_dir, tracking_dir)

    assert inventory.total == 3
    assert inventory.processed == 1
    assert inventory.pending == 1
    assert inventory.processing == 1
    by_name = {item.filename: item.status.value for item in inventory.files}
    assert by_name["done.jsonl"] == "processed"
    assert by_name["waiting.jsonl"] == "pending"
    assert by_name["active.jsonl"] == "processing"
