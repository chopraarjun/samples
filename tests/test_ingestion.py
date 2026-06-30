"""Tests for JSONL ingest: validation, deduplication, DLQ, and landing batching."""

from pathlib import Path

from audit_service.backend.config import AppConfig, PipelineConfig
from audit_service.backend.db import DuckDBClient
from audit_service.backend.models import EventQueryParams, EventRecord
from audit_service.backend.models.events.table import EventsTable
from audit_service.backend.processing.ingestion import (
    PROCESSED_SUFFIX,
    PROCESSING_SUFFIX,
    ingest_file,
    ingest_landing_dir,
)
from audit_service.backend.storage.parquet_paths import partition_dir, sql_path
from tests.helpers import event_pipeline_config

VALID_EVENT = (
    '{"event_id":"evt_1","tenant_id":"acme_corp","action":"download",'
    '"package":"pkg","version":"1.0","timestamp":"2025-03-15T10:00:00+00:00","actor":"a"}'
)
INVALID_ACTION = (
    '{"event_id":"evt_2","tenant_id":"acme_corp","action":"bad","package":"pkg",'
    '"timestamp":"2025-03-15T10:00:00+00:00","actor":"a"}'
)
INVALID_TIMESTAMP = (
    '{"event_id":"evt_3","tenant_id":"acme_corp","action":"download","package":"pkg",'
    '"timestamp":"","actor":"a"}'
)
DUP_EVENT_LATE = (
    '{"event_id":"evt_dup","tenant_id":"acme_corp","action":"download","package":"pkg",'
    '"timestamp":"2025-03-15T12:00:00+00:00","actor":"a"}'
)
DUP_EVENT_EARLY_A = (
    '{"event_id":"evt_dup","tenant_id":"acme_corp","action":"download","package":"pkg",'
    '"timestamp":"2025-03-15T10:00:00+00:00","actor":"a"}'
)
DUP_EVENT_LATER_UPLOAD = (
    '{"event_id":"evt_dup","tenant_id":"acme_corp","action":"upload","package":"new-pkg",'
    '"timestamp":"2025-03-15T12:00:00+00:00","actor":"late"}'
)


def _app_cfg(tmp_path: Path) -> AppConfig:
    """Build a temporary AppConfig rooted under tmp_path.

    Args:
        tmp_path: Pytest temporary directory.

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


def _pipeline_cfg(**overrides: object) -> PipelineConfig:
    """Build an events pipeline config with optional field overrides.

    Args:
        **overrides: PipelineConfig field overrides.

    Returns:
        Validated pipeline configuration for tests.
    """
    return event_pipeline_config(**overrides)


def _write_landing_file(path: Path, *lines: str) -> None:
    """Write JSONL lines to a landing file.

    Args:
        path: Landing file destination.
        *lines: Line strings joined with newlines.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _stored_events(
    db: DuckDBClient, app: AppConfig, pipeline: PipelineConfig, tenant_id: str
) -> list:
    """List all stored events for a tenant from Parquet storage.

    Args:
        db: DuckDB client for queries.
        app: Application filesystem configuration.
        pipeline: Pipeline configuration.
        tenant_id: Tenant whose events are listed.

    Returns:
        List of :class:`EventRecord` instances for *tenant_id*.
    """
    storage = EventsTable(db, app, pipeline)
    events, _ = storage.list_events(EventQueryParams(tenant_id=tenant_id))
    return events


def test_ingestion_skips_malformed_and_invalid(tmp_path: Path) -> None:
    """Verify malformed JSON and schema violations go to DLQ; valid rows are stored."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    events_file = tmp_path / "batch.jsonl"
    _write_landing_file(
        events_file,
        VALID_EVENT,
        "{malformed",
        INVALID_ACTION,
        INVALID_TIMESTAMP,
    )

    with DuckDBClient.connect() as db:
        summary = ingest_file(events_file, db, app, pipeline)
        events = _stored_events(db, app, pipeline, "acme_corp")
        dlq_files = list(app.pipeline_dlq_dir(pipeline).rglob("*.parquet"))

    assert summary.read == 4
    assert summary.inserted == 1
    assert summary.skipped.malformed == 1
    assert summary.skipped.invalid == 2
    assert summary.dlq_written == 3
    assert len(events) == 1
    assert events[0].event_id == "evt_1"
    assert dlq_files

    with DuckDBClient.connect() as db:
        dlq_glob = sql_path(app.pipeline_dlq_dir(pipeline) / "**" / "*.parquet")
        dlq_rows = db.fetchall(f"""
            SELECT source_file, line_number, reason
            FROM read_parquet('{dlq_glob}')
            ORDER BY line_number
            """)

    assert len(dlq_rows) == 3
    assert all(row[0] == "batch.jsonl" for row in dlq_rows)
    assert [row[1] for row in dlq_rows] == [2, 3, 4]
    assert dlq_rows[0][2] == "malformed_json"
    assert dlq_rows[1][2].startswith("action:")
    assert dlq_rows[2][2].startswith("timestamp:")


def test_events_table_uses_event_record_model() -> None:
    """Verify EventsTable metadata points at EventRecord and expected columns."""
    assert EventsTable.RECORD_MODEL is EventRecord
    assert EventsTable.ID_COLUMN == "event_id"
    assert EventsTable.PARTITION_COLUMNS == ("tenant_id", "event_date")


def test_ingestion_dedupes_by_event_id_earliest_timestamp(tmp_path: Path) -> None:
    """Verify duplicate event IDs within one file keep the earliest timestamp."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    events_file = tmp_path / "batch.jsonl"
    _write_landing_file(
        events_file,
        DUP_EVENT_LATE,
        DUP_EVENT_EARLY_A,
    )

    with DuckDBClient.connect() as db:
        summary = ingest_file(events_file, db, app, pipeline)
        events = _stored_events(db, app, pipeline, "acme_corp")

    assert summary.skipped.duplicate_in_file == 1
    assert len(events) == 1
    assert str(events[0].timestamp).startswith("2025-03-15 10:00:00")


def test_ingestion_cross_file_duplicate_keeps_earliest_record(tmp_path: Path) -> None:
    """Verify a later duplicate event ID in a second file does not overwrite storage."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    first_file = tmp_path / "batch_1.jsonl"
    second_file = tmp_path / "batch_2.jsonl"
    _write_landing_file(
        first_file,
        DUP_EVENT_EARLY_A.replace('"a"}', '"early"}'),
    )
    _write_landing_file(
        second_file,
        DUP_EVENT_LATER_UPLOAD,
    )

    with DuckDBClient.connect() as db:
        first_summary = ingest_file(first_file, db, app, pipeline)
        second_summary = ingest_file(second_file, db, app, pipeline)
        events = _stored_events(db, app, pipeline, "acme_corp")

    data_part = partition_dir(app.pipeline_data_lake_dir(pipeline), "acme_corp", "2025-03-15")
    assert first_summary.inserted == 1
    assert second_summary.inserted == 0
    assert second_summary.skipped.already_in_lake == 1
    assert len(events) == 1
    assert events[0].action.value == "download"
    assert events[0].package == "pkg"
    assert str(events[0].timestamp).startswith("2025-03-15 10:00:00")
    assert events[0].actor == "early"
    assert list(data_part.glob("*.parquet"))


def test_ingest_landing_dir_uses_tracking_markers(tmp_path: Path) -> None:
    """Verify landing batch ingest creates .processed markers and removes .processing."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    landing_dir = app.pipeline_landing_dir(pipeline)
    tracking_dir = app.pipeline_tracking_dir(pipeline)
    _write_landing_file(landing_dir / "batch_a.jsonl", VALID_EVENT)
    _write_landing_file(landing_dir / "batch_b.jsonl", VALID_EVENT.replace("evt_1", "evt_2"))

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    assert batch.files_attempted == 2
    assert batch.files_succeeded == 2
    assert (landing_dir / "batch_a.jsonl").exists()
    assert (landing_dir / "batch_b.jsonl").exists()
    assert (tracking_dir / f"batch_a.jsonl{PROCESSED_SUFFIX}").exists()
    assert (tracking_dir / f"batch_b.jsonl{PROCESSED_SUFFIX}").exists()
    assert not (tracking_dir / f"batch_a.jsonl{PROCESSING_SUFFIX}").exists()


def test_ingest_landing_dir_skips_already_processed(tmp_path: Path) -> None:
    """Verify files with an existing .processed marker are not re-ingested."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    landing_dir = app.pipeline_landing_dir(pipeline)
    tracking_dir = app.pipeline_tracking_dir(pipeline)
    _write_landing_file(landing_dir / "done.jsonl", VALID_EVENT)
    tracking_dir.mkdir(parents=True, exist_ok=True)
    (tracking_dir / f"done.jsonl{PROCESSED_SUFFIX}").touch()

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    assert batch.files_attempted == 0


def test_ingest_landing_dir_respects_batch_size(tmp_path: Path) -> None:
    """Verify ingest_landing_dir processes at most ingest_batch_size files per run."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg(ingest_batch_size=5)
    landing_dir = app.pipeline_landing_dir(pipeline)
    tracking_dir = app.pipeline_tracking_dir(pipeline)
    for index in range(6):
        _write_landing_file(
            landing_dir / f"batch_{index}.jsonl",
            VALID_EVENT.replace("evt_1", f"evt_{index}"),
        )

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    assert batch.files_attempted == 5
    assert batch.files_succeeded == 5
    assert len(list(tracking_dir.glob(f"*{PROCESSED_SUFFIX}"))) == 5
    assert len(list(landing_dir.glob("*.jsonl"))) == 6


def test_ingest_landing_dir_retries_orphaned_processing_marker(tmp_path: Path) -> None:
    """Verify orphaned .processing markers cause the file to be retried."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    landing_dir = app.pipeline_landing_dir(pipeline)
    tracking_dir = app.pipeline_tracking_dir(pipeline)
    _write_landing_file(landing_dir / "orphan.jsonl", VALID_EVENT)
    tracking_dir.mkdir(parents=True, exist_ok=True)
    (tracking_dir / f"orphan.jsonl{PROCESSING_SUFFIX}").touch()

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    assert batch.files_succeeded == 1
    assert (tracking_dir / f"orphan.jsonl{PROCESSED_SUFFIX}").exists()
    assert (landing_dir / "orphan.jsonl").exists()


def test_ingest_batch_summary_reports_inventory_and_completion(tmp_path: Path) -> None:
    """Verify batch summary tracks inventory counts and ingest_complete across runs."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg(ingest_batch_size=2)
    landing_dir = app.pipeline_landing_dir(pipeline)
    for index in range(3):
        _write_landing_file(
            landing_dir / f"batch_{index}.jsonl",
            VALID_EVENT.replace("evt_1", f"evt_{index}"),
        )

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    assert batch.inventory_before.total == 3
    assert batch.inventory_before.pending == 3
    assert batch.files_attempted == 2
    assert batch.inventory_after.processed == 2
    assert batch.inventory_after.pending == 1
    assert batch.ingest_complete is False

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    assert batch.inventory_after.processed == 3
    assert batch.inventory_after.pending == 0
    assert batch.ingest_complete is True


def test_ingestion_summary_reconciles_all_line_categories(tmp_path: Path) -> None:
    """Verify read lines equal inserted + skips + dlq categories."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    seed_file = tmp_path / "seed.jsonl"
    events_file = tmp_path / "batch.jsonl"
    _write_landing_file(seed_file, VALID_EVENT)
    _write_landing_file(
        events_file,
        VALID_EVENT,
        "",
        "{malformed",
        INVALID_ACTION,
        DUP_EVENT_LATE,
        DUP_EVENT_EARLY_A,
    )

    with DuckDBClient.connect() as db:
        ingest_file(seed_file, db, app, pipeline)
        summary = ingest_file(events_file, db, app, pipeline)

    assert summary.read == 6
    assert summary.skipped.blank == 1
    assert summary.skipped.malformed == 1
    assert summary.skipped.invalid == 1
    assert summary.skipped.duplicate_in_file == 1
    assert summary.skipped.already_in_lake == 1
    assert summary.inserted == 1
    assert summary.dlq_written == 2
    assert summary.balanced is True
    assert summary.accounted_for == summary.read


def test_ingest_batch_summary_includes_run_totals(tmp_path: Path) -> None:
    """Verify batch rollup totals reconcile across files in one run."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg(ingest_batch_size=5)
    landing_dir = app.pipeline_landing_dir(pipeline)
    _write_landing_file(landing_dir / "batch_a.jsonl", VALID_EVENT, INVALID_ACTION)
    _write_landing_file(
        landing_dir / "batch_b.jsonl",
        VALID_EVENT.replace("evt_1", "evt_2"),
        "",
        VALID_EVENT.replace("evt_1", "evt_3"),
    )

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    totals = batch.totals
    assert totals.read == 5
    assert totals.inserted == 3
    assert totals.skipped.invalid == 1
    assert totals.skipped.blank == 1
    assert totals.balanced is True


def test_ingest_batch_public_json_omits_landing_file_list(tmp_path: Path) -> None:
    """Verify stdout JSON shows inventory counts without repeating every landing filename."""
    app = _app_cfg(tmp_path)
    pipeline = _pipeline_cfg()
    landing_dir = app.pipeline_landing_dir(pipeline)
    _write_landing_file(landing_dir / "batch_a.jsonl", VALID_EVENT)

    with DuckDBClient.connect() as db:
        batch = ingest_landing_dir(db, app, pipeline)

    payload = batch.public_json()
    assert '"total": 1' in payload
    assert '"processed": 1' in payload
    assert '"processed_files": [' in payload
    assert "batch_a.jsonl" in payload
    assert '"files": [' not in payload
    assert batch.processed_files == ["batch_a.jsonl"]
    assert len(batch.inventory_after.files) == 1
