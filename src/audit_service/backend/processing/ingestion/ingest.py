"""Ingest JSONL landing files: validate, dedupe, DLQ rejects, upsert Parquet.

Uses tracking markers (``.processing`` / ``.processed``) for idempotent file-level
batching. Supports optional ``--watch`` polling with configurable idle interval.

Main entry points:
    ingest_file: Ingest a single landing JSONL file.
    ingest_landing_dir: Process a batch of pending landing files.
    landing_inventory: Snapshot landing file tracking status.
    main: ``audit-ingest`` CLI entry point.
    PROCESSING_SUFFIX, PROCESSED_SUFFIX: Tracking marker filename suffixes.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from pydantic import BaseModel, ValidationError

from audit_service.backend.config import (
    AppConfig,
    PipelineConfig,
    load_app_config,
    load_pipeline_config,
)
from audit_service.backend.db import DuckDBClient
from audit_service.backend.db.tables import storage_table_for
from audit_service.backend.logging import LOG_LEVELS, LogMode, configure_logging
from audit_service.backend.models import (
    FileIngestionResult,
    IngestionBatchSummary,
    IngestionSummary,
    LandingFileSnapshot,
    LandingFileStatus,
    LandingInventorySummary,
    format_validation_error,
)
from audit_service.backend.models.events.record import EventRecord

PROCESSING_SUFFIX = ".processing"
"""Filename suffix for in-flight ingest tracking markers."""

PROCESSED_SUFFIX = ".processed"
"""Filename suffix for completed ingest tracking markers."""


def _marker(tracking_dir: Path, landing_file: Path, suffix: str) -> Path:
    """Build the tracking marker path for a landing file.

    Args:
        tracking_dir: Pipeline tracking directory root.
        landing_file: Landing JSONL file path.
        suffix: Marker suffix (e.g. :data:`PROCESSING_SUFFIX`).

    Returns:
        Path to ``{landing_file.name}{suffix}`` under *tracking_dir*.
    """
    return tracking_dir / f"{landing_file.name}{suffix}"


def _file_status(tracking_dir: Path, landing_path: Path) -> LandingFileStatus:
    """Derive ingest status from tracking marker files.

    Args:
        tracking_dir: Pipeline tracking directory root.
        landing_path: Landing JSONL file to inspect.

    Returns:
        :class:`~audit_service.backend.models.LandingFileStatus` for *landing_path*.
    """
    if _marker(tracking_dir, landing_path, PROCESSED_SUFFIX).exists():
        return LandingFileStatus.PROCESSED
    if _marker(tracking_dir, landing_path, PROCESSING_SUFFIX).exists():
        return LandingFileStatus.PROCESSING
    return LandingFileStatus.PENDING


def landing_inventory(landing_dir: Path, tracking_dir: Path) -> LandingInventorySummary:
    """Snapshot of all landing JSONL files and their ingest status.

    Args:
        landing_dir: Directory containing ``*.jsonl`` landing batches.
        tracking_dir: Directory containing per-file tracking markers.

    Returns:
        :class:`~audit_service.backend.models.LandingInventorySummary` with counts
        and per-file snapshots.
    """
    if not landing_dir.is_dir():
        return LandingInventorySummary()

    files: list[LandingFileSnapshot] = []
    counts = {status: 0 for status in LandingFileStatus}
    for path in sorted(landing_dir.glob("*.jsonl")):
        status = _file_status(tracking_dir, path)
        counts[status] += 1
        files.append(LandingFileSnapshot(filename=path.name, status=status))

    return LandingInventorySummary(
        total=len(files),
        processed=counts[LandingFileStatus.PROCESSED],
        pending=counts[LandingFileStatus.PENDING],
        processing=counts[LandingFileStatus.PROCESSING],
        files=files,
    )


def _log_inventory(logger: logging.Logger, label: str, inventory: LandingInventorySummary) -> None:
    """Log summary counts and per-file debug lines for a landing inventory.

    Args:
        logger: Logger to write to.
        label: Prefix label (e.g. ``"Landing inventory (before)"``).
        inventory: Inventory snapshot to log.
    """
    logger.info(
        "%s: total=%d processed=%d pending=%d processing=%d",
        label,
        inventory.total,
        inventory.processed,
        inventory.pending,
        inventory.processing,
    )
    for snapshot in inventory.files:
        logger.debug("  %s [%s]", snapshot.filename, snapshot.status.value)


def collect_pending_files(landing_dir: Path, tracking_dir: Path) -> list[Path]:
    """List landing ``.jsonl`` files without a ``.processed`` marker.

    Orphaned ``.processing`` markers are retried before fresh pending files.

    Args:
        landing_dir: Directory containing landing JSONL batches.
        tracking_dir: Directory containing tracking markers.

    Returns:
        Sorted list of landing file paths eligible for ingest.
    """
    if not landing_dir.is_dir():
        return []

    orphans: list[Path] = []
    fresh: list[Path] = []
    for path in sorted(landing_dir.glob("*.jsonl")):
        if _file_status(tracking_dir, path) == LandingFileStatus.PROCESSED:
            continue
        if _file_status(tracking_dir, path) == LandingFileStatus.PROCESSING:
            orphans.append(path)
        else:
            fresh.append(path)
    return orphans + fresh


def _read_jsonl(
    path: Path,
    pipeline: PipelineConfig,
    *,
    logger: logging.Logger | None = None,
) -> tuple[IngestionSummary, list[EventRecord], list[tuple[int, str, str]]]:
    """Parse and validate JSONL lines from a landing file.

    Args:
        path: Landing JSONL file to read.
        pipeline: Pipeline config (selects record model and ID column).
        logger: Optional logger for parse progress.

    Returns:
        Tuple of ``(summary, valid_records, dlq_rows)`` where *dlq_rows* are
        ``(line_number, raw_line, reason)`` tuples.
    """
    summary = IngestionSummary()
    table_cls = storage_table_for(pipeline)
    record_model = table_cls.RECORD_MODEL
    id_field = table_cls.ID_COLUMN
    timestamp_field = table_cls.TIMESTAMP_COLUMN
    records_by_id: dict[str, BaseModel] = {}
    dlq_batch: list[tuple[int, str, str]] = []

    if logger:
        logger.info("Reading %s", path.name)

    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            summary.read += 1
            stripped = line.strip()
            if not stripped:
                summary.skipped.blank += 1
                continue

            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                summary.skipped.malformed += 1
                dlq_batch.append((line_number, stripped, "malformed_json"))
                continue

            if not isinstance(payload, dict):
                summary.skipped.invalid += 1
                dlq_batch.append((line_number, stripped, "payload: value is not a JSON object"))
                continue

            try:
                record = record_model.model_validate(payload)
            except ValidationError as exc:
                summary.skipped.invalid += 1
                dlq_batch.append((line_number, stripped, format_validation_error(exc)))
                continue

            record_id = str(getattr(record, id_field))
            existing = records_by_id.get(record_id)
            if existing is None:
                records_by_id[record_id] = record
                continue

            summary.skipped.duplicate_in_file += 1
            records_by_id[record_id] = (
                record
                if getattr(record, timestamp_field) < getattr(existing, timestamp_field)
                else existing
            )

    if logger:
        logger.info(
            "Parsed %s: read=%d unique_event_ids=%d skipped=%s",
            path.name,
            summary.read,
            len(records_by_id),
            summary.skipped.format_compact(),
        )

    return summary, list(records_by_id.values()), dlq_batch


def ingest_file(
    path: Path,
    db: DuckDBClient,
    app: AppConfig,
    pipeline: PipelineConfig,
    *,
    logger: logging.Logger | None = None,
) -> IngestionSummary:
    """Ingest one landing JSONL file into Parquet storage and the DLQ.

    Args:
        path: Landing JSONL file to ingest.
        db: DuckDB client for storage operations.
        app: Application filesystem configuration.
        pipeline: Pipeline configuration.
        logger: Optional logger for progress messages.

    Returns:
        :class:`~audit_service.backend.models.IngestionSummary` for the file.
    """
    summary, records, dlq_batch = _read_jsonl(path, pipeline, logger=logger)
    storage = storage_table_for(pipeline)(db, app, pipeline)

    tenant_ids = {record.tenant_id for record in records}
    existing_ids = storage.existing_ids(
        [str(getattr(record, storage.ID_COLUMN)) for record in records],
        tenant_ids,
    )
    new_records = [
        record for record in records if str(getattr(record, storage.ID_COLUMN)) not in existing_ids
    ]

    if logger:
        logger.info(
            "Upserting %d record(s) from %s (%d new to lake)",
            len(records),
            path.name,
            len(new_records),
        )

    partitions = storage.ingest_records(records)
    if logger and partitions:
        partition_labels = sorted(
            f"{tenant_id}/event_date={event_date}" for tenant_id, event_date in partitions
        )
        logger.info("Wrote partition(s) for %s: %s", path.name, ", ".join(partition_labels))

    summary.dlq_written = storage.insert_dlq(dlq_batch, source_file=path.name)
    if logger and summary.dlq_written:
        logger.info("Wrote %d row(s) to DLQ from %s", summary.dlq_written, path.name)

    summary.skipped.already_in_lake = len(records) - len(new_records)
    summary.inserted = len(new_records)
    return summary


def ingest_landing_dir(
    db: DuckDBClient,
    app: AppConfig,
    pipeline: PipelineConfig,
    *,
    logger: logging.Logger | None = None,
) -> IngestionBatchSummary:
    """Process up to ``ingest_batch_size`` pending landing files with tracking markers.

    Args:
        db: DuckDB client for storage operations.
        app: Application filesystem configuration.
        pipeline: Pipeline configuration including batch size.
        logger: Optional logger for batch progress.

    Returns:
        :class:`~audit_service.backend.models.IngestionBatchSummary` for the run.
    """
    landing_dir = app.pipeline_landing_dir(pipeline)
    tracking_dir = app.pipeline_tracking_dir(pipeline)
    landing_dir.mkdir(parents=True, exist_ok=True)
    tracking_dir.mkdir(parents=True, exist_ok=True)

    batch = IngestionBatchSummary()
    batch.inventory_before = landing_inventory(landing_dir, tracking_dir)

    if logger:
        logger.info(
            "Ingest run started pipeline=%s batch_limit=%d landing_dir=%s",
            pipeline.name,
            pipeline.ingest_batch_size,
            landing_dir,
        )
        _log_inventory(logger, "Landing inventory (before)", batch.inventory_before)

    pending = collect_pending_files(landing_dir, tracking_dir)[: pipeline.ingest_batch_size]
    if logger:
        if pending:
            logger.info(
                "Selected %d file(s) this run: %s",
                len(pending),
                ", ".join(path.name for path in pending),
            )
        else:
            logger.info("No pending landing files to ingest")

    for landing_path in pending:
        batch.files_attempted += 1
        processing = _marker(tracking_dir, landing_path, PROCESSING_SUFFIX)
        processed = _marker(tracking_dir, landing_path, PROCESSED_SUFFIX)

        try:
            if logger:
                logger.info("Processing %s", landing_path.name)
            processing.touch()
            if logger:
                logger.info("Marked %s as processing", landing_path.name)

            summary = ingest_file(landing_path, db, app, pipeline, logger=logger)
            processed.touch()
            batch.files_succeeded += 1
            batch.results.append(
                FileIngestionResult(
                    filename=landing_path.name,
                    status="succeeded",
                    summary=summary,
                )
            )
            if logger:
                logger.info("Completed %s: %s", landing_path.name, summary.reconciliation_line())
                if not summary.balanced:
                    logger.warning(
                        "Line accounting mismatch for %s: read=%d accounted_for=%d",
                        landing_path.name,
                        summary.read,
                        summary.accounted_for,
                    )
        except Exception as exc:
            batch.files_failed += 1
            batch.results.append(
                FileIngestionResult(
                    filename=landing_path.name,
                    status="failed",
                    error=str(exc),
                )
            )
            if logger:
                logger.exception("Failed %s: %s", landing_path.name, exc)
        finally:
            processing.unlink(missing_ok=True)

    batch.inventory_after = landing_inventory(landing_dir, tracking_dir)
    batch.ingest_complete = (
        batch.inventory_after.pending == 0 and batch.inventory_after.processing == 0
    )

    if logger:
        _log_inventory(logger, "Landing inventory (after)", batch.inventory_after)
        logger.info(
            "Ingest run finished: attempted=%d succeeded=%d failed=%d complete=%s",
            batch.files_attempted,
            batch.files_succeeded,
            batch.files_failed,
            batch.ingest_complete,
        )
        if not batch.ingest_complete:
            logger.info(
                "%d file(s) still pending — re-run audit-ingest or schedule another batch",
                batch.inventory_after.pending,
            )
        if batch.processed_files:
            logger.info(
                "Batch processed: %s",
                ", ".join(batch.processed_files),
            )
        if batch.failed_files:
            logger.info("Batch failed: %s", ", ".join(batch.failed_files))
        if batch.totals.read:
            logger.info("Run totals: %s", batch.totals.reconciliation_line())
        logger.info("Batch summary JSON:\n%s", batch.public_json())

    return batch


def resolve_watch_interval(cli_interval: int | None, pipeline: PipelineConfig) -> int:
    """Resolve idle sleep seconds for ``--watch`` mode.

    Args:
        cli_interval: Value from ``--interval`` CLI flag, or ``None``.
        pipeline: Pipeline config providing ``watch_interval_seconds`` default.

    Returns:
        CLI interval when provided, otherwise ``pipeline.watch_interval_seconds``.
    """
    if cli_interval is not None:
        return cli_interval
    return pipeline.watch_interval_seconds


def run_ingest(
    db: DuckDBClient,
    app: AppConfig,
    pipeline: PipelineConfig,
    *,
    logger: logging.Logger | None = None,
    watch: bool = False,
    watch_interval: int = 30,
    sleep=time.sleep,
) -> IngestionBatchSummary:
    """Run one or more ingest batches; in watch mode, sleep after each batch cycle.

    Args:
        db: DuckDB client for storage operations.
        app: Application filesystem configuration.
        pipeline: Pipeline configuration.
        logger: Optional logger for batch progress.
        watch: When ``True``, keep running and sleep between batch cycles.
        watch_interval: Seconds to sleep after each batch (default from config).
        sleep: Injectable sleep function (for tests).

    Returns:
        The last :class:`~audit_service.backend.models.IngestionBatchSummary` produced.
    """
    last_batch: IngestionBatchSummary | None = None
    while True:
        last_batch = ingest_landing_dir(db, app, pipeline, logger=logger)
        print(last_batch.public_json())

        if not watch:
            return last_batch

        try:
            if logger:
                pending = last_batch.inventory_after.pending
                if last_batch.ingest_complete:
                    logger.info(
                        "Watch mode: caught up, sleeping %ds (Ctrl+C to stop)",
                        watch_interval,
                    )
                else:
                    logger.info(
                        "Watch mode: batch done, %d file(s) still pending, sleeping %ds",
                        pending,
                        watch_interval,
                    )
            sleep(watch_interval)
        except KeyboardInterrupt:
            if logger:
                logger.info("Watch mode stopped")
            return last_batch


def parse_ingest_cli(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for ``audit-ingest``.

    Args:
        argv: Argument vector; uses ``sys.argv`` when ``None``.

    Returns:
        Parsed namespace with pipeline, watch, interval, and logging options.
    """
    parser = argparse.ArgumentParser(description="Ingest JSONL files for a pipeline")
    parser.add_argument(
        "pipeline",
        nargs="?",
        default="events",
        help="Pipeline name (config/{name}.config, default: events)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Poll landing repeatedly; sleep after each batch cycle (see --interval / config)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        metavar="SECS",
        help="Idle sleep in --watch mode (overrides watch_interval_seconds in config)",
    )
    parser.add_argument(
        "--log-mode",
        choices=[mode.value for mode in LogMode],
        default=LogMode.BOTH.value,
        help="Where to write ingest logs (default: both)",
    )
    parser.add_argument(
        "--log-level",
        choices=sorted(LOG_LEVELS),
        default="INFO",
        help="Log verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run the ingest CLI with optional watch mode and structured JSON output.

    Args:
        argv: Optional argument vector for testing.

    Raises:
        SystemExit: When ``--interval`` is less than 1 in watch mode.
    """
    args = parse_ingest_cli(argv)
    app = load_app_config()
    pipeline = load_pipeline_config(args.pipeline)
    logger = configure_logging(
        logs_dir=app.logs_dir,
        pipeline_name=pipeline.name,
        command="ingest",
        mode=LogMode(args.log_mode),
        level_name=args.log_level,
    )

    if args.watch and args.interval is not None and args.interval < 1:
        raise SystemExit("--interval must be >= 1")

    watch_interval = resolve_watch_interval(args.interval, pipeline)

    with DuckDBClient.connect() as db:
        run_ingest(
            db,
            app,
            pipeline,
            logger=logger,
            watch=args.watch,
            watch_interval=watch_interval,
        )


if __name__ == "__main__":
    main()
