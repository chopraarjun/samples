"""Reset pipeline storage: data lake, DLQ, tracking markers, optional landing batches.

Main entry points:
    reset_pipeline_data: Programmatic reset with optional landing cleanup.
    main: ``audit-reset`` CLI entry point.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from audit_service.backend.config import (
    AppConfig,
    PipelineConfig,
    load_app_config,
    load_pipeline_config,
)
from audit_service.backend.db import DuckDBClient
from audit_service.backend.db.tables import storage_table_for
from audit_service.backend.logging import LOG_LEVELS, LogMode, configure_logging
from audit_service.backend.models import ResetSummary


def _reset_scope_description(
    app: AppConfig,
    pipeline: PipelineConfig,
    *,
    clear_landing: bool,
) -> str:
    """Build a human-readable list of paths and effects for reset confirmation/output.

    Args:
        app: Application filesystem configuration.
        pipeline: Pipeline being reset.
        clear_landing: Whether ``--landing`` batch files are included.

    Returns:
        Multi-line formatted string describing what will be deleted and what is kept.
    """
    lake = app.pipeline_data_lake_dir(pipeline)
    tmp = app.pipeline_data_lake_tmp_dir(pipeline)
    dlq = app.pipeline_dlq_dir(pipeline)
    tracking = app.pipeline_tracking_dir(pipeline)
    landing = app.pipeline_landing_dir(pipeline)

    lines = [
        "The following will be permanently deleted:",
        "",
        f"  • Data lake (Parquet events — source of truth for the API)",
        f"      {lake}",
        f"  • Staging area (in-progress partition rewrites)",
        f"      {tmp}",
        f"  • Dead-letter queue (malformed / invalid JSONL lines)",
        f"      {dlq}",
        f"  • Tracking markers (.processing / .processed — ingest idempotency)",
        f"      {tracking}",
    ]
    if clear_landing:
        lines.extend(
            [
                f"  • Landing batch files (batch_*.jsonl — input copies)",
                f"      {landing}",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "The following are NOT deleted (re-ingest after reset):",
                f"  • Landing JSONL files",
                f"      {landing}",
                "    Add --landing to also remove batch_*.jsonl files.",
            ]
        )
    lines.extend(
        [
            "",
            "Does not touch: log files under data/logs/, or events.jsonl in the project root.",
        ]
    )
    return "\n".join(lines)


def _confirm_reset(
    app: AppConfig,
    pipeline: PipelineConfig,
    *,
    clear_landing: bool,
) -> bool:
    """Prompt the user to confirm a destructive reset with a formatted scope summary.

    Args:
        app: Application filesystem configuration.
        pipeline: Pipeline being reset.
        clear_landing: Whether landing batch files are included.

    Returns:
        ``True`` when the user answers ``y`` or ``yes`` (case-insensitive).
    """
    print()
    print(f"Reset pipeline: {pipeline.name}")
    print("=" * 60)
    print(_reset_scope_description(app, pipeline, clear_landing=clear_landing))
    print("=" * 60)
    answer = input("\nProceed with reset? [y/N]: ").strip().lower()
    return answer in ("y", "yes")


def _format_reset_summary_text(summary: ResetSummary, *, clear_landing: bool) -> str:
    """Format reset counts as a readable summary block.

    Args:
        summary: Deletion counts from :func:`reset_pipeline_data`.
        clear_landing: Whether landing batches were included in the reset.

    Returns:
        Multi-line human-readable summary.
    """
    lines = [
        "Reset complete.",
        "",
        f"  Event rows removed (data lake):     {summary.rows_deleted}",
        f"  DLQ Parquet file(s) removed:        {summary.dlq_deleted}",
        f"  Tracking marker file(s) removed:    {summary.tracking_markers_removed}",
        f"  Landing batch file(s) removed:      {summary.landing_batches_removed}",
    ]
    if not clear_landing and summary.landing_batches_removed == 0:
        lines.append("")
        lines.append("  Landing JSONL files were kept — run audit-ingest to reload the lake.")
    return "\n".join(lines)


def _clear_tracking_markers(tracking_dir: Path) -> int:
    """Remove all files in the tracking directory.

    Args:
        tracking_dir: Pipeline tracking marker root.

    Returns:
        Number of marker files deleted.
    """
    if not tracking_dir.is_dir():
        return 0
    removed = 0
    for marker in tracking_dir.iterdir():
        if marker.is_file():
            marker.unlink()
            removed += 1
    return removed


def _clear_landing_batches(landing_dir: Path) -> int:
    """Delete ``batch_*.jsonl`` files from the landing directory.

    Args:
        landing_dir: Pipeline landing directory.

    Returns:
        Number of batch files removed.
    """
    if not landing_dir.is_dir():
        return 0
    removed = 0
    for path in list(landing_dir.glob("batch_*.jsonl")):
        path.unlink()
        removed += 1
    return removed


def reset_pipeline_data(
    db: DuckDBClient,
    app: AppConfig,
    pipeline: PipelineConfig,
    *,
    clear_landing: bool = False,
    logger: logging.Logger | None = None,
) -> ResetSummary:
    """Wipe data lake, DLQ, and all tracking markers; optionally remove landing batches.

    Args:
        db: DuckDB client passed to storage truncation.
        app: Application filesystem configuration.
        pipeline: Pipeline whose storage is reset.
        clear_landing: When ``True``, also delete ``batch_*.jsonl`` landing files.
        logger: Optional logger for progress messages.

    Returns:
        :class:`~audit_service.backend.models.ResetSummary` with deletion counts.
    """
    if logger:
        logger.info(
            "Reset started pipeline=%s clear_landing=%s — wiping data lake, "
            "data_lake_tmp, DLQ, and tracking markers",
            pipeline.name,
            clear_landing,
        )

    storage = storage_table_for(pipeline)(db, app, pipeline)
    tracking_dir = app.pipeline_tracking_dir(pipeline)
    dlq_dir = app.pipeline_dlq_dir(pipeline)
    dlq_deleted = sum(1 for _ in dlq_dir.rglob("*.parquet")) if dlq_dir.is_dir() else 0

    rows_deleted, _ = storage.truncate_all()
    tracking_markers_removed = _clear_tracking_markers(tracking_dir)

    landing_batches_removed = 0
    if clear_landing:
        landing_batches_removed = _clear_landing_batches(app.pipeline_landing_dir(pipeline))
        if logger and landing_batches_removed:
            logger.info("Removed %d landing batch file(s)", landing_batches_removed)

    if logger:
        logger.info(
            "Reset complete — event_rows=%d dlq_files=%d tracking_markers=%d " "landing_batches=%d",
            rows_deleted,
            dlq_deleted,
            tracking_markers_removed,
            landing_batches_removed,
        )

    return ResetSummary(
        rows_deleted=rows_deleted,
        dlq_deleted=dlq_deleted,
        tracking_markers_removed=tracking_markers_removed,
        landing_batches_removed=landing_batches_removed,
    )


def parse_reset_cli(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for ``audit-reset``.

    Args:
        argv: Argument vector; uses ``sys.argv`` when ``None``.

    Returns:
        Parsed namespace with ``pipeline``, ``landing``, ``yes``, ``log_mode``,
        and ``log_level`` attributes.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Wipe pipeline storage so you can re-ingest from scratch. "
            "Removes Parquet data lake, DLQ rejects, tracking markers, and "
            "staging (data_lake_tmp). Landing JSONL is kept unless --landing is set."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "What gets deleted:\n"
            "  data/data_lake/{pipeline}/   — queryable event Parquet\n"
            "  data/data_lake_tmp/{pipeline}/ — partition rewrite staging\n"
            "  data/dlq/{pipeline}/          — rejected JSONL lines\n"
            "  data/tracking/{pipeline}/   — .processed / .processing markers\n"
            "\n"
            "With --landing, also deletes data/landing/{pipeline}/batch_*.jsonl\n"
            "\n"
            "Not deleted: data/logs/, source events.jsonl in project root"
        ),
    )
    parser.add_argument(
        "pipeline",
        nargs="?",
        default="events",
        help="Pipeline name (default: events)",
    )
    parser.add_argument(
        "--landing",
        action="store_true",
        help="Also delete batch_*.jsonl files in the landing directory",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip confirmation prompt",
    )
    parser.add_argument(
        "--log-mode",
        choices=[mode.value for mode in LogMode],
        default=LogMode.BOTH.value,
        help="Where to write reset logs (default: both)",
    )
    parser.add_argument(
        "--log-level",
        choices=sorted(LOG_LEVELS),
        default="INFO",
        help="Log verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run the reset CLI: confirm (unless ``-y``), reset storage, print JSON summary.

    Args:
        argv: Optional argument vector for testing.

    Raises:
        SystemExit: When the user declines confirmation or aborts.
    """
    args = parse_reset_cli(argv)
    app = load_app_config()
    pipeline = load_pipeline_config(args.pipeline)
    logger = configure_logging(
        logs_dir=app.logs_dir,
        pipeline_name=pipeline.name,
        command="reset",
        mode=LogMode(args.log_mode),
        level_name=args.log_level,
    )

    if not args.yes and not _confirm_reset(app, pipeline, clear_landing=args.landing):
        raise SystemExit("Aborted.")

    with DuckDBClient.connect() as db:
        summary = reset_pipeline_data(
            db,
            app,
            pipeline,
            clear_landing=args.landing,
            logger=logger,
        )
    print()
    print(_format_reset_summary_text(summary, clear_landing=args.landing))
    print()
    print(summary.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
