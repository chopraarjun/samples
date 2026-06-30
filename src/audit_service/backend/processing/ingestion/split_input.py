"""Split a large JSONL source file into landing-dir batches for incremental ingest.

Main entry points:
    split_source_file: Write timestamped batch JSONL files from a source file.
    main: ``audit-split-input`` CLI entry point.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from audit_service.backend.config import (
    AppConfig,
    PipelineConfig,
    load_app_config,
    load_pipeline_config,
)


class SplitMode(StrEnum):
    """How an existing landing directory was handled during a split run.

    Members:
        CREATE: Wrote batches into an empty output directory.
        REPLACE: Removed existing batches before writing (--overwrite).
        APPEND: Added a new timestamped batch group after existing files (--append).
    """

    CREATE = "create"
    REPLACE = "replace"
    APPEND = "append"


class SplitInputSummary(BaseModel):
    """JSON stdout payload for one split-input invocation.

    Attributes:
        source: Path to the source JSONL file.
        output_dir: Landing directory receiving batch files.
        lines_per_file: Maximum JSON lines per output batch.
        mode: How existing batches were handled.
        existing_batches_before: Batch file count before the run.
        batches_removed: Batch files deleted (replace mode).
        tracking_markers_removed: Markers cleared when batches were replaced.
        lines_read: Total lines read from the source (including skips).
        lines_written: Valid JSON lines written to batch files.
        blank_lines_skipped: Empty lines skipped.
        malformed_lines_skipped: Lines that failed JSON parsing.
        files_written: Number of batch files created this run.
        batch_files: Basenames of batch files written this run.
    """

    source: str
    output_dir: str
    lines_per_file: int
    mode: SplitMode
    existing_batches_before: int = 0
    batches_removed: int = 0
    tracking_markers_removed: int = 0
    lines_read: int = 0
    lines_written: int = 0
    blank_lines_skipped: int = 0
    malformed_lines_skipped: int = 0
    files_written: int = 0
    batch_files: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _files_written_matches_batch_list(self) -> SplitInputSummary:
        """Ensure ``files_written`` matches the length of ``batch_files``.

        Returns:
            Validated model instance.

        Raises:
            ValueError: When counts are inconsistent.
        """
        if self.files_written != len(self.batch_files):
            raise ValueError("files_written must match len(batch_files)")
        return self


BATCH_FILENAME_PATTERN = re.compile(r"^batch_(\d{8}T\d{6}Z)_(\d{4})\.jsonl$")


def _utc_run_timestamp() -> str:
    """Return a compact UTC timestamp for batch filenames.

    Returns:
        Timestamp string like ``20260630T215014Z`` (same format as ingest logs).
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _make_batch_filename(prefix: str, run_timestamp: str, sequence: int) -> str:
    """Build a landing batch basename with run timestamp and sequence.

    Args:
        prefix: Batch filename prefix (default ``batch``).
        run_timestamp: UTC run stamp shared by all files in one split invocation.
        sequence: 1-based sequence within the run.

    Returns:
        Basename like ``batch_20260630T215014Z_0001.jsonl``.
    """
    return f"{prefix}_{run_timestamp}_{sequence:04d}.jsonl"


def _list_batch_files(output_dir: Path, prefix: str) -> list[Path]:
    """List existing batch files matching ``{prefix}_*.jsonl``.

    Args:
        output_dir: Landing directory to scan.
        prefix: Batch filename prefix (default ``batch``).

    Returns:
        Sorted list of matching batch file paths.
    """
    return sorted(output_dir.glob(f"{prefix}_*.jsonl"))


def _clear_markers_for_batch_files(tracking_dir: Path, batch_filenames: list[str]) -> int:
    """Remove processing and processed markers for removed batch files.

    Args:
        tracking_dir: Pipeline tracking directory.
        batch_filenames: Landing basenames whose markers should be cleared.

    Returns:
        Number of marker files deleted.
    """
    from audit_service.backend.processing.ingestion.ingest import (
        PROCESSED_SUFFIX,
        PROCESSING_SUFFIX,
    )

    if not tracking_dir.is_dir():
        return 0
    removed = 0
    for filename in batch_filenames:
        for suffix in (PROCESSED_SUFFIX, PROCESSING_SUFFIX):
            marker = tracking_dir / f"{filename}{suffix}"
            if marker.is_file():
                marker.unlink()
                removed += 1
    return removed


def split_source_file(
    source: Path,
    output_dir: Path,
    *,
    lines_per_file: int,
    prefix: str = "batch",
    overwrite: bool = False,
    append: bool = False,
    tracking_dir: Path | None = None,
    run_timestamp: str | None = None,
) -> SplitInputSummary:
    """Write non-empty JSON lines from *source* into timestamped batch files.

    Args:
        source: Input JSONL file to split.
        output_dir: Landing directory for ``{prefix}_{timestamp}_{NNNN}.jsonl`` outputs.
        lines_per_file: Maximum valid JSON lines per batch file.
        prefix: Batch filename prefix.
        overwrite: Replace existing batches and clear their tracking markers.
        append: Add a new timestamped batch group; existing landing files are kept.
        tracking_dir: Tracking directory for marker cleanup on overwrite replace.
        run_timestamp: Fixed UTC stamp for tests; generated at split time when ``None``.

    Returns:
        :class:`SplitInputSummary` describing the split operation.

    Raises:
        ValueError: When ``lines_per_file < 1`` or both *overwrite* and *append* are set.
        FileNotFoundError: When *source* does not exist.
        FileExistsError: When batches exist and neither *overwrite* nor *append* is set.
    """
    if lines_per_file < 1:
        raise ValueError("lines_per_file must be >= 1")
    if overwrite and append:
        raise ValueError("Use only one of overwrite=True or append=True")
    if not source.is_file():
        raise FileNotFoundError(f"Source file not found: {source}")

    output_dir.mkdir(parents=True, exist_ok=True)
    existing = _list_batch_files(output_dir, prefix)
    existing_count = len(existing)

    if existing and not overwrite and not append:
        names = ", ".join(path.name for path in existing[:3])
        suffix = "..." if len(existing) > 3 else ""
        raise FileExistsError(
            f"Output dir already has {existing_count} batch file(s) ({names}{suffix}). "
            "Use --overwrite to replace landing batches (and clear their tracking markers) "
            "or --append to add new batch files after the last index."
        )

    batches_removed = 0
    markers_removed = 0
    removed_names: list[str] = []
    if overwrite:
        removed_names = [path.name for path in existing]
        if tracking_dir and removed_names:
            markers_removed = _clear_markers_for_batch_files(tracking_dir, removed_names)
        for path in existing:
            path.unlink()
            batches_removed += 1
        existing = []
        mode = SplitMode.REPLACE
    elif append and existing:
        mode = SplitMode.APPEND
    else:
        mode = SplitMode.CREATE

    summary = SplitInputSummary(
        source=str(source),
        output_dir=str(output_dir),
        lines_per_file=lines_per_file,
        mode=mode,
        existing_batches_before=existing_count,
        batches_removed=batches_removed,
        tracking_markers_removed=markers_removed,
    )

    batch_index = 0
    lines_in_batch = 0
    run_ts = run_timestamp or _utc_run_timestamp()
    handle = None
    current_batch_name: str | None = None

    def open_next_batch() -> None:
        """Open the next timestamped batch file for writing."""
        nonlocal batch_index, handle, lines_in_batch, current_batch_name
        if handle is not None:
            handle.close()
        batch_index += 1
        lines_in_batch = 0
        current_batch_name = _make_batch_filename(prefix, run_ts, batch_index)
        batch_path = output_dir / current_batch_name
        handle = batch_path.open("w", encoding="utf-8", newline="\n")
        summary.files_written += 1
        summary.batch_files.append(current_batch_name)

    try:
        with source.open(encoding="utf-8") as source_handle:
            for line in source_handle:
                summary.lines_read += 1
                stripped = line.strip()
                if not stripped:
                    summary.blank_lines_skipped += 1
                    continue

                try:
                    json.loads(stripped)
                except json.JSONDecodeError:
                    summary.malformed_lines_skipped += 1
                    continue

                if handle is None or lines_in_batch >= lines_per_file:
                    open_next_batch()

                handle.write(stripped)
                handle.write("\n")
                lines_in_batch += 1
                summary.lines_written += 1
    finally:
        if handle is not None:
            handle.close()
        elif summary.files_written > 0 and summary.lines_written == 0:
            # Empty batch opened at end with no writes — should not happen
            pass

    if summary.lines_written == 0 and summary.files_written == 0:
        return summary

    return summary


def resolve_split_paths(
    app: AppConfig,
    pipeline: PipelineConfig,
    source: Path | None,
) -> tuple[Path, Path]:
    """Resolve default source and landing paths for the split CLI.

    Args:
        app: Application filesystem configuration.
        pipeline: Pipeline configuration.
        source: Explicit source path, or ``None`` for defaults.

    Returns:
        Tuple of ``(source_path, landing_dir)``.
    """
    landing_dir = app.pipeline_landing_dir(pipeline)
    source_path = source or app.pipeline_source_file(pipeline)
    if not source_path.is_file() and source_path.name == f"{pipeline.name}.jsonl":
        fallback = Path("events.jsonl")
        if fallback.is_file():
            source_path = fallback
    return source_path, landing_dir


def parse_split_cli(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the split-input CLI.

    Args:
        argv: Argument vector; uses ``sys.argv`` when ``None``.

    Returns:
        Parsed namespace with pipeline, source, lines-per-file, and mode flags.
    """
    parser = argparse.ArgumentParser(
        description="Split a JSONL source file into landing-dir batches for audit-ingest",
    )
    parser.add_argument(
        "pipeline",
        nargs="?",
        default="events",
        help="Pipeline name (default: events) → writes to data/landing/{pipeline}/",
    )
    parser.add_argument(
        "source",
        nargs="?",
        default=None,
        help="Source JSONL file (default: events.jsonl in project root)",
    )
    parser.add_argument(
        "--lines-per-file",
        type=int,
        default=100,
        metavar="N",
        help="Max JSON lines per batch file (default: 100)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Override landing directory (default: from config/app.config)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite landing batches and clear their tracking markers",
    )
    mode.add_argument(
        "--append",
        action="store_true",
        help="Append new timestamped batch_*.jsonl files alongside existing landing files",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run the split-input CLI and print a JSON summary to stdout.

    Args:
        argv: Optional argument vector for testing.
    """
    args = parse_split_cli(argv)
    app = load_app_config()
    pipeline = load_pipeline_config(args.pipeline)
    source_arg = Path(args.source) if args.source else None
    source_path, landing_dir = resolve_split_paths(app, pipeline, source_arg)
    output_dir = args.output_dir or landing_dir
    tracking_dir = app.pipeline_tracking_dir(pipeline)

    summary = split_source_file(
        source_path,
        output_dir,
        lines_per_file=args.lines_per_file,
        overwrite=args.overwrite,
        append=args.append,
        tracking_dir=tracking_dir,
    )
    print(summary.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
