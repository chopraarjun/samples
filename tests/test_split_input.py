"""Tests for splitting a source JSONL file into landing batch files."""

from pathlib import Path

import pytest

from audit_service.backend.processing.ingestion.split_input import (
    BATCH_FILENAME_PATTERN,
    SplitMode,
    split_source_file,
)

RUN_TS = "20260630T120000Z"


def _write_source(path: Path, *lines: str) -> None:
    """Write JSONL lines to a source file.

    Args:
        path: Source file path.
        *lines: Line strings to join with newlines.
    """
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_batch_name(name: str) -> tuple[str, int]:
    """Parse a timestamped batch basename into run stamp and sequence.

    Args:
        name: Basename like ``batch_20260630T120000Z_0001.jsonl``.

    Returns:
        Tuple of ``(run_timestamp, sequence)``.
    """
    match = BATCH_FILENAME_PATTERN.match(name)
    assert match, name
    return match.group(1), int(match.group(2))


def test_split_source_file_writes_timestamped_batches(tmp_path: Path) -> None:
    """Verify split creates timestamped batch files with the configured line limit."""
    source = tmp_path / "events.jsonl"
    output_dir = tmp_path / "landing" / "events"
    _write_source(
        source,
        '{"event_id":"evt_1"}',
        '{"event_id":"evt_2"}',
        '{"event_id":"evt_3"}',
        '{"event_id":"evt_4"}',
        '{"event_id":"evt_5"}',
    )

    summary = split_source_file(
        source,
        output_dir,
        lines_per_file=2,
        run_timestamp=RUN_TS,
    )

    assert summary.mode == SplitMode.CREATE
    assert summary.files_written == 3
    assert summary.lines_written == 5
    assert summary.lines_read == 5
    assert len(summary.batch_files) == 3
    stamps_and_seqs = [_parse_batch_name(name) for name in summary.batch_files]
    assert all(stamp == RUN_TS for stamp, _ in stamps_and_seqs)
    assert [seq for _, seq in stamps_and_seqs] == [1, 2, 3]


def test_split_source_file_skips_blank_and_malformed_lines(tmp_path: Path) -> None:
    """Verify blank and malformed lines are skipped without failing the split."""
    source = tmp_path / "events.jsonl"
    output_dir = tmp_path / "landing" / "events"
    _write_source(
        source,
        '{"event_id":"evt_1"}',
        "",
        "{bad",
        '{"event_id":"evt_2"}',
    )

    summary = split_source_file(
        source,
        output_dir,
        lines_per_file=10,
        run_timestamp=RUN_TS,
    )

    assert summary.lines_read == 4
    assert summary.lines_written == 2
    assert summary.blank_lines_skipped == 1
    assert summary.malformed_lines_skipped == 1


def test_split_source_file_refuses_existing_batches_without_overwrite(tmp_path: Path) -> None:
    """Verify split raises FileExistsError when batches exist without --overwrite/--append."""
    source = tmp_path / "events.jsonl"
    output_dir = tmp_path / "landing" / "events"
    _write_source(source, '{"event_id":"evt_1"}')
    split_source_file(source, output_dir, lines_per_file=10, run_timestamp=RUN_TS)

    with pytest.raises(FileExistsError, match="--overwrite|--append"):
        split_source_file(source, output_dir, lines_per_file=10, run_timestamp=RUN_TS)


def test_split_source_file_overwrite_replaces_existing_batches(tmp_path: Path) -> None:
    """Verify --overwrite removes existing batches and rewrites with new line limits."""
    source = tmp_path / "events.jsonl"
    output_dir = tmp_path / "landing" / "events"
    _write_source(source, '{"event_id":"evt_1"}', '{"event_id":"evt_2"}')
    split_source_file(source, output_dir, lines_per_file=10, run_timestamp=RUN_TS)

    summary = split_source_file(
        source,
        output_dir,
        lines_per_file=1,
        overwrite=True,
        run_timestamp="20260630T130000Z",
    )

    assert summary.mode == SplitMode.REPLACE
    assert summary.existing_batches_before == 1
    assert summary.batches_removed == 1
    assert summary.files_written == 2
    assert summary.lines_written == 2
    stamps_and_seqs = [_parse_batch_name(name) for name in summary.batch_files]
    assert all(stamp == "20260630T130000Z" for stamp, _ in stamps_and_seqs)
    assert [seq for _, seq in stamps_and_seqs] == [1, 2]


def test_split_source_file_append_adds_new_timestamped_group(tmp_path: Path) -> None:
    """Verify --append keeps existing batches and adds a new timestamped group."""
    source = tmp_path / "events.jsonl"
    output_dir = tmp_path / "landing" / "events"
    output_dir.mkdir(parents=True)
    existing_name = f"batch_{RUN_TS}_0001.jsonl"
    (output_dir / existing_name).write_text('{"event_id":"old"}\n', encoding="utf-8")
    _write_source(source, '{"event_id":"evt_1"}', '{"event_id":"evt_2"}')

    summary = split_source_file(
        source,
        output_dir,
        lines_per_file=1,
        append=True,
        run_timestamp="20260630T140000Z",
    )

    assert summary.mode == SplitMode.APPEND
    assert summary.existing_batches_before == 1
    assert summary.batches_removed == 0
    assert summary.files_written == 2
    stamps_and_seqs = [_parse_batch_name(name) for name in summary.batch_files]
    assert all(stamp == "20260630T140000Z" for stamp, _ in stamps_and_seqs)
    assert [seq for _, seq in stamps_and_seqs] == [1, 2]
    assert (output_dir / existing_name).read_text(
        encoding="utf-8"
    ).strip() == '{"event_id":"old"}'


def test_split_overwrite_clears_tracking_markers_for_removed_batches(tmp_path: Path) -> None:
    """Verify overwrite replace clears .processed markers for removed batch files."""
    source = tmp_path / "events.jsonl"
    output_dir = tmp_path / "landing" / "events"
    tracking_dir = tmp_path / "tracking" / "events"
    _write_source(source, '{"event_id":"evt_1"}')
    summary = split_source_file(
        source,
        output_dir,
        lines_per_file=10,
        tracking_dir=tracking_dir,
        run_timestamp=RUN_TS,
    )
    tracking_dir.mkdir(parents=True, exist_ok=True)
    (tracking_dir / f"{summary.batch_files[0]}.processed").touch()

    split_source_file(
        source,
        output_dir,
        lines_per_file=10,
        overwrite=True,
        tracking_dir=tracking_dir,
        run_timestamp="20260630T150000Z",
    )

    assert not (tracking_dir / f"{summary.batch_files[0]}.processed").exists()
