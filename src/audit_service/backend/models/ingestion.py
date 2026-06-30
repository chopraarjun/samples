"""Pydantic models for ingest CLI summaries and landing-file inventory.

Main entry points:
    LandingFileStatus, LandingInventorySummary: Landing directory tracking state.
    IngestionSummary, FileIngestionResult, IngestionBatchSummary: Ingest run output.
"""

from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, Field, computed_field


class LandingFileStatus(StrEnum):
    """Tracking state of a landing JSONL file (see ``data/tracking/``).

    Members:
        PENDING: Awaiting ingest (no marker file).
        PROCESSING: Ingest in progress (``.processing`` marker present).
        PROCESSED: Successfully ingested (``.processed`` marker present).
    """

    PENDING = "pending"
    PROCESSING = "processing"
    PROCESSED = "processed"


class LandingFileSnapshot(BaseModel):
    """One landing file and its ingest status at a point in time.

    Attributes:
        filename: Basename of the JSONL file in the landing directory.
        status: Current :class:`LandingFileStatus` derived from tracking markers.
    """

    filename: str
    status: LandingFileStatus


class LandingInventorySummary(BaseModel):
    """Counts and per-file status for all ``*.jsonl`` files in landing.

    Attributes:
        total: Number of landing JSONL files discovered.
        processed: Files with a ``.processed`` tracking marker.
        pending: Files with no processing markers.
        processing: Files with a ``.processing`` marker (orphaned or in-flight).
        files: Per-file snapshots sorted by filename.
    """

    total: int = 0
    processed: int = 0
    pending: int = 0
    processing: int = 0
    files: list[LandingFileSnapshot] = Field(default_factory=list)


class IngestionSkippedSummary(BaseModel):
    """Lines that were not newly inserted, grouped by reason.

    Attributes:
        blank: Empty or whitespace-only lines.
        malformed: Lines that failed JSON parsing (also in DLQ).
        invalid: Lines that failed schema validation (also in DLQ).
        duplicate_in_file: Same ``event_id`` seen again in this file (earliest kept).
        already_in_lake: Valid line, but that ``event_id`` is already in the data lake.
    """

    blank: int = 0
    malformed: int = 0
    invalid: int = 0
    duplicate_in_file: int = 0
    already_in_lake: int = 0

    def total(self) -> int:
        """Sum of all skip categories."""
        return self.blank + self.malformed + self.invalid + self.duplicate_in_file + self.already_in_lake

    def format_compact(self) -> str:
        """Compact ``{blank: n, invalid: n, ...}`` string for logs."""
        parts = ", ".join(
            f"{name}={getattr(self, name)}"
            for name in IngestionSkippedSummary.model_fields
        )
        return f"{{{parts}}}"


class IngestionSummary(BaseModel):
    """Per-file ingest statistics (lines read, inserted, skips, DLQ writes).

    Line accounting (always balances when ``balanced`` is true)::

        read = inserted + skipped.blank + skipped.malformed + skipped.invalid
               + skipped.duplicate_in_file + skipped.already_in_lake

    Attributes:
        read: Total physical lines in the file (including blanks).
        inserted: New event IDs written to the data lake.
        skipped: Non-inserted lines grouped by reason.
        dlq_written: Rows appended to the DLQ Parquet file.
    """

    read: int = 0
    inserted: int = 0
    skipped: IngestionSkippedSummary = Field(default_factory=IngestionSkippedSummary)
    dlq_written: int = 0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def accounted_for(self) -> int:
        """Sum of all categorized lines; equals :attr:`read` when balanced."""
        return self.inserted + self.skipped.total()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def balanced(self) -> bool:
        """Whether every line read is assigned to exactly one category."""
        return self.accounted_for == self.read

    def reconciliation_line(self) -> str:
        """Human-readable line accounting for logs."""
        return (
            f"read={self.read} = inserted={self.inserted} + skipped={self.skipped.format_compact()}"
        )

    @classmethod
    def rollup(cls, summaries: list["IngestionSummary"]) -> "IngestionSummary":
        """Sum per-file summaries into one totals object."""
        total = cls()
        for summary in summaries:
            total.read += summary.read
            total.inserted += summary.inserted
            total.dlq_written += summary.dlq_written
            for name in IngestionSkippedSummary.model_fields:
                setattr(
                    total.skipped,
                    name,
                    getattr(total.skipped, name) + getattr(summary.skipped, name),
                )
        return total


class FileIngestionResult(BaseModel):
    """Outcome of ingesting a single landing file in one batch run.

    Attributes:
        filename: Landing file basename that was attempted.
        status: ``"succeeded"`` or ``"failed"``.
        summary: Per-file stats when ingestion succeeded.
        error: Error message string when ingestion failed.
    """

    filename: str
    status: str
    summary: Optional["IngestionSummary"] = None
    error: Optional[str] = None


class IngestionBatchSummary(BaseModel):
    """JSON stdout payload for one ``audit-ingest`` invocation.

    Attributes:
        files_attempted: Landing files selected for this batch.
        files_succeeded: Files that completed without exception.
        files_failed: Files that raised during processing.
        ingest_complete: ``True`` when no pending or processing files remain.
        inventory_before: Landing inventory snapshot before the batch.
        inventory_after: Landing inventory snapshot after the batch.
        results: Per-file :class:`FileIngestionResult` entries for **this batch only**.
    """

    files_attempted: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    ingest_complete: bool = False
    inventory_before: LandingInventorySummary = Field(default_factory=LandingInventorySummary)
    inventory_after: LandingInventorySummary = Field(default_factory=LandingInventorySummary)
    results: list[FileIngestionResult] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def processed_files(self) -> list[str]:
        """Landing basenames successfully ingested in this batch."""
        return [result.filename for result in self.results if result.status == "succeeded"]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def failed_files(self) -> list[str]:
        """Landing basenames that raised during this batch."""
        return [result.filename for result in self.results if result.status == "failed"]

    def public_json(self, *, indent: int | None = 2) -> str:
        """Serialize for stdout/logs with landing inventory counts only.

        The full per-file landing list remains in memory for DEBUG logs but is
        omitted from JSON to avoid repeating every batch filename each cycle.
        """
        return self.model_dump_json(
            indent=indent,
            exclude={
                "inventory_before": {"files"},
                "inventory_after": {"files"},
            },
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def totals(self) -> IngestionSummary:
        """Roll up per-file stats from succeeded ingest results."""
        summaries = [result.summary for result in self.results if result.summary is not None]
        return IngestionSummary.rollup(summaries)
