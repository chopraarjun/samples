"""Summaries returned by retention and reset CLIs.

Main entry points:
    RetentionSummary: Row and partition counts removed by ``audit-retention``.
    ResetSummary: Counts cleared by ``audit-reset``.
"""

from pydantic import BaseModel


class RetentionSummary(BaseModel):
    """Row and partition counts removed by ``audit-retention``.

    Attributes:
        rows_deleted: Event rows removed from Parquet partitions.
        partitions_deleted: Hive ``event_date`` directories removed entirely.
    """

    rows_deleted: int = 0
    partitions_deleted: int = 0


class ResetSummary(BaseModel):
    """Counts cleared by ``audit-reset`` (lake, DLQ, markers, optional landing).

    Attributes:
        rows_deleted: Event rows removed from the data lake.
        dlq_deleted: DLQ Parquet files removed.
        tracking_markers_removed: Files deleted from the tracking directory.
        landing_batches_removed: ``batch_*.jsonl`` files removed when requested.
    """

    rows_deleted: int = 0
    dlq_deleted: int = 0
    tracking_markers_removed: int = 0
    landing_batches_removed: int = 0
