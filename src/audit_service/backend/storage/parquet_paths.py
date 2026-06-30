"""Hive-style Parquet path helpers for DuckDB ``read_parquet`` and ``COPY``.

Main entry points:
    sql_path: Escape a filesystem path for embedding in SQL strings.
    partition_dir: Build a hive partition directory path.
    partition_glob: Glob path for Parquet files in one partition.
    has_parquet_files: Check whether a partition directory contains Parquet files.
"""

from pathlib import Path


def sql_path(path: Path) -> str:
    """Return a DuckDB-safe absolute path string for SQL embedding.

    Args:
        path: Filesystem path (may contain backslashes on Windows).

    Returns:
        Forward-slash absolute path with single quotes escaped.
    """
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def partition_dir(root: Path, tenant_id: str, event_date: str) -> Path:
    """Build a hive partition directory path.

    Args:
        root: Data lake or temp root directory.
        tenant_id: Tenant partition value.
        event_date: ISO date string partition value.

    Returns:
        ``root/tenant_id={tenant_id}/event_date={event_date}``.
    """
    return root / f"tenant_id={tenant_id}" / f"event_date={event_date}"


def partition_glob(root: Path, tenant_id: str, event_date: str) -> Path:
    """Return a glob path matching Parquet files in one partition.

    Args:
        root: Data lake root directory.
        tenant_id: Tenant partition value.
        event_date: ISO date string partition value.

    Returns:
        Path ending in ``*.parquet`` under the partition directory.
    """
    return partition_dir(root, tenant_id, event_date) / "*.parquet"


def has_parquet_files(path: Path) -> bool:
    """Return whether *path* is a directory containing at least one Parquet file.

    Args:
        path: Candidate partition directory.

    Returns:
        ``True`` when ``*.parquet`` files exist directly under *path*.
    """
    return path.is_dir() and any(path.glob("*.parquet"))
