"""Hive-partitioned Parquet I/O, dedupe merge, DLQ append, and tenant-scoped reads.

Main entry points:
    EventsTable: Parquet-backed storage for artifact access audit events.
"""

from __future__ import annotations

import shutil
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel

from audit_service.backend.config import AppConfig, PipelineConfig
from audit_service.backend.models.events.query import EventQueryParams
from audit_service.backend.models.events.record import EventRecord
from audit_service.backend.storage.parquet_paths import (
    has_parquet_files,
    partition_dir,
    partition_glob,
    sql_path,
)

if TYPE_CHECKING:
    from audit_service.backend.db import DuckDBClient


class EventsTable:
    """Parquet events store: read, dedupe against existing partition, rewrite.

    Hive partitions use ``tenant_id`` and ``event_date`` directory keys.

    Class attributes:
        ID_COLUMN: Primary deduplication key column name.
        TENANT_COLUMN: Tenant filter column name.
        TIMESTAMP_COLUMN: Event time column used for ordering and retention.
        EVENT_DATE_COLUMN: Date partition column name.
        PARTITION_COLUMNS: Hive partition column names for COPY.
        RECORD_MODEL: Pydantic model class for validated rows.
        DATA_COLUMNS: Parquet data column order.
        DLQ_COLUMNS: Dead-letter queue Parquet column order.
    """

    ID_COLUMN = "event_id"
    TENANT_COLUMN = "tenant_id"
    TIMESTAMP_COLUMN = "timestamp"
    EVENT_DATE_COLUMN = "event_date"
    PARTITION_COLUMNS: ClassVar[tuple[str, ...]] = ("tenant_id", "event_date")
    RECORD_MODEL: ClassVar[type[BaseModel]] = EventRecord
    DATA_COLUMNS: ClassVar[tuple[str, ...]] = (
        "event_id",
        "tenant_id",
        "event_date",
        "timestamp",
        "action",
        "package",
        "version",
        "actor",
    )
    DLQ_COLUMNS: ClassVar[tuple[str, ...]] = (
        "source_file",
        "line_number",
        "raw_line",
        "reason",
        "rejected_at",
    )

    def __init__(self, db: DuckDBClient, app: AppConfig, cfg: PipelineConfig) -> None:
        """Bind storage to a DuckDB client and configured filesystem roots.

        Args:
            db: DuckDB client for Parquet SQL operations.
            app: Application paths for lake, DLQ, and temp directories.
            cfg: Pipeline configuration (name selects subdirectories).
        """
        self.db = db
        self.app = app
        self.cfg = cfg
        self.data_root = app.pipeline_data_lake_dir(cfg)
        self.data_tmp_root = app.pipeline_data_lake_tmp_dir(cfg)
        self.dlq_root = app.pipeline_dlq_dir(cfg)

    def ingest_records(self, records: list[EventRecord]) -> set[tuple[str, str]]:
        """Upsert records into hive partitions, deduping by tenant and event ID.

        Args:
            records: Validated events to merge into Parquet storage.

        Returns:
            Set of ``(tenant_id, event_date)`` partition keys that were written.
        """
        if not records:
            return set()
        by_partition: dict[tuple[str, str], list[EventRecord]] = defaultdict(list)
        for record in records:
            by_partition[(record.tenant_id, record.dt)].append(record)

        for (tenant_id, event_date), part_records in by_partition.items():
            self._upsert_partition(tenant_id, event_date, part_records)

        return set(by_partition.keys())

    def existing_ids(self, record_ids: list[str], tenant_ids: set[str]) -> set[str]:
        """Return event IDs already present in storage for the given tenants.

        Args:
            record_ids: Candidate IDs to look up.
            tenant_ids: Tenants whose partitions are scanned.

        Returns:
            Subset of *record_ids* found in existing Parquet files.
        """
        if not record_ids or not tenant_ids:
            return set()
        found: set[str] = set()
        placeholders = ", ".join("?" * len(record_ids))
        for tenant_id in tenant_ids:
            tenant_dir = self.data_root / f"tenant_id={tenant_id}"
            if not tenant_dir.is_dir():
                continue
            glob_path = sql_path(tenant_dir / "**" / "*.parquet")
            rows = self.db.fetchall(
                f"""
                SELECT {self.ID_COLUMN}
                FROM read_parquet('{glob_path}', hive_partitioning=true)
                WHERE {self.ID_COLUMN} IN ({placeholders})
                """,
                record_ids,
            )
            found.update(str(row[0]) for row in rows)
        return found

    def insert_dlq(
        self,
        rows: list[tuple[int, str, str]],
        *,
        source_file: str,
    ) -> int:
        """Append rejected lines to the pipeline DLQ Parquet file.

        Args:
            rows: Tuples of ``(line_number, raw_line, reason)``.
            source_file: Landing filename that produced the rejects.

        Returns:
            Number of DLQ rows written.
        """
        if not rows:
            return 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.dlq_root.mkdir(parents=True, exist_ok=True)
        self.db.execute("""
            CREATE OR REPLACE TEMP TABLE dlq_batch (
                source_file VARCHAR,
                line_number BIGINT,
                raw_line VARCHAR,
                reason VARCHAR,
                rejected_at TIMESTAMP
            )
            """)
        self.db.executemany(
            "INSERT INTO dlq_batch VALUES (?, ?, ?, ?, ?)",
            [(source_file, line_no, raw, reason, now) for line_no, raw, reason in rows],
        )
        target = sql_path(self.dlq_root / "rejects.parquet")
        self.db.execute(f"""
            COPY dlq_batch TO '{target}' (
                FORMAT parquet,
                APPEND,
                FILENAME_PATTERN 'batch_{{uuid}}'
            )
            """)
        self.db.execute("DROP TABLE IF EXISTS dlq_batch")
        return len(rows)

    def delete_events_before(self, cutoff: datetime) -> tuple[int, int]:
        """Remove partitions whose ``event_date`` is strictly before *cutoff*.

        Args:
            cutoff: Events on dates before ``cutoff.date()`` are deleted.

        Returns:
            Tuple of ``(rows_deleted, partitions_deleted)``.
        """
        cutoff_date = cutoff.date()
        rows_deleted = 0
        partitions_deleted = 0
        if not self.data_root.is_dir():
            return rows_deleted, partitions_deleted

        for tenant_dir in list(self.data_root.iterdir()):
            if not tenant_dir.is_dir() or not tenant_dir.name.startswith("tenant_id="):
                continue
            for date_dir in list(tenant_dir.iterdir()):
                if not date_dir.is_dir() or not date_dir.name.startswith("event_date="):
                    continue
                try:
                    folder_date = date.fromisoformat(date_dir.name.split("=", 1)[1])
                except ValueError:
                    continue
                if folder_date >= cutoff_date:
                    continue
                glob_path = sql_path(date_dir / "*.parquet")
                count_row = self.db.fetchone(
                    f"SELECT COUNT(*) FROM read_parquet('{glob_path}', hive_partitioning=true)"
                )
                rows_deleted += int(count_row[0]) if count_row else 0
                shutil.rmtree(date_dir)
                partitions_deleted += 1
        return rows_deleted, partitions_deleted

    def truncate_all(self) -> tuple[int, int]:
        """Delete all data lake, temp, and DLQ files and recreate empty roots.

        Returns:
            Tuple of ``(rows_deleted, partitions_deleted)``; partitions_deleted is
            always ``0`` (entire tree is removed rather than per-partition).
        """
        rows_deleted = 0
        if self.data_root.is_dir() and any(self.data_root.rglob("*.parquet")):
            glob_path = sql_path(self.data_root / "**" / "*.parquet")
            count_row = self.db.fetchone(f"""
                SELECT COUNT(*)
                FROM read_parquet('{glob_path}', hive_partitioning=true)
                """)
            rows_deleted = int(count_row[0]) if count_row else 0
        for root in (self.data_root, self.data_tmp_root, self.dlq_root):
            if root.exists():
                shutil.rmtree(root)
            root.mkdir(parents=True, exist_ok=True)
        return rows_deleted, 0

    def list_events(self, params: EventQueryParams) -> tuple[list[EventRecord], int]:
        """Query tenant-scoped events with optional filters and pagination.

        Args:
            params: Validated query parameters including required ``tenant_id``.

        Returns:
            Tuple of ``(events, total)`` where *total* ignores limit and offset.
        """
        tenant_glob = sql_path(
            self.data_root / f"tenant_id={params.tenant_id}" / "**" / "*.parquet"
        )
        tenant_dir = self.data_root / f"tenant_id={params.tenant_id}"
        if not tenant_dir.is_dir():
            return [], 0

        where_clause, bind = self._build_event_filters(params)
        total_row = self.db.fetchone(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{tenant_glob}', hive_partitioning=true)
            WHERE {where_clause}
            """,
            bind,
        )
        total = int(total_row[0]) if total_row else 0

        rows = self.db.fetchall(
            f"""
            SELECT
                {self.ID_COLUMN},
                {self.TENANT_COLUMN},
                action,
                package,
                version,
                {self.TIMESTAMP_COLUMN},
                actor
            FROM read_parquet('{tenant_glob}', hive_partitioning=true)
            WHERE {where_clause}
            ORDER BY {self.TIMESTAMP_COLUMN}
            LIMIT ? OFFSET ?
            """,
            bind + [params.limit, params.offset],
        )
        return [EventRecord.from_db_row(row) for row in rows], total

    def _upsert_partition(
        self,
        tenant_id: str,
        event_date: str,
        incoming: list[EventRecord],
    ) -> None:
        """Merge *incoming* records with an existing partition via DuckDB SQL.

        Deduplicates by ``(tenant_id, event_id)`` keeping the earliest timestamp.

        Args:
            tenant_id: Hive partition tenant key.
            event_date: ISO date string partition key.
            incoming: New records for the partition.
        """
        self.data_root.mkdir(parents=True, exist_ok=True)
        cols = ", ".join(self.DATA_COLUMNS)
        self.db.execute(f"""
            CREATE OR REPLACE TEMP TABLE incoming_batch (
                {self.ID_COLUMN} VARCHAR,
                {self.TENANT_COLUMN} VARCHAR,
                {self.EVENT_DATE_COLUMN} DATE,
                {self.TIMESTAMP_COLUMN} TIMESTAMP,
                action VARCHAR,
                package VARCHAR,
                version VARCHAR,
                actor VARCHAR
            )
            """)
        self.db.executemany(
            "INSERT INTO incoming_batch VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [self._record_row(record) for record in incoming],
        )

        data_part = partition_dir(self.data_root, tenant_id, event_date)
        existing_glob = (
            sql_path(partition_glob(self.data_root, tenant_id, event_date))
            if has_parquet_files(data_part)
            else None
        )

        if existing_glob:
            source_sql = f"""
                SELECT {cols} FROM read_parquet('{existing_glob}', hive_partitioning=true)
                UNION ALL BY NAME
                SELECT {cols} FROM incoming_batch
            """
        else:
            source_sql = f"SELECT {cols} FROM incoming_batch"

        self.db.execute(f"""
            CREATE OR REPLACE TEMP TABLE final_partition AS
            SELECT {cols}
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY {self.TENANT_COLUMN}, {self.ID_COLUMN}
                        ORDER BY {self.TIMESTAMP_COLUMN} ASC
                    ) AS rn
                FROM ({source_sql})
            )
            WHERE rn = 1
            """)
        self.db.execute("DROP TABLE IF EXISTS incoming_batch")

        self.data_tmp_root.mkdir(parents=True, exist_ok=True)
        tmp_root = sql_path(self.data_tmp_root)
        self.db.execute(f"""
            COPY (SELECT {cols} FROM final_partition)
            TO '{tmp_root}'
            (
                FORMAT parquet,
                PARTITION_BY ({", ".join(self.PARTITION_COLUMNS)}),
                FILENAME_PATTERN 'part_{{uuid}}'
            )
            """)
        self.db.execute("DROP TABLE IF EXISTS final_partition")

        tmp_part = partition_dir(self.data_tmp_root, tenant_id, event_date)
        if tmp_part.exists():
            data_part.parent.mkdir(parents=True, exist_ok=True)
            if data_part.exists():
                shutil.rmtree(data_part)
            shutil.move(str(tmp_part), str(data_part))

    @staticmethod
    def _record_row(record: EventRecord) -> list[object]:
        """Serialize a record to INSERT column order for ``incoming_batch``.

        Args:
            record: Event to serialize.

        Returns:
            Column values matching :attr:`DATA_COLUMNS` order.
        """
        return [
            record.event_id,
            record.tenant_id,
            record.event_date,
            record.timestamp,
            record.action.value,
            record.package,
            record.version,
            record.actor,
        ]

    def _build_event_filters(self, params: EventQueryParams) -> tuple[str, list[Any]]:
        """Build a SQL WHERE clause and bind parameters from query params.

        Args:
            params: Validated event query parameters.

        Returns:
            Tuple of ``(where_clause, bind_parameters)``.
        """
        conditions = [f"{self.TENANT_COLUMN} = ?"]
        bind: list[Any] = [params.tenant_id]

        if params.start_time is not None:
            conditions.append(f"{self.TIMESTAMP_COLUMN} >= ?")
            bind.append(params.start_time)
        if params.end_time is not None:
            conditions.append(f"{self.TIMESTAMP_COLUMN} <= ?")
            bind.append(params.end_time)
        if params.action is not None:
            conditions.append("action = ?")
            bind.append(params.action.value)
        if params.package is not None:
            conditions.append("package = ?")
            bind.append(params.package)
        if params.actor is not None:
            conditions.append(f"actor LIKE '%{params.actor}%'")

        return " AND ".join(conditions), bind
