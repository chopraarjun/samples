"""Application runtime paths loaded from ``config/app.config``.

Main entry points:
    AppConfig: Pydantic model of data directory roots.
    load_app_config: Load TOML from ``config/app.config`` or return defaults.
"""

import tomllib
from pathlib import Path

from pydantic import BaseModel

from audit_service.backend.config.pipeline import PipelineConfig


class AppConfig(BaseModel):
    """Filesystem roots for pipeline data (landing, lake, DLQ, tracking, logs).

    Attributes:
        data_lake_dir: Hive-partitioned Parquet storage root.
        dlq_dir: Dead-letter queue Parquet files per pipeline.
        landing_dir: Incoming JSONL batch files awaiting ingest.
        tracking_dir: Per-file ingest status marker files.
        logs_dir: Processing command log output directory.
    """

    data_lake_dir: Path = Path("./data/data_lake")
    dlq_dir: Path = Path("./data/dlq")
    landing_dir: Path = Path("./data/landing")
    tracking_dir: Path = Path("./data/tracking")
    logs_dir: Path = Path("./data/logs")

    def pipeline_data_lake_dir(self, pipeline: PipelineConfig) -> Path:
        """Return the data lake subdirectory for a pipeline.

        Args:
            pipeline: Pipeline whose ``name`` selects the subdirectory.

        Returns:
            ``data_lake_dir / pipeline.name``.
        """
        return self.data_lake_dir / pipeline.name

    def pipeline_data_lake_tmp_dir(self, pipeline: PipelineConfig) -> Path:
        """Return the staging directory used during Parquet partition rewrites.

        Args:
            pipeline: Pipeline whose ``name`` selects the subdirectory.

        Returns:
            ``data_lake_tmp/{pipeline.name}`` sibling of ``data_lake_dir``.
        """
        return self.data_lake_dir.parent / "data_lake_tmp" / pipeline.name

    def pipeline_dlq_dir(self, pipeline: PipelineConfig) -> Path:
        """Return the DLQ directory for a pipeline.

        Args:
            pipeline: Pipeline whose ``name`` selects the subdirectory.

        Returns:
            ``dlq_dir / pipeline.name``.
        """
        return self.dlq_dir / pipeline.name

    def pipeline_landing_dir(self, pipeline: PipelineConfig) -> Path:
        """Return the landing directory for a pipeline's JSONL batches.

        Args:
            pipeline: Pipeline whose ``name`` selects the subdirectory.

        Returns:
            ``landing_dir / pipeline.name``.
        """
        return self.landing_dir / pipeline.name

    def pipeline_tracking_dir(self, pipeline: PipelineConfig) -> Path:
        """Return the tracking marker directory for a pipeline.

        Args:
            pipeline: Pipeline whose ``name`` selects the subdirectory.

        Returns:
            ``tracking_dir / pipeline.name``.
        """
        return self.tracking_dir / pipeline.name

    def pipeline_logs_dir(self, pipeline: PipelineConfig) -> Path:
        """Return the log output directory for a pipeline.

        Args:
            pipeline: Pipeline whose ``name`` selects the subdirectory.

        Returns:
            ``logs_dir / pipeline.name``.
        """
        return self.logs_dir / pipeline.name

    def pipeline_source_file(self, pipeline: PipelineConfig) -> Path:
        """Return the default JSONL source filename for split-input commands.

        Args:
            pipeline: Pipeline whose ``name`` forms the default filename.

        Returns:
            Path to ``{pipeline.name}.jsonl`` in the project root.
        """
        return Path(f"{pipeline.name}.jsonl")


def load_app_config(*, config_dir: Path | None = None) -> AppConfig:
    """Load application paths from ``config/app.config``.

    Args:
        config_dir: Directory containing ``app.config``; uses
            :func:`~audit_service.backend.config.pipeline.default_config_dir`
            when omitted.

    Returns:
        Validated :class:`AppConfig`, or defaults when the file is missing.
    """
    from audit_service.backend.config.pipeline import default_config_dir

    root = config_dir or default_config_dir()
    path = root / "app.config"
    if not path.is_file():
        return AppConfig()

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return AppConfig.model_validate(data)
