"""Pipeline configuration loaded from ``config/{name}.config`` files.

Main entry points:
    PipelineConfig: Per-pipeline operational settings model.
    load_pipeline_config: Load TOML for a named pipeline.
    parse_pipeline_cli: Parse pipeline name from CLI argv.
    validate_sql_identifier: Validate safe SQL identifier strings.
"""

import argparse
import os
import re
import tomllib
from pathlib import Path

from pydantic import BaseModel, Field

_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_sql_identifier(value: str) -> str:
    """Ensure *value* is a safe unquoted SQL identifier.

    Args:
        value: Candidate identifier (letters, digits, underscore; no leading digit).

    Returns:
        The validated identifier unchanged.

    Raises:
        ValueError: When *value* does not match the identifier pattern.
    """
    if not _IDENTIFIER.match(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


class PipelineConfig(BaseModel):
    """Per-pipeline operational settings; record schema lives on table models.

    Attributes:
        name: Pipeline identifier (set from CLI / config filename).
        ingest_batch_size: Max landing files processed per ingest invocation.
        retention_days: Age threshold for partition deletion in retention CLI.
        watch_interval_seconds: Default idle sleep for ``audit-ingest --watch``.
    """

    name: str = Field(min_length=1)
    ingest_batch_size: int = Field(default=5, ge=1)
    retention_days: int = Field(default=90, ge=1)
    watch_interval_seconds: int = Field(
        default=30,
        ge=1,
        description="Default sleep between batch cycles when audit-ingest --watch is used",
    )


def default_config_dir() -> Path:
    """Return the configuration directory path.

    Uses the ``AUDIT_CONFIG_DIR`` environment variable when set, otherwise
    ``config`` relative to the working directory.

    Returns:
        Path to the directory containing ``app.config`` and pipeline configs.
    """
    return Path(os.environ.get("AUDIT_CONFIG_DIR", "config"))


def load_pipeline_config(name: str, *, config_dir: Path | None = None) -> PipelineConfig:
    """Load ``config/{name}.config`` (TOML) for a named pipeline.

    Args:
        name: Pipeline name (also written into the returned model).
        config_dir: Override config root; defaults to
            :func:`default_config_dir`.

    Returns:
        Validated :class:`PipelineConfig` with ``name`` set from *name*.

    Raises:
        FileNotFoundError: When ``{name}.config`` does not exist.
    """
    root = config_dir or default_config_dir()
    path = root / f"{name}.config"
    if not path.is_file():
        raise FileNotFoundError(f"Pipeline config not found: {path.resolve()}")

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    data["name"] = name
    return PipelineConfig.model_validate(data)


def parse_pipeline_cli(argv: list[str] | None = None, *, default: str = "events") -> str:
    """Parse the pipeline name positional argument from CLI argv.

    Args:
        argv: Argument vector; uses ``sys.argv`` when ``None``.
        default: Pipeline name when the positional argument is omitted.

    Returns:
        Selected pipeline name (without ``.config`` extension).
    """
    parser = argparse.ArgumentParser(description="Pipeline config name (config/{name}.config)")
    parser.add_argument(
        "pipeline",
        nargs="?",
        default=default,
        help=f"Pipeline name without .config extension (default: {default})",
    )
    return parser.parse_args(argv).pipeline
