"""Backend packages for configuration, storage, ingestion, and retention.

Subpackages:
    config: Application and pipeline TOML configuration loaders.
    db: DuckDB client and pipeline storage registry.
    models: Pydantic domain models and Parquet table implementations.
    storage: Shared Parquet path helpers and storage base types.
    processing: CLI entry points for ingest, retention, reset, and split.
    logging: Shared logging setup for processing commands.
"""
