"""In-memory DuckDB SQL engine for Parquet reads and writes.

Main entry points:
    DuckDBClient: Thin wrapper around a DuckDB connection with transaction helpers.
"""

from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import duckdb


class DuckDBClient:
    """Ephemeral DuckDB connection with no persistent tables—only Parquet I/O via SQL.

    Attributes:
        _conn: Underlying :class:`duckdb.DuckDBPyConnection` instance.
    """

    def __init__(self) -> None:
        """Open a new in-memory DuckDB connection."""
        self._conn = duckdb.connect()

    @classmethod
    def connect(cls, *_args: object, **_kwargs: object) -> "DuckDBClient":
        """Create a new client instance (factory alias for consistency).

        Returns:
            A connected :class:`DuckDBClient`.
        """
        return cls()

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._conn.close()

    def __enter__(self) -> "DuckDBClient":
        """Enter a context manager returning ``self``.

        Returns:
            This client instance.
        """
        return self

    def __exit__(self, *args: object) -> None:
        """Exit the context manager and close the connection."""
        self.close()

    def execute(
        self, sql: str, parameters: Sequence[Any] | None = None
    ) -> duckdb.DuckDBPyConnection:
        """Execute a SQL statement.

        Args:
            sql: SQL text to run.
            parameters: Optional bound parameters for placeholders.

        Returns:
            The DuckDB connection after execution (for chaining).
        """
        if parameters is None:
            return self._conn.execute(sql)
        return self._conn.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> None:
        """Execute a SQL statement for each row in *parameters*.

        Args:
            sql: SQL text with placeholders.
            parameters: Sequence of parameter tuples per row.
        """
        self._conn.executemany(sql, parameters)

    def fetchone(self, sql: str, parameters: Sequence[Any] | None = None) -> tuple[Any, ...] | None:
        """Execute SQL and return the first result row.

        Args:
            sql: SQL text to run.
            parameters: Optional bound parameters.

        Returns:
            First row as a tuple, or ``None`` when the result set is empty.
        """
        return self.execute(sql, parameters).fetchone()

    def fetchall(self, sql: str, parameters: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute SQL and return all result rows.

        Args:
            sql: SQL text to run.
            parameters: Optional bound parameters.

        Returns:
            List of result rows as tuples.
        """
        return self.execute(sql, parameters).fetchall()

    def begin(self) -> None:
        """Start a database transaction."""
        self._conn.begin()

    def commit(self) -> None:
        """Commit the current transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._conn.rollback()

    @contextmanager
    def transaction(self) -> Iterator["DuckDBClient"]:
        """Context manager that commits on success and rolls back on error.

        Yields:
            This client for use inside the transaction block.

        Raises:
            Exception: Re-raises any exception after rolling back.
        """
        self.begin()
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise
