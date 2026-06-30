"""Shared validation helpers for tenant IDs and Pydantic error formatting.

Main entry points:
    validate_tenant_id: Enforce alphanumeric/underscore tenant identifiers.
    format_validation_error: Serialize :class:`pydantic.ValidationError` for clients.
    TENANT_ID_PATTERN: Compiled regex used by tenant ID validation.
"""

import re

from pydantic import ValidationError

TENANT_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")
"""Regex matching valid tenant identifiers (letters, digits, underscore only)."""


def validate_tenant_id(value: str) -> str:
    """Require alphanumeric and underscore tenant identifiers.

    Args:
        value: Candidate tenant ID string.

    Returns:
        The validated tenant ID unchanged.

    Raises:
        ValueError: When *value* does not match :data:`TENANT_ID_PATTERN`.
    """
    if not TENANT_ID_PATTERN.match(value):
        raise ValueError(f"Invalid tenant_id: {value!r}")
    return value


def format_validation_error(exc: ValidationError) -> str:
    """Format Pydantic errors as ``field: message`` for DLQ rows and API 422 responses.

    Args:
        exc: Validation error from a Pydantic model.

    Returns:
        Semicolon-separated list of ``loc: msg`` pairs, or ``"Validation error"``
        when no error details are present.
    """
    parts: list[str] = []
    for err in exc.errors():
        field = ".".join(str(part) for part in err["loc"])
        parts.append(f"{field}: {err['msg']}")
    return "; ".join(parts) if parts else "Validation error"
