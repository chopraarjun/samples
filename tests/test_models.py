"""Tests for Pydantic domain models and validation error formatting."""

import pytest
from pydantic import ValidationError

from audit_service.backend.models import EventQueryParams, EventRecord, format_validation_error


def test_event_query_params_requires_tenant_id() -> None:
    """Verify EventQueryParams rejects construction without tenant_id."""
    with pytest.raises(ValidationError):
        EventQueryParams()


def test_event_query_params_rejects_invalid_tenant_id() -> None:
    """Verify EventQueryParams rejects tenant IDs with invalid characters."""
    with pytest.raises(ValidationError):
        EventQueryParams(tenant_id="bad-tenant!")


def test_event_query_params_accepts_valid_tenant_id() -> None:
    """Verify EventQueryParams accepts alphanumeric underscore tenant IDs."""
    params = EventQueryParams(tenant_id="acme_corp")
    assert params.tenant_id == "acme_corp"


def test_format_validation_error_includes_field_and_message() -> None:
    """Verify format_validation_error includes field name and message text."""
    try:
        EventRecord.model_validate(
            {
                "event_id": "e1",
                "tenant_id": "bad-tenant!",
                "action": "download",
                "package": "pkg",
                "timestamp": "2025-03-15T10:00:00+00:00",
                "actor": "a",
            }
        )
    except ValidationError as exc:
        message = format_validation_error(exc)
        assert "tenant_id:" in message
        assert "Invalid tenant_id" in message
    else:
        pytest.fail("expected ValidationError")


def test_format_validation_error_joins_multiple_errors() -> None:
    """Verify format_validation_error joins multiple field errors with semicolons."""
    try:
        EventRecord.model_validate({})
    except ValidationError as exc:
        message = format_validation_error(exc)
        assert "; " in message
        assert message.count(":") >= 2
    else:
        pytest.fail("expected ValidationError")
