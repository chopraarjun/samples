"""Default ASGI application module for ``uvicorn audit_service.api.main:app``.

Main entry points:
    app: Pre-built FastAPI instance from :func:`audit_service.api.app.create_app`.
    create_app: Re-exported factory for custom configuration in tests.
"""

from audit_service.api.app import create_app

app = create_app()

__all__ = ["app", "create_app"]
