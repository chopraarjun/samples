"""ASGI re-export for deployment tools (uvicorn, gunicorn).

Main entry points:
    app: Default FastAPI application instance.
    create_app: Factory to build a configured FastAPI app for tests or custom config.
"""

from audit_service.api.main import app, create_app

__all__ = ["app", "create_app"]
