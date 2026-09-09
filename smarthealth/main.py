"""ASGI entrypoint — `smarthealth.main:app`, the module gunicorn/uvicorn and
the Docker image's CMD target.

Re-exports the FastAPI app built by the identity service's app factory.
Later services (Plan 04/05 and beyond) mount additional routers onto this
same app rather than each defining their own ASGI entrypoint, keeping one
process serving the whole Phase-1 API surface.
"""

from __future__ import annotations

from services.identity.app import app

__all__ = ["app"]
