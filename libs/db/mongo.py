"""Loop-aware PyMongo async client (shared audit sink, D-02).

PyMongo's native async client (never Motor, per CLAUDE.md's "What NOT to
Use") binds itself to the event loop it was created on and raises
`RuntimeError` if reused from another loop. A process-wide singleton is
therefore only safe under ONE long-lived loop (a real uvicorn/gunicorn
worker) — NOT across pytest-asyncio's per-test loops. This module tracks the
owning loop and transparently builds a fresh client when it changes, so the
same call is correct in production and in the test suite.

Mongo is cross-cutting infrastructure (the shared `audit_log` collection),
not one of the per-service Postgres databases, so one client for the process
is correct for every service.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from pymongo import AsyncMongoClient

from libs.config import shared_settings

_mongo_client: AsyncMongoClient | None = None
_mongo_client_loop: asyncio.AbstractEventLoop | None = None


def get_mongo_client() -> AsyncMongoClient:
    """AsyncMongoClient bound to the current running loop (rebuilt if the
    cached one belongs to a different, e.g. prior test's, loop)."""
    global _mongo_client, _mongo_client_loop  # noqa: PLW0603

    current_loop = asyncio.get_running_loop()
    if _mongo_client is None or _mongo_client_loop is not current_loop:
        _mongo_client = AsyncMongoClient(shared_settings.mongo_url)
        _mongo_client_loop = current_loop
    return _mongo_client


async def get_mongo() -> AsyncIterator[AsyncMongoClient]:
    """FastAPI dependency yielding the loop-bound AsyncMongoClient — used by
    every mutating route to call `libs.audit.writer.write_audit`."""
    yield get_mongo_client()
