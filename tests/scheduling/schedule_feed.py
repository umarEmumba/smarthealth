"""Helpers for tests that read Provider's schedule-change feed (plan 02.1-08).

The feed is global, oldest first and capped at 200 per read, and other tests
(templates, providers) leave pending events behind in the shared test database.
Without draining it first, a fresh provider's events would eventually sit beyond
the first page, so every module that reads it drains it before each test with a
small autouse fixture calling `drain_feed` (the same idea as the autouse fixture
in `tests/provider/test_schedule_change_feed.py`). This is only safe because it
is the test database and no consumer runs.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx

from tests.conftest import service_base_url


async def scheduling_headers(mesh: httpx.AsyncClient) -> dict[str, str]:
    """A REAL Scheduling service token for `aud=provider`, minted through Identity."""
    from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret  # noqa: PLC0415

    provider = ServiceTokenProvider(
        "scheduling",
        load_client_secret(os.environ["SCHEDULING_CLIENT_SECRET_PATH"]),
        service_base_url("identity"),
        http=mesh,
    )
    return {"Authorization": f"Bearer {await provider.get('provider')}"}


def feed_url(path: str = "/internal/schedule-changes") -> str:
    return f"{service_base_url('provider')}{path}"


async def pending_events(mesh: httpx.AsyncClient) -> list[dict[str, Any]]:
    resp = await mesh.get(feed_url(), params={"limit": 200}, headers=await scheduling_headers(mesh))
    assert resp.status_code == 200, resp.text
    return resp.json()


async def drain_feed(mesh: httpx.AsyncClient) -> None:
    """Acknowledge every pending event."""
    headers = await scheduling_headers(mesh)
    for _ in range(1000):
        pending = await pending_events(mesh)
        if not pending:
            return
        for event in pending:
            ack = await mesh.post(feed_url(f"/internal/schedule-changes/{event['id']}/ack"), headers=headers)
            assert ack.status_code == 200, ack.text
    raise AssertionError("feed did not drain")


async def events_for(mesh: httpx.AsyncClient, provider_id: uuid.UUID) -> list[dict[str, Any]]:
    """The pending events of one provider (assertions are scoped to ids a test created)."""
    return [e for e in await pending_events(mesh) if e["provider_id"] == str(provider_id)]
