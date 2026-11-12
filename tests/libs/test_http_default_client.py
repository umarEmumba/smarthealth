"""Regression: the default internal HTTP client works inside a real server loop.

`get_internal_http_client` is used as a FastAPI dependency. A plain `def`
dependency runs in a worker THREAD, where `asyncio.get_running_loop()` raises,
so with no injected client (every real container; the in-process test harness
always injects one) the first protected request failed with a 500. Found when
the real Compose stack first served traffic (plan 01.1-08).
"""

from __future__ import annotations

import httpx
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

from libs.http import get_internal_http_client


async def test_default_client_resolves_when_the_app_has_no_injected_client() -> None:
    app = FastAPI()
    app.state.internal_http_client = None  # production: use the process default

    @app.get("/probe")
    async def probe(http: httpx.AsyncClient = Depends(get_internal_http_client)) -> dict:
        return {"timeout_bounded": http.timeout.read is not None}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://probe") as client:
        resp = await client.get("/probe")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"timeout_bounded": True}


async def test_injected_client_wins_over_the_default() -> None:
    app = FastAPI()
    injected = httpx.AsyncClient()
    app.state.internal_http_client = injected

    @app.get("/probe")
    async def probe(http: httpx.AsyncClient = Depends(get_internal_http_client)) -> dict:
        return {"is_injected": http is injected}

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://probe") as client:
            resp = await client.get("/probe")
    finally:
        await injected.aclose()

    assert resp.json() == {"is_injected": True}
