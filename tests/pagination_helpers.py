"""Test helper: read every row of a paginated list endpoint.

A long-lived dev database accumulates clinics and patients across test runs, so
a test that must find a row it just created cannot assume it sits on the first
page. `list_all` pages with the maximum page size until a short page.
"""

from __future__ import annotations

from httpx import AsyncClient

from libs.pagination import MAX_LIMIT


async def list_all(
    client: AsyncClient,
    path: str,
    headers: dict | None = None,
    params: dict | None = None,
) -> list[dict]:
    """Return the concatenated rows of every page of `GET path`."""
    rows: list[dict] = []
    offset = 0
    while True:
        page_params = {**(params or {}), "limit": MAX_LIMIT, "offset": offset}
        response = await client.get(path, headers=headers, params=page_params)
        assert response.status_code == 200, response.text
        page = response.json()
        rows.extend(page)
        if len(page) < MAX_LIMIT:
            return rows
        offset += MAX_LIMIT
