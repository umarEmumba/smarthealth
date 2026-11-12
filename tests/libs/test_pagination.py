"""Unit tests for the shared pagination dependency (C5): bounds and defaults."""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient

from libs.pagination import DEFAULT_LIMIT, MAX_LIMIT, Page, page_params


def _app() -> FastAPI:
    app = FastAPI()

    @app.get("/items")
    async def items(page: Page = Depends(page_params)) -> dict:
        return {"limit": page.limit, "offset": page.offset}

    return app


@pytest.fixture
async def paged_client():
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as client:
        yield client


def test_constants_match_the_visit_list_convention() -> None:
    assert DEFAULT_LIMIT == 50
    assert MAX_LIMIT == 200


async def test_defaults_are_limit_50_offset_0(paged_client: AsyncClient) -> None:
    response = await paged_client.get("/items")
    assert response.status_code == 200
    assert response.json() == {"limit": 50, "offset": 0}


@pytest.mark.parametrize(("limit", "offset"), [(1, 0), (200, 0), (50, 1000), (7, 3)])
async def test_in_range_values_are_passed_through(
    paged_client: AsyncClient, limit: int, offset: int
) -> None:
    response = await paged_client.get("/items", params={"limit": limit, "offset": offset})
    assert response.status_code == 200
    assert response.json() == {"limit": limit, "offset": offset}


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 201},
        {"limit": -1},
        {"offset": -1},
        {"limit": "abc"},
        {"offset": "abc"},
        {"limit": "1.5"},
        {"limit": ""},
    ],
)
async def test_out_of_range_or_malformed_values_are_422_not_clamped(
    paged_client: AsyncClient, params: dict
) -> None:
    response = await paged_client.get("/items", params=params)
    assert response.status_code == 422


def test_page_is_frozen() -> None:
    page = Page(limit=5, offset=0)
    with pytest.raises(AttributeError):
        page.limit = 10  # type: ignore[misc]
