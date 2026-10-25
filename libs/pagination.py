"""Shared limit/offset pagination for list routes (C5).

Infrastructure only: no service or domain imports. Every list route that can
grow without bound declares `page: Page = Depends(page_params)`, orders its
query by a total order (`created_at, id`) and returns the grand total in the
`X-Total-Count` header, so the platform has one pagination style (Visit's list
established it: `limit` 1..200 default 50, `offset` >= 0).

Out-of-range or malformed values are a 422 from FastAPI's own validation — they
are never silently clamped, so a client asking for 10 000 rows learns the cap
instead of quietly getting fewer.
"""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Query

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
TOTAL_COUNT_HEADER = "X-Total-Count"


@dataclass(frozen=True)
class Page:
    """A validated page request."""

    limit: int
    offset: int


def page_params(
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
) -> Page:
    """FastAPI dependency: the `limit`/`offset` query parameters as a `Page`."""
    return Page(limit=limit, offset=offset)
