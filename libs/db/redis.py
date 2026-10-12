"""Per-request Redis dependency (shared JWT denylist + rate-limit counters).

A fresh client per request is intentionally simple — connection pooling/reuse
is a later optimization, not a correctness requirement for this phase.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from redis.asyncio import Redis

from libs.config import shared_settings


async def get_redis() -> AsyncIterator[Redis]:
    client = Redis.from_url(shared_settings.redis_url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()
