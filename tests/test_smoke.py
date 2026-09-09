"""Smoke tests proving the pytest + asyncio_mode=auto infrastructure works.

No PHI, no real credentials — these tests only touch smarthealth.config's
settings object (always local-dev placeholder values) and asyncio primitives.
"""

from __future__ import annotations

import asyncio

from smarthealth.config import settings


def test_settings_import() -> None:
    """settings imports and exposes the fields every later plan depends on."""
    assert settings.jwt_algorithm == "HS256"
    assert settings.database_url.startswith("postgresql+asyncpg://")
    assert settings.mongo_db_name
    assert settings.redis_url.startswith("redis://")


async def test_asyncio_mode_auto_works() -> None:
    """A bare `async def test_...` runs without an explicit @pytest.mark.asyncio,
    proving asyncio_mode = "auto" (set in pyproject.toml) is honored.
    """
    await asyncio.sleep(0)
    assert True
