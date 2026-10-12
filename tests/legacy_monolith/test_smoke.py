"""Smoke tests proving the pytest + asyncio_mode=auto infrastructure works.

No PHI, no real credentials — these tests only touch smarthealth.config's
settings object (always local-dev placeholder values) and asyncio primitives.
"""

from __future__ import annotations

import asyncio

import pytest

from smarthealth.config import Settings, settings


def test_settings_import() -> None:
    """settings imports and exposes the fields every later plan depends on."""
    assert settings.jwt_algorithm == "HS256"
    assert settings.database_url.startswith("postgresql+asyncpg://")
    assert settings.mongo_db_name
    assert settings.redis_url.startswith("redis://")


def test_settings_defaults_to_development_and_boots_fine() -> None:
    """The default `environment="development"` is exempt from the CR-03
    fail-closed check, so constructing Settings with no overrides (the
    same shape local dev / CI relies on) must not raise.
    """
    s = Settings(_env_file=None)
    assert s.environment == "development"
    assert s.jwt_secret == "changeme-local-dev-only"


def test_settings_fails_closed_on_default_secret_outside_development() -> None:
    """CR-03: constructing Settings with `environment != "development"`
    and the well-known, source-visible default `jwt_secret` must raise
    immediately (fail-closed) rather than silently booting with a secret
    an attacker can read directly from this repository.
    """
    with pytest.raises(RuntimeError, match="JWT_SECRET must be set"):
        Settings(_env_file=None, environment="production")


def test_settings_boots_in_production_with_real_secret() -> None:
    """A non-development environment WITH a real, non-default secret must
    boot normally — the fail-closed check only rejects the insecure
    default, not the entire non-development environment class.
    """
    s = Settings(_env_file=None, environment="production", jwt_secret="a-real-generated-secret")
    assert s.environment == "production"
    assert s.jwt_secret == "a-real-generated-secret"


async def test_asyncio_mode_auto_works() -> None:
    """A bare `async def test_...` runs without an explicit @pytest.mark.asyncio,
    proving asyncio_mode = "auto" (set in pyproject.toml) is honored.
    """
    await asyncio.sleep(0)
    assert True
