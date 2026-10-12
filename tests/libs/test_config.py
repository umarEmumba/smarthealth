"""Settings contract tests for the split configuration (plan 01.1-02).

Relocated from Phase 1's `test_smoke.py`, rewritten around the new settings
layout: `libs.config.SharedSettings` (infrastructure every service needs) and
`services.identity.config.IdentitySettings` (Identity's own prefixed database
URL and signing-key path). Verifies that the pytest + `asyncio_mode = "auto"`
infrastructure works and that no shared secret or unprefixed database URL
survives the split.

Supersedes the Phase 1 CR-03 "default JWT secret fails closed outside
development" tests: there is no shared JWT secret any more. The equivalent
fail-closed guarantee for RS256 — Identity refuses to start without a loadable
private key — is `SigningKeyUnavailable` in tests/identity/test_signing_key.py.

No PHI, no real credentials: only placeholder URLs and asyncio primitives.
"""

from __future__ import annotations

import asyncio

import pytest

from libs.config import SharedSettings, shared_settings
from services.identity.config import IdentitySettings


def test_shared_settings_exposes_infrastructure_fields() -> None:
    """SharedSettings carries the infra every service depends on, and derives
    the JWKS URL every verifier fetches from Identity's base URL."""
    assert shared_settings.mongo_db_name
    assert shared_settings.redis_url.startswith("redis://")
    assert shared_settings.otlp_endpoint.startswith("http")
    assert shared_settings.jwks_url.endswith("/.well-known/jwks.json")
    assert shared_settings.jwks_url.startswith(shared_settings.identity_service_url)


def test_identity_database_url_reads_prefixed_variable_despite_stale_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Database-per-service: Identity's URL comes from IDENTITY_DATABASE_URL
    only. A stale unprefixed DATABASE_URL (e.g. left over from the Phase 1
    monolith's .env) must never win."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://wrong/wrong")
    monkeypatch.setenv("IDENTITY_DATABASE_URL", "postgresql+asyncpg://right/identity_db")

    resolved = IdentitySettings(_env_file=None).database_url

    assert resolved.endswith("identity_db")
    assert "wrong" not in resolved
    assert resolved.startswith("postgresql+asyncpg://")


def test_no_shared_secret_or_algorithm_setting_exists() -> None:
    """RS256 + JWKS removed the shared signing secret and the configurable
    algorithm (the verifier pins RS256): no settings class may define either."""
    for settings_cls in (SharedSettings, IdentitySettings):
        offenders = [
            name
            for name in settings_cls.model_fields
            if "secret" in name or "algorithm" in name
        ]
        assert not offenders, f"{settings_cls.__name__} defines {offenders}"


def test_identity_settings_defaults_to_development() -> None:
    """The default environment boots without any production-only requirement
    (the key file path is validated when the app is built, not at import)."""
    settings = IdentitySettings(_env_file=None)
    assert settings.environment == "development"
    assert settings.database_url.startswith("postgresql+asyncpg://")


async def test_asyncio_mode_auto_works() -> None:
    """A bare `async def test_...` runs without an explicit @pytest.mark.asyncio,
    proving asyncio_mode = "auto" (set in pyproject.toml) is honored.
    """
    await asyncio.sleep(0)
    assert True
