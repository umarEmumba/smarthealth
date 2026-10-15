"""Settings isolation (assumption-delta decision, plan 01.1-01): a service's
database URL is read ONLY from its own prefixed variable.

pydantic-settings' `populate_by_name=True` would also read the env var named
after the field, so a stale unprefixed `DATABASE_URL` would silently point
Identity at the wrong database. `SharedSettings` therefore binds by alias only.
"""

from __future__ import annotations

import pytest

from libs.config import SharedSettings
from services.identity.config import IdentitySettings


def test_identity_reads_its_prefixed_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://wrong/wrong")
    monkeypatch.setenv("IDENTITY_DATABASE_URL", "postgresql+asyncpg://right/right")
    assert IdentitySettings().database_url == "postgresql+asyncpg://right/right"


def test_identity_ignores_a_stale_unprefixed_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://wrong/wrong")
    monkeypatch.delenv("IDENTITY_DATABASE_URL", raising=False)
    assert "wrong" not in IdentitySettings().database_url


def test_shared_settings_has_no_database_url_or_shared_signing_secret() -> None:
    fields = set(SharedSettings.model_fields)
    assert "database_url" not in fields
    assert not {name for name in fields if name.startswith("jwt_")}
    assert not {
        name for name in IdentitySettings.model_fields if "secret" in name or "algorithm" in name
    }


def test_jwks_url_derives_from_identity_service_url() -> None:
    settings = SharedSettings(identity_service_url="http://identity:8000")
    assert settings.jwks_url == "http://identity:8000/.well-known/jwks.json"
