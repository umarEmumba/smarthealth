"""Service tokens for processes with no user context (plan 02-01, D-06).

A Scheduling / Visit / Billing worker authenticates to Identity's
client-credentials mint (`POST /internal/service-tokens`) with its own
per-service secret and gets a 60-second token naming exactly ONE target service
in `aud`. Every internal route then requires its own audience on a service token
and an explicit allowed-callers set; user access tokens (no `aud`) are unchanged.

Secrets come from the throwaway files `tests/conftest.py` writes before import
(`SCHEDULING_CLIENT_SECRET_PATH`, ...); no secret is ever printed.
"""

from __future__ import annotations

import os

import httpx
import pytest
from httpx import AsyncClient

from libs.auth.service_tokens import ServiceTokenProvider, load_client_secret
from tests.conftest import bootstrap_admin_and_login, service_base_url, verify_identity_token

pytestmark = pytest.mark.integration


def _auth(tokens: dict) -> dict:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _secret(client: str) -> str:
    return load_client_secret(os.environ[f"{client.upper()}_CLIENT_SECRET_PATH"])


def _provider_for(client_name: str, http: httpx.AsyncClient, **kwargs) -> ServiceTokenProvider:
    return ServiceTokenProvider(
        client_name, _secret(client_name), service_base_url("identity"), http=http, **kwargs
    )


async def _create_provider(client: AsyncClient, admin: dict) -> tuple[str, str]:
    """(provider id, department id) of a fresh provider created through the gateway."""
    clinic = await client.post(
        "/api/clinics", json={"name": "Service Token Clinic", "timezone": "UTC"}, headers=_auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    department = await client.post(
        f"/api/clinics/{clinic.json()['id']}/departments",
        json={"name": "Service Token Dept"},
        headers=_auth(admin),
    )
    assert department.status_code == 201, department.text
    created = await client.post(
        "/api/providers",
        json={
            "department_id": department.json()["id"],
            "full_name": "Dr. Service Token",
            "specialties": ["General"],
        },
        headers=_auth(admin),
    )
    assert created.status_code == 201, created.text
    return created.json()["id"], department.json()["id"]


def _provider_url(provider_id: str) -> str:
    return f"{service_base_url('provider')}/internal/providers/{provider_id}"


# --- tracer --------------------------------------------------------------------------


async def test_scheduling_mints_provider_token_and_reads_internal_provider(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    tokens = _provider_for("scheduling", mesh)
    token = await tokens.get("provider")

    admin = await bootstrap_admin_and_login(client)
    provider_id, department_id = await _create_provider(client, admin)

    resp = await mesh.get(_provider_url(provider_id), headers=_bearer(token))

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"id": provider_id, "department_id": department_id, "is_active": True}

    claims = verify_identity_token(token, audience="provider")
    assert claims["aud"] == "provider"
    assert claims["sub"] == "service:scheduling"
    assert claims["type"] == "service"
    assert claims["exp"] - claims["iat"] == 60
