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

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
from httpx import AsyncClient
from jose import jwt

from libs.auth.service_tokens import (
    ServiceTokenProvider,
    ServiceTokenUnavailable,
    load_client_secret,
)
from tests.conftest import (
    RequestRecorder,
    bootstrap_admin_and_login,
    degraded_mesh,
    service_base_url,
    verify_identity_token,
)

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


# --- helpers for hand-built tokens (real key + kid, exactly one claim wrong) ----------


def _signed(claims: dict[str, Any]) -> str:
    from services.identity.security.keys import get_signing_key  # noqa: PLC0415

    key = get_signing_key()
    return jwt.encode(claims, key.private_pem, algorithm="RS256", headers={"kid": key.kid})


def _service_claims(**overrides: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    claims: dict[str, Any] = {
        "sub": "service:scheduling",
        "aud": "provider",
        "type": "service",
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + timedelta(seconds=60),
    }
    claims.update(overrides)
    return {k: v for k, v in claims.items() if v is not _DROP}


_DROP = object()


def _mint_url() -> str:
    return f"{service_base_url('identity')}/internal/service-tokens"


async def _mint(mesh: httpx.AsyncClient, client_name: str, secret: str, audience: Any) -> httpx.Response:
    return await mesh.post(
        _mint_url(), json={"audience": audience}, auth=httpx.BasicAuth(client_name, secret)
    )


# --- verifier side: /internal/providers/{id} rejection matrix -------------------------


async def test_hand_built_provider_token_is_accepted(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _ = await _create_provider(client, admin)

    resp = await mesh.get(_provider_url(provider_id), headers=_bearer(_signed(_service_claims())))

    assert resp.status_code == 200, resp.text


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"aud": "organization"}, id="wrong-aud"),
        pytest.param({"aud": ["organization", "patient"]}, id="wrong-aud-list"),
        pytest.param({"aud": _DROP}, id="missing-aud"),
        pytest.param({"exp": _DROP}, id="missing-exp"),
        pytest.param(
            {
                "iat": datetime.now(timezone.utc) - timedelta(seconds=120),
                "exp": datetime.now(timezone.utc) - timedelta(seconds=60),
            },
            id="expired",
        ),
    ],
)
async def test_bad_service_token_on_provider_route_is_401(
    client: AsyncClient, mesh: httpx.AsyncClient, overrides: dict[str, Any]
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _ = await _create_provider(client, admin)

    resp = await mesh.get(
        _provider_url(provider_id), headers=_bearer(_signed(_service_claims(**overrides)))
    )

    assert resp.status_code == 401, resp.text


async def test_aud_list_naming_the_service_is_accepted(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _ = await _create_provider(client, admin)

    token = _signed(_service_claims(aud=["organization", "provider"]))
    resp = await mesh.get(_provider_url(provider_id), headers=_bearer(token))

    assert resp.status_code == 200, resp.text


async def test_unlisted_service_caller_on_provider_route_is_403(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    provider_id, _ = await _create_provider(client, admin)

    token = _signed(_service_claims(sub="service:billing"))
    resp = await mesh.get(_provider_url(provider_id), headers=_bearer(token))

    assert resp.status_code == 403, resp.text
    assert resp.json() == {"detail": "service not permitted for this operation"}


# --- Organization and Identity internal routes ---------------------------------------


async def test_organization_clinic_lookup_allowed_services(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    clinic = await client.post(
        "/api/clinics", json={"name": "Aud Clinic", "timezone": "UTC"}, headers=_auth(admin)
    )
    assert clinic.status_code == 201, clinic.text
    url = f"{service_base_url('organization')}/internal/clinics/{clinic.json()['id']}"

    for caller in ("identity", "scheduling"):
        token = _signed(_service_claims(sub=f"service:{caller}", aud="organization"))
        resp = await mesh.get(url, headers=_bearer(token))
        assert resp.status_code == 200, (caller, resp.text)

    visit = _signed(_service_claims(sub="service:visit", aud="organization"))
    assert (await mesh.get(url, headers=_bearer(visit))).status_code == 403
    # A provider-audience token is not an organization token.
    wrong_aud = _signed(_service_claims(sub="service:scheduling", aud="provider"))
    assert (await mesh.get(url, headers=_bearer(wrong_aud))).status_code == 401
    # The admin's forwarded user token (no aud) still works.
    assert (await mesh.get(url, headers=_auth(admin))).status_code == 200


async def test_organization_department_lookup_allows_scheduling_only(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    _provider_id, department_id = await _create_provider(client, admin)
    url = f"{service_base_url('organization')}/internal/departments/{department_id}"

    scheduling = _signed(_service_claims(sub="service:scheduling", aud="organization"))
    identity = _signed(_service_claims(sub="service:identity", aud="organization"))

    assert (await mesh.get(url, headers=_bearer(scheduling))).status_code == 200
    assert (await mesh.get(url, headers=_bearer(identity))).status_code == 403
    assert (await mesh.get(url, headers=_auth(admin))).status_code == 200


async def test_identity_user_lookup_refuses_every_service(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    admin = await bootstrap_admin_and_login(client)
    admin_id = verify_identity_token(admin["access_token"])["sub"]
    url = f"{service_base_url('identity')}/internal/users/{admin_id}"

    for caller in ("identity", "scheduling", "visit", "billing"):
        token = _signed(_service_claims(sub=f"service:{caller}", aud="identity"))
        resp = await mesh.get(url, headers=_bearer(token))
        assert resp.status_code == 403, (caller, resp.text)

    assert (await mesh.get(url, headers=_auth(admin))).status_code == 200


# --- mint endpoint ---------------------------------------------------------------------


async def test_mint_wrong_secret_and_unknown_client_get_identical_401(
    mesh: httpx.AsyncClient,
) -> None:
    wrong_secret = await _mint(mesh, "scheduling", "not-the-secret", "provider")
    unknown_client = await _mint(mesh, "mallory", _secret("scheduling"), "provider")
    no_credentials = await mesh.post(_mint_url(), json={"audience": "provider"})

    for resp in (wrong_secret, unknown_client, no_credentials):
        assert resp.status_code == 401, resp.text
        assert resp.json() == {"detail": "invalid client credentials"}
        assert "access_token" not in resp.text


async def test_mint_audience_outside_client_map_is_403(mesh: httpx.AsyncClient) -> None:
    resp = await _mint(mesh, "billing", _secret("billing"), "provider")

    assert resp.status_code == 403, resp.text
    assert "access_token" not in resp.text


@pytest.mark.parametrize("audience", ["Provider", "p" * 40, "", "provider/../x"])
async def test_mint_malformed_audience_is_422(mesh: httpx.AsyncClient, audience: str) -> None:
    resp = await _mint(mesh, "scheduling", _secret("scheduling"), audience)

    assert resp.status_code == 422, resp.text


async def test_mint_response_shape(mesh: httpx.AsyncClient) -> None:
    resp = await _mint(mesh, "visit", _secret("visit"), "scheduling")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == {"access_token", "token_type", "expires_in"}
    assert (body["token_type"], body["expires_in"]) == ("Bearer", 60)
    claims = verify_identity_token(body["access_token"], audience="scheduling")
    assert claims["sub"] == "service:visit"


# --- provider-side cache and failure ---------------------------------------------------


async def test_provider_caches_per_audience_and_refreshes_before_expiry(
    mesh: httpx.AsyncClient, request_recorder: RequestRecorder
) -> None:
    now = [1000.0]
    tokens = _provider_for("scheduling", mesh, clock=lambda: now[0])
    request_recorder.requests.clear()

    first = await tokens.get("provider")
    second = await tokens.get("provider")
    assert first == second
    assert len(request_recorder.paths("/internal/service-tokens")) == 1

    # A different audience is a different token.
    other = await tokens.get("organization")
    assert other != first
    assert verify_identity_token(other, audience="organization")["aud"] == "organization"
    assert len(request_recorder.paths("/internal/service-tokens")) == 2

    # Still inside the window (60 s lifetime - 15 s margin).
    now[0] += 44
    assert await tokens.get("provider") == first
    assert len(request_recorder.paths("/internal/service-tokens")) == 2

    # Past expires_at - 15 s: a fresh mint.
    now[0] += 2
    refreshed = await tokens.get("provider")
    assert len(request_recorder.paths("/internal/service-tokens")) == 3
    assert verify_identity_token(refreshed, audience="provider")["aud"] == "provider"


async def test_concurrent_gets_share_one_mint(
    mesh: httpx.AsyncClient, request_recorder: RequestRecorder
) -> None:
    tokens = _provider_for("scheduling", mesh)
    request_recorder.requests.clear()

    results = await asyncio.gather(*(tokens.get("patient") for _ in range(5)))

    assert len(set(results)) == 1
    assert len(request_recorder.paths("/internal/service-tokens")) == 1


async def test_provider_raises_unavailable_when_identity_is_down(mesh: httpx.AsyncClient) -> None:
    async with degraded_mesh("identity") as degraded:
        tokens = _provider_for("scheduling", degraded)
        with pytest.raises(ServiceTokenUnavailable):
            await tokens.get("provider")


async def test_provider_raises_unavailable_when_identity_refuses(mesh: httpx.AsyncClient) -> None:
    tokens = ServiceTokenProvider(
        "scheduling", "not-the-secret", service_base_url("identity"), http=mesh
    )

    with pytest.raises(ServiceTokenUnavailable):
        await tokens.get("provider")


def test_load_client_secret_missing_file_is_unavailable(tmp_path: Path) -> None:
    with pytest.raises(ServiceTokenUnavailable):
        load_client_secret(str(tmp_path / "absent.secret"))
    empty = tmp_path / "empty.secret"
    empty.write_text("  \n")
    with pytest.raises(ServiceTokenUnavailable):
        load_client_secret(str(empty))
