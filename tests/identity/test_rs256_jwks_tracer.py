"""TRACER (plan 01.1-01, Task 1): Identity signs RS256, publishes a JWKS, and
the shared verifier (`libs.auth`) validates Identity's tokens from the public
JWKS alone — through the multi-app in-process mesh, against live
Postgres/Mongo/Redis.

This is the riskiest seam of the whole re-architecture: if RS256 + JWKS +
database-per-service + the mounted-transport harness do not work end to end,
nothing downstream does. Every later service plan replicates this shape.
"""

from __future__ import annotations

import httpx
import pytest
from httpx import AsyncClient
from jose import jwt

from libs.auth.jwks_client import JwksClient
from libs.auth.verification import ACCESS_ONLY, decode_token
from tests.conftest import (
    bootstrap_admin_and_login,
    service_base_url,
    unique_email,
    verify_identity_token,
)

pytestmark = pytest.mark.integration

_PRIVATE_JWK_MEMBERS = {"d", "p", "q", "dp", "dq", "qi"}


async def test_login_issues_rs256_token_verifiable_via_jwks(
    client: AsyncClient, mesh: httpx.AsyncClient
) -> None:
    tokens = await bootstrap_admin_and_login(client)
    access_token = tokens["access_token"]

    header = jwt.get_unverified_header(access_token)
    assert header["alg"] == "RS256"
    assert header["kid"]

    jwks_resp = await client.get("/.well-known/jwks.json")
    assert jwks_resp.status_code == 200
    keys = jwks_resp.json()["keys"]
    assert header["kid"] in [key["kid"] for key in keys]
    for key in keys:
        assert key["kty"] == "RSA"
        assert key["use"] == "sig"
        assert key["alg"] == "RS256"
        assert not (_PRIVATE_JWK_MEMBERS & set(key)), "JWKS must never expose private members"

    jwks_client = JwksClient(f"{service_base_url('identity')}/.well-known/jwks.json")
    claims = await decode_token(
        access_token, expected_types=ACCESS_ONLY, resolver=jwks_client, http=mesh
    )
    assert claims["role"] == "admin"
    assert set(claims) == {"sub", "role", "clinic_id", "jti", "type", "iat", "exp"}
    assert jwks_client.fetch_count == 1

    # Local verification: the second decode must be served from the cache.
    await decode_token(access_token, expected_types=ACCESS_ONLY, resolver=jwks_client, http=mesh)
    assert jwks_client.fetch_count == 1


async def test_protected_route_rbac_through_shared_verifier(client: AsyncClient) -> None:
    admin_tokens = await bootstrap_admin_and_login(client)
    admin_headers = {"Authorization": f"Bearer {admin_tokens['access_token']}"}

    staff_email = unique_email("front-desk")
    staff_password = "front-desk-password-1"
    create_resp = await client.post(
        "/api/auth/admin/users",
        json={"email": staff_email, "password": staff_password, "role": "front_desk"},
        headers=admin_headers,
    )
    assert create_resp.status_code == 201, create_resp.text
    staff_login = await client.post(
        "/api/auth/login", json={"email": staff_email, "password": staff_password}
    )
    assert staff_login.status_code == 200, staff_login.text
    staff_headers = {"Authorization": f"Bearer {staff_login.json()['access_token']}"}

    assert (await client.get("/api/admin/ping", headers=admin_headers)).status_code == 200
    assert (await client.get("/api/admin/ping", headers=staff_headers)).status_code == 403
    # IAM-02 empty-input edge: no credentials is 401, never 403.
    assert (await client.get("/api/admin/ping")).status_code == 401
    garbage = {"Authorization": "Bearer not-a-jwt"}
    assert (await client.get("/api/admin/ping", headers=garbage)).status_code == 401
    empty = {"Authorization": "Bearer "}
    assert (await client.get("/api/admin/ping", headers=empty)).status_code == 401


async def test_logout_revokes_via_shared_denylist(client: AsyncClient) -> None:
    tokens = await bootstrap_admin_and_login(client)
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}

    assert (await client.get("/api/admin/ping", headers=headers)).status_code == 200

    logout_resp = await client.post("/api/auth/logout", headers=headers)
    assert logout_resp.status_code == 204

    assert (await client.get("/api/admin/ping", headers=headers)).status_code == 401


async def test_refresh_claims_stay_minimal(client: AsyncClient) -> None:
    tokens = await bootstrap_admin_and_login(client)

    claims = verify_identity_token(tokens["refresh_token"])
    assert claims["type"] == "refresh"
    assert claims["sub"]
    assert claims["jti"]
    assert set(claims) == {"sub", "jti", "type", "iat", "exp"}
    assert "role" not in claims
    assert "clinic_id" not in claims
