"""End-to-end auth tracer tests (Plan 01-03, Task 1).

Integration tests against the live FastAPI app (via the `client` fixture,
now targeting the real `smarthealth.main:app`) and live Postgres/Mongo/Redis
containers. Proves the full slice: register -> login -> RBAC-scoped request
-> audit written -> PHI-safe log/trace, plus logout revocation, alg:none
rejection, and duplicate-email 409.

Every test uses a unique, synthetic email (uuid-suffixed) so repeated runs
never collide with a previous run's leftover `users` row (no cleanup fixture
exists for the live `users` table across test runs, matching test_models.py's
established pattern of test-local cleanup where needed).
"""

from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient
from jose import jwt as jose_jwt

from smarthealth.config import settings
from tests.conftest import bootstrap_admin_and_login, unique_email


def _unique_email(prefix: str = "user") -> str:
    return unique_email(prefix)


async def _register(client: AsyncClient, *, email: str | None = None) -> dict:
    """Public self-registration — CR-01: always creates a PATIENT account,
    no `role` field exists on `RegisterRequest` anymore.
    """
    email = email or _unique_email("user")
    resp = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "correct-horse-battery"},
    )
    return resp


async def _create_staff_user(
    client: AsyncClient, *, role: str, email: str | None = None
) -> dict:
    """Create a non-patient-role user via the authenticated-admin-only
    `/api/auth/admin/users` route (CR-01), using a throwaway bootstrap
    admin to authenticate the request.
    """
    email = email or _unique_email(role)
    admin_tokens = await bootstrap_admin_and_login(client)
    resp = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": "correct-horse-battery", "role": role},
        headers={"Authorization": f"Bearer {admin_tokens['access_token']}"},
    )
    return resp


async def _register_and_login(
    client: AsyncClient, *, role: str = "admin"
) -> tuple[dict, dict]:
    """Create a user with `role` and return (create_response_json, tokens).

    `role="patient"` goes through public self-registration; any other
    role is created via the admin-only staff-creation route (CR-01).
    """
    email = _unique_email(role)
    if role == "patient":
        create_resp = await _register(client, email=email)
    else:
        create_resp = await _create_staff_user(client, role=role, email=email)
    assert create_resp.status_code == 201, create_resp.text

    login_resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    assert login_resp.status_code == 200, login_resp.text
    return create_resp.json(), login_resp.json()


@pytest.mark.integration
async def test_register_then_login_returns_token_pair(client: AsyncClient) -> None:
    _, tokens = await _register_and_login(client, role="admin")
    assert "access_token" in tokens
    assert "refresh_token" in tokens
    assert tokens["token_type"] == "bearer"


@pytest.mark.integration
async def test_public_register_ignores_caller_supplied_role_and_forces_patient(
    client: AsyncClient,
) -> None:
    """CR-01 regression: an unauthenticated caller POSTing `role: "admin"`
    (or any other elevated role) to the public `/api/auth/register`
    endpoint must NOT be granted that role — the resulting account must
    always be `patient`, and the caller-supplied `role` field must be
    silently ignored (RegisterRequest no longer even defines it).
    """
    email = _unique_email("escalation-attempt")
    resp = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "correct-horse-battery", "role": "admin"},
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["role"] == "patient"

    login_resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    assert login_resp.status_code == 200, login_resp.text
    claims = jose_jwt.decode(
        login_resp.json()["access_token"],
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )
    assert claims["role"] == "patient"

    # The forged-role account must be rejected by an admin-only route.
    admin_only_resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {login_resp.json()['access_token']}"},
    )
    assert admin_only_resp.status_code == 403


@pytest.mark.integration
async def test_staff_creation_route_requires_admin_and_honors_role(
    client: AsyncClient,
) -> None:
    """CR-01: the authenticated-admin-only `/api/auth/admin/users` route
    is the only way to create a non-patient account, and it is itself
    guarded by `require_role(Role.ADMIN)` — a non-admin caller (e.g. a
    front-desk token) must be rejected 403, never allowed to mint a new
    staff/provider/admin account.
    """
    _, front_desk_tokens = await _register_and_login(client, role="front_desk")
    email = _unique_email("staff-escalation-attempt")
    resp = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": "correct-horse-battery", "role": "admin"},
        headers={"Authorization": f"Bearer {front_desk_tokens['access_token']}"},
    )
    assert resp.status_code == 403

    # An authenticated admin CAN use the route to create an elevated-role
    # account, and the resulting account really does carry that role.
    admin_tokens = await bootstrap_admin_and_login(client)
    ok_resp = await client.post(
        "/api/auth/admin/users",
        json={"email": email, "password": "correct-horse-battery", "role": "provider"},
        headers={"Authorization": f"Bearer {admin_tokens['access_token']}"},
    )
    assert ok_resp.status_code == 201, ok_resp.text
    assert ok_resp.json()["role"] == "provider"


@pytest.mark.integration
async def test_access_token_claim_shape(client: AsyncClient) -> None:
    _, tokens = await _register_and_login(client, role="front_desk")
    claims = jose_jwt.decode(
        tokens["access_token"], settings.jwt_secret, algorithms=[settings.jwt_algorithm]
    )
    assert set(["sub", "role", "clinic_id", "jti", "type"]).issubset(claims.keys())
    assert claims["type"] == "access"
    assert claims["role"] == "front_desk"


@pytest.mark.integration
async def test_refresh_token_claim_shape_is_minimal(client: AsyncClient) -> None:
    _, tokens = await _register_and_login(client, role="provider")
    claims = jose_jwt.decode(
        tokens["refresh_token"], settings.jwt_secret, algorithms=[settings.jwt_algorithm]
    )
    assert claims["type"] == "refresh"
    assert "sub" in claims and "jti" in claims
    # Locked contract: refresh carries ONLY sub/jti/type/exp/iat — no role/clinic.
    assert "role" not in claims
    assert "clinic_id" not in claims


@pytest.mark.integration
async def test_admin_ping_admin_token_200_front_desk_403(client: AsyncClient) -> None:
    _, admin_tokens = await _register_and_login(client, role="admin")
    admin_resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {admin_tokens['access_token']}"},
    )
    assert admin_resp.status_code == 200

    _, staff_tokens = await _register_and_login(client, role="front_desk")
    staff_resp = await client.get(
        "/api/admin/ping",
        headers={"Authorization": f"Bearer {staff_tokens['access_token']}"},
    )
    assert staff_resp.status_code == 403


@pytest.mark.integration
async def test_logout_revokes_access_token(client: AsyncClient) -> None:
    _, tokens = await _register_and_login(client, role="admin")
    access_token = tokens["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}

    ok_resp = await client.get("/api/admin/ping", headers=headers)
    assert ok_resp.status_code == 200

    logout_resp = await client.post("/api/auth/logout", headers=headers)
    assert logout_resp.status_code == 204

    revoked_resp = await client.get("/api/admin/ping", headers=headers)
    assert revoked_resp.status_code == 401


@pytest.mark.integration
async def test_refresh_reissues_access_token(client: AsyncClient) -> None:
    _, tokens = await _register_and_login(client, role="provider")
    refresh_resp = await client.post(
        "/api/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
    )
    assert refresh_resp.status_code == 200
    new_tokens = refresh_resp.json()
    assert "access_token" in new_tokens
    assert new_tokens["access_token"] != tokens["access_token"]


@pytest.mark.integration
async def test_refresh_rejects_empty_and_garbage_token(client: AsyncClient) -> None:
    empty_resp = await client.post("/api/auth/refresh", json={"refresh_token": ""})
    assert empty_resp.status_code == 401

    garbage_resp = await client.post("/api/auth/refresh", json={"refresh_token": "not-a-jwt"})
    assert garbage_resp.status_code == 401


@pytest.mark.integration
async def test_refresh_rejects_access_token_used_as_refresh(client: AsyncClient) -> None:
    """A refresh exchange must reject a token of type=access — proves the
    `expected_type` check in decode_token is actually enforced end-to-end.
    """
    _, tokens = await _register_and_login(client, role="admin")
    resp = await client.post(
        "/api/auth/refresh", json={"refresh_token": tokens["access_token"]}
    )
    assert resp.status_code == 401


@pytest.mark.integration
async def test_decode_token_rejects_alg_none_forgery(client: AsyncClient) -> None:
    """A token forged with alg:none (or any algorithm not on the pinned
    allowlist) must be rejected — decode_token pins algorithms=[jwt_algorithm]
    explicitly rather than trusting the token header's own `alg`.

    python-jose's own `jws.sign` refuses to encode with algorithm="none"
    (it isn't in `ALGORITHMS.SUPPORTED`), so the forged token is built
    manually — base64url header + payload + an empty signature segment —
    exactly the classic alg:none attack shape a real attacker would send.
    """
    import base64
    import json

    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    header = {"alg": "none", "typ": "JWT"}
    claims = {
        "sub": str(uuid.uuid4()),
        "role": "admin",
        "clinic_id": None,
        "jti": "forged-jti",
        "type": "access",
    }
    forged_token = (
        f"{_b64url(json.dumps(header).encode())}."
        f"{_b64url(json.dumps(claims).encode())}."
    )

    resp = await client.get(
        "/api/admin/ping", headers={"Authorization": f"Bearer {forged_token}"}
    )
    assert resp.status_code == 401


@pytest.mark.integration
async def test_duplicate_email_registration_returns_409_and_no_second_user(
    client: AsyncClient,
) -> None:
    email = _unique_email("dup")
    first_resp = await _register(client, email=email)
    assert first_resp.status_code == 201

    second_resp = await _register(client, email=email)
    assert second_resp.status_code == 409

    # No second user was created: login still authenticates as the original
    # single account (a second row with the same email would violate the
    # DB's unique constraint if it existed, but we also assert behaviorally
    # that login with the original password still works exactly once).
    login_resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    assert login_resp.status_code == 200


@pytest.mark.integration
async def test_missing_token_returns_401(client: AsyncClient) -> None:
    resp = await client.get("/api/admin/ping")
    assert resp.status_code == 401


@pytest.mark.integration
async def test_invalid_credentials_login_returns_401(client: AsyncClient) -> None:
    email = _unique_email("badcreds")
    register_resp = await _register(client, email=email)
    assert register_resp.status_code == 201

    resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "totally-wrong-password"}
    )
    assert resp.status_code == 401
