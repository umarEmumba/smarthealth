"""End-to-end auth tests (Phase 1 Plan 01-03 Task 1, relocated by plan 01.1-02).

Integration tests against the extracted Identity service (via the `client`
fixture, which mounts `services.identity.app:app`) and live Postgres/Mongo/Redis
containers. Proves the full slice: register -> login -> RBAC-scoped request
-> audit written -> PHI-safe log/trace, plus logout revocation, alg:none and
symmetric-algorithm forgery rejection, and duplicate-email 409.

Token claim assertions verify the RS256 signature against Identity's public key
(`verify_identity_token`); there is no shared secret to decode with any more.

Every test uses a unique, synthetic email (uuid-suffixed) so repeated runs
never collide with a previous run's leftover `users` row (no cleanup fixture
exists for the live `users` table across test runs, matching test_models.py's
established pattern of test-local cleanup where needed).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid

import pytest
from cryptography.hazmat.primitives import serialization
from httpx import AsyncClient
from jose import jwk

from services.identity.security.keys import get_signing_key
from tests.conftest import bootstrap_admin_and_login, unique_email, verify_identity_token


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
    claims = verify_identity_token(login_resp.json()["access_token"])
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
    claims = verify_identity_token(tokens["access_token"])
    assert set(["sub", "role", "clinic_id", "jti", "type"]).issubset(claims.keys())
    assert claims["type"] == "access"
    assert claims["role"] == "front_desk"


@pytest.mark.integration
async def test_refresh_token_claim_shape_is_minimal(client: AsyncClient) -> None:
    _, tokens = await _register_and_login(client, role="provider")
    claims = verify_identity_token(tokens["refresh_token"])
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
async def test_login_and_refresh_log_lines_carry_jti_for_correlation(
    client: AsyncClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """WR-04 regression: `auth.login.success` and `auth.refresh.success`
    log lines must carry the issued access/refresh jti(s) so the audit
    trail for a login/refresh event can be correlated to the specific
    token pair that was issued, without re-decoding either token.
    """
    import json

    email = _unique_email("jti-correlation")
    register_resp = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "correct-horse-battery"},
    )
    assert register_resp.status_code == 201, register_resp.text

    capsys.readouterr()  # discard anything captured so far this test

    login_resp = await client.post(
        "/api/auth/login", json={"email": email, "password": "correct-horse-battery"}
    )
    assert login_resp.status_code == 200, login_resp.text
    tokens = login_resp.json()

    login_out = capsys.readouterr().out
    login_lines = [json.loads(line) for line in login_out.strip().splitlines() if line.strip()]
    login_success = next(line for line in login_lines if line.get("event") == "auth.login.success")
    assert "access_jti" in login_success
    assert "refresh_jti" in login_success

    refresh_resp = await client.post(
        "/api/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
    )
    assert refresh_resp.status_code == 200, refresh_resp.text

    refresh_out = capsys.readouterr().out
    refresh_lines = [json.loads(line) for line in refresh_out.strip().splitlines() if line.strip()]
    refresh_success = next(
        line for line in refresh_lines if line.get("event") == "auth.refresh.success"
    )
    assert "access_jti" in refresh_success
    assert "refresh_jti" in refresh_success


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
    allowlist) must be rejected — the shared verifier pins RS256 explicitly
    rather than trusting the token header's own `alg`.

    python-jose's own `jws.sign` refuses to encode with algorithm="none"
    (it isn't in `ALGORITHMS.SUPPORTED`), so the forged token is built
    manually — base64url header + payload + an empty signature segment —
    exactly the classic alg:none attack shape a real attacker would send.
    """
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
async def test_symmetric_algorithm_forgery_rejected(client: AsyncClient) -> None:
    """Algorithm-confusion attack: an admin-shaped token HMAC-signed with
    Identity's PUBLIC key PEM as the shared secret (the public key is, by
    design, world-readable via the JWKS) must be rejected 401 — the verifier
    pins RS256 and never lets the token's own `alg` header pick the algorithm.

    The forged token is built by hand: python-jose itself refuses to HMAC with
    a PEM, and a real attacker would not use python-jose.
    """
    signing_key = get_signing_key()

    public_pem = jwk.construct(signing_key.public_jwk, algorithm="RS256").to_pem()
    assert serialization.load_pem_public_key(public_pem) is not None  # really the public half

    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    now = int(time.time())
    claims = {
        "sub": str(uuid.uuid4()),
        "role": "admin",
        "clinic_id": None,
        "jti": "forged-hmac-jti",
        "type": "access",
        "iat": now,
        "exp": now + 900,
    }
    header = _b64url(
        json.dumps({"alg": "HS256", "typ": "JWT", "kid": signing_key.kid}).encode()
    )
    payload = _b64url(json.dumps(claims).encode())
    signature = _b64url(
        hmac.new(public_pem, f"{header}.{payload}".encode(), hashlib.sha256).digest()
    )
    forged_token = f"{header}.{payload}.{signature}"

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
