"""Key lifecycle tests (STRIDE T-01.1-03): the signing key is generated safely,
loaded fail-closed, never exposed by the JWKS, and never committed.

Pure-unit except `test_health_endpoint_reports_ok`, which drives Identity's
real app against live Postgres.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from services.identity.security.keys import (
    SigningKeyUnavailable,
    generate_private_key_pem,
    get_signing_key,
    jwks_document,
    load_signing_key,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "generate_jwt_keypair.py"
_PRIVATE_JWK_MEMBERS = {"d", "p", "q", "dp", "dq", "qi"}


def _run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_missing_key_file_raises_with_guidance(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.pem"
    with pytest.raises(SigningKeyUnavailable) as excinfo:
        load_signing_key(missing, "kid-1")
    message = str(excinfo.value)
    assert str(missing) in message
    assert "scripts/generate_jwt_keypair.py" in message


def test_garbage_key_file_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "garbage.pem"
    path.write_text("this is not a pem")
    with pytest.raises(SigningKeyUnavailable):
        load_signing_key(path, "kid-1")


def test_non_rsa_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "ec.pem"
    path.write_bytes(
        ec.generate_private_key(ec.SECP256R1()).private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    with pytest.raises(SigningKeyUnavailable):
        load_signing_key(path, "kid-1")


def test_jwks_document_exposes_public_members_only() -> None:
    keys = jwks_document()["keys"]
    assert len(keys) == 1
    key = keys[0]
    assert key["kty"] == "RSA"
    assert key["use"] == "sig"
    assert key["alg"] == "RS256"
    assert key["kid"]
    assert not (_PRIVATE_JWK_MEMBERS & set(key)), "private JWK members must never be published"


def test_signing_key_repr_never_contains_key_material() -> None:
    assert "BEGIN" not in repr(get_signing_key())


def test_loaded_key_matches_generated_pem(tmp_path: Path) -> None:
    path = tmp_path / "k.pem"
    path.write_text(generate_private_key_pem())
    loaded = load_signing_key(path, "kid-1")
    assert loaded.kid == "kid-1"
    assert loaded.public_jwk["kid"] == "kid-1"


def test_keypair_script_writes_owner_only_pem(tmp_path: Path) -> None:
    output = tmp_path / "keys" / "jwt_private.pem"
    result = _run_script("--output", str(output))

    assert result.returncode == 0, result.stderr
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert load_signing_key(output, "kid-1").public_jwk["kty"] == "RSA"
    # Prints the path and a reminder — never the key material.
    assert str(output) in result.stdout
    assert "BEGIN" not in result.stdout and "BEGIN" not in result.stderr


def test_keypair_script_refuses_to_overwrite_without_force(tmp_path: Path) -> None:
    output = tmp_path / "jwt_private.pem"
    assert _run_script("--output", str(output)).returncode == 0
    original = output.read_bytes()

    refused = _run_script("--output", str(output))
    assert refused.returncode == 1
    assert output.read_bytes() == original

    forced = _run_script("--output", str(output), "--force")
    assert forced.returncode == 0
    assert output.read_bytes() != original
    assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_identity_refuses_to_build_without_a_signing_key() -> None:
    env = dict(os.environ, JWT_PRIVATE_KEY_PATH="/nonexistent/key.pem")
    result = subprocess.run(
        [sys.executable, "-c", "import services.identity.app"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "SigningKeyUnavailable" in result.stderr


async def test_health_router_reports_503_when_database_is_unreachable() -> None:
    from libs.health import build_health_router  # noqa: PLC0415

    app = FastAPI()
    # Port 1 refuses connections immediately; no infrastructure needed.
    app.include_router(
        build_health_router("demo", lambda: "postgresql+asyncpg://nobody:x@127.0.0.1:1/none")
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://demo") as client:
        response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "unhealthy", "service": "demo"}


@pytest.mark.integration
async def test_health_endpoint_reports_ok(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "identity"}
