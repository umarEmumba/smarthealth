"""Unit tests (no infrastructure) for `libs.auth.verification.decode_token`.

These lock the verifier's security properties (STRIDE T-01.1-02): RS256
pinned, `kid` required and used for key selection, token type checked, and
every forgery shape rejected — alg:none, a symmetric-algorithm token "signed"
with the PUBLIC key as the HMAC secret, expired tokens, tampered payloads.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from jose import jwt

from libs.auth.verification import (
    ACCESS_ONLY,
    INTERNAL_CALLER_TYPES,
    REFRESH_ONLY,
    StaticKeyResolver,
    TokenError,
    decode_token,
)
from services.identity.security.keys import SigningKey, generate_private_key_pem, load_signing_key


def _make_key(tmp_path: Path, kid: str) -> SigningKey:
    path = tmp_path / f"{kid}.pem"
    path.write_text(generate_private_key_pem())
    return load_signing_key(path, kid)


def _claims(token_type: str = "access", *, exp_offset: int = 900) -> dict[str, Any]:
    now = int(time.time())
    return {
        "sub": "11111111-1111-1111-1111-111111111111",
        "role": "admin",
        "clinic_id": None,
        "jti": "jti-1",
        "type": token_type,
        "iat": now,
        "exp": now + exp_offset,
    }


def _sign(claims: dict[str, Any], key: SigningKey, *, with_kid: bool = True) -> str:
    headers = {"kid": key.kid} if with_kid else {}
    return jwt.encode(claims, key.private_pem, algorithm="RS256", headers=headers)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


@pytest.fixture
def key(tmp_path: Path) -> SigningKey:
    return _make_key(tmp_path, "key-1")


@pytest.fixture
def resolver(key: SigningKey) -> StaticKeyResolver:
    return StaticKeyResolver({key.kid: key.public_jwk})


async def test_valid_access_token_decodes(key: SigningKey, resolver: StaticKeyResolver) -> None:
    claims = await decode_token(_sign(_claims(), key), expected_types=ACCESS_ONLY, resolver=resolver)
    assert claims["role"] == "admin"
    assert claims["type"] == "access"


async def test_refresh_token_rejected_when_access_expected(
    key: SigningKey, resolver: StaticKeyResolver
) -> None:
    token = _sign(_claims("refresh"), key)
    with pytest.raises(TokenError):
        await decode_token(token, expected_types=ACCESS_ONLY, resolver=resolver)
    # ...and accepted where a refresh token is what the endpoint expects.
    claims = await decode_token(token, expected_types=REFRESH_ONLY, resolver=resolver)
    assert claims["type"] == "refresh"


async def test_service_token_only_accepted_for_internal_callers(
    key: SigningKey, resolver: StaticKeyResolver
) -> None:
    token = _sign(_claims("service"), key)
    with pytest.raises(TokenError):
        await decode_token(token, expected_types=ACCESS_ONLY, resolver=resolver)
    claims = await decode_token(token, expected_types=INTERNAL_CALLER_TYPES, resolver=resolver)
    assert claims["type"] == "service"


async def test_missing_kid_header_rejected(key: SigningKey, resolver: StaticKeyResolver) -> None:
    token = _sign(_claims(), key, with_kid=False)
    with pytest.raises(TokenError):
        await decode_token(token, expected_types=ACCESS_ONLY, resolver=resolver)


async def test_unknown_kid_rejected(key: SigningKey, resolver: StaticKeyResolver) -> None:
    other = SigningKey(kid="not-published", private_pem=key.private_pem, public_jwk=key.public_jwk)
    token = _sign(_claims(), other)
    with pytest.raises(TokenError):
        await decode_token(token, expected_types=ACCESS_ONLY, resolver=resolver)


async def test_alg_none_forgery_rejected(key: SigningKey, resolver: StaticKeyResolver) -> None:
    header = _b64(json.dumps({"alg": "none", "typ": "JWT", "kid": key.kid}).encode())
    payload = _b64(json.dumps(_claims()).encode())
    forged = f"{header}.{payload}."
    with pytest.raises(TokenError):
        await decode_token(forged, expected_types=ACCESS_ONLY, resolver=resolver)


async def test_symmetric_algorithm_forgery_with_public_key_secret_rejected(
    key: SigningKey, resolver: StaticKeyResolver
) -> None:
    """Classic algorithm-confusion attack: sign with HS256 using the (public)
    key material as the HMAC secret. Built by hand because python-jose itself
    refuses to HMAC with a PEM — an attacker would not use python-jose."""
    public_pem = (
        serialization.load_pem_private_key(key.private_pem.encode(), password=None)
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT", "kid": key.kid}).encode())
    payload = _b64(json.dumps(_claims()).encode())
    signing_input = f"{header}.{payload}".encode()
    signature = _b64(hmac.new(public_pem, signing_input, hashlib.sha256).digest())
    forged = f"{header}.{payload}.{signature}"

    with pytest.raises(TokenError):
        await decode_token(forged, expected_types=ACCESS_ONLY, resolver=resolver)


async def test_expired_token_rejected(key: SigningKey, resolver: StaticKeyResolver) -> None:
    # IAM-03 adjacency edge: exp has JUST passed.
    token = _sign(_claims(exp_offset=-1), key)
    with pytest.raises(TokenError):
        await decode_token(token, expected_types=ACCESS_ONLY, resolver=resolver)


async def test_tampered_payload_rejected(key: SigningKey, resolver: StaticKeyResolver) -> None:
    token = _sign(_claims(), key)
    header, _payload, signature = token.split(".")
    escalated = dict(_claims(), role="patient", jti="jti-evil")
    tampered = f"{header}.{_b64(json.dumps(escalated).encode())}.{signature}"
    with pytest.raises(TokenError):
        await decode_token(tampered, expected_types=ACCESS_ONLY, resolver=resolver)


@pytest.mark.parametrize("garbage", ["", "not-a-jwt", "a.b.c", "....", " "])
async def test_empty_or_garbage_token_rejected(
    garbage: str, resolver: StaticKeyResolver
) -> None:
    # IAM-03 empty-input edge: never accepted by falling back to a default key.
    with pytest.raises(TokenError):
        await decode_token(garbage, expected_types=ACCESS_ONLY, resolver=resolver)


async def test_key_selection_is_by_kid_not_position(tmp_path: Path) -> None:
    # IAM-03 ordering edge: the token is signed by the SECOND published key and
    # must verify against that key — and a token that claims the first key's
    # kid but was signed by the second must NOT verify.
    first = _make_key(tmp_path, "key-a")
    second = _make_key(tmp_path, "key-b")
    resolver = StaticKeyResolver({first.kid: first.public_jwk, second.kid: second.public_jwk})

    good = _sign(_claims(), second)
    assert (await decode_token(good, expected_types=ACCESS_ONLY, resolver=resolver))["jti"] == "jti-1"

    misattributed = jwt.encode(
        _claims(), second.private_pem, algorithm="RS256", headers={"kid": first.kid}
    )
    with pytest.raises(TokenError):
        await decode_token(misattributed, expected_types=ACCESS_ONLY, resolver=resolver)


# --- audience and required exp (plan 02-01, D-06, review WR-03) --------------------


async def test_token_with_aud_decodes_with_matching_audience(
    key: SigningKey, resolver: StaticKeyResolver
) -> None:
    token = _sign(dict(_claims("service"), aud="provider"), key)
    claims = await decode_token(
        token, expected_types=INTERNAL_CALLER_TYPES, resolver=resolver, audience="provider"
    )
    assert claims["aud"] == "provider"


async def test_token_with_aud_rejected_for_other_audience(
    key: SigningKey, resolver: StaticKeyResolver
) -> None:
    token = _sign(dict(_claims("service"), aud="provider"), key)
    with pytest.raises(TokenError):
        await decode_token(
            token, expected_types=INTERNAL_CALLER_TYPES, resolver=resolver, audience="patient"
        )


async def test_token_with_aud_rejected_when_no_audience_expected(
    key: SigningKey, resolver: StaticKeyResolver
) -> None:
    token = _sign(dict(_claims("service"), aud="provider"), key)
    with pytest.raises(TokenError):
        await decode_token(token, expected_types=INTERNAL_CALLER_TYPES, resolver=resolver)


async def test_token_without_aud_still_decodes_with_an_audience(
    key: SigningKey, resolver: StaticKeyResolver
) -> None:
    """User access tokens carry no aud and must keep working on internal routes;
    python-jose skips the check, which is why `require_internal_caller` checks
    `aud` presence itself for service tokens."""
    claims = await decode_token(
        _sign(_claims(), key), expected_types=ACCESS_ONLY, resolver=resolver, audience="provider"
    )
    assert "aud" not in claims


async def test_token_without_exp_rejected(key: SigningKey, resolver: StaticKeyResolver) -> None:
    claims = _claims()
    del claims["exp"]
    with pytest.raises(TokenError):
        await decode_token(_sign(claims, key), expected_types=ACCESS_ONLY, resolver=resolver)
