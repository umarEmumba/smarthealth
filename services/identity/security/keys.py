"""Identity's RSA signing key: generation, fail-closed loading, JWKS export.

Only Identity ever loads the private key; everyone else sees the public half
through `GET /.well-known/jwks.json`. There is NO insecure fallback: if the
key file is missing, unreadable, or not RSA, loading raises
`SigningKeyUnavailable` and the service refuses to start (STRIDE T-01.1-03).

Key handling rules (V6 cryptography):
  * Generated with `cryptography` (never hand-rolled ASN.1/PEM).
  * Lives in a gitignored file (`infra/secrets/`, `*.pem`), never in git, the
    Docker build context, a log line, or an endpoint response.
  * The JWKS built here contains PUBLIC members only (`kty`, `n`, `e`, `kid`,
    `use`, `alg`) — never `d`, `p`, `q`, `dp`, `dq`, `qi`.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwk

from services.identity.config import settings

_KEYPAIR_SCRIPT = "scripts/generate_jwt_keypair.py"
_MIN_KEY_BITS = 2048


class SigningKeyUnavailable(RuntimeError):
    """The RSA private key could not be loaded. Identity must not start."""


@dataclass(frozen=True, slots=True)
class SigningKey:
    kid: str
    private_pem: str
    public_jwk: dict[str, Any]

    def __repr__(self) -> str:  # never let the private key reach a log/traceback
        return f"SigningKey(kid={self.kid!r}, private_pem=<redacted>)"


def generate_private_key_pem() -> str:
    """A fresh RSA-2048 private key as an unencrypted PKCS8 PEM string."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=_MIN_KEY_BITS)
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")


def _unavailable(path: str | Path, reason: str) -> SigningKeyUnavailable:
    return SigningKeyUnavailable(
        f"cannot load the JWT signing key from JWT_PRIVATE_KEY_PATH={str(path)!r}: {reason}. "
        f"Generate one with `python {_KEYPAIR_SCRIPT}` (the file is gitignored; never commit it)."
    )


def load_signing_key(path: str | Path, kid: str) -> SigningKey:
    try:
        pem_bytes = Path(path).read_bytes()
    except OSError as exc:
        raise _unavailable(path, f"file missing or unreadable ({type(exc).__name__})") from None

    try:
        private_key = serialization.load_pem_private_key(pem_bytes, password=None)
    except (ValueError, TypeError):
        raise _unavailable(path, "not a valid unencrypted PEM private key") from None

    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise _unavailable(path, "not an RSA private key")
    if private_key.key_size < _MIN_KEY_BITS:
        raise _unavailable(path, f"RSA key is smaller than {_MIN_KEY_BITS} bits")

    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    public_jwk = jwk.construct(public_pem, algorithm="RS256").to_dict()
    public_jwk.update(kid=kid, use="sig", alg="RS256")

    return SigningKey(
        kid=kid,
        private_pem=private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("ascii"),
        public_jwk=public_jwk,
    )


@lru_cache(maxsize=1)
def get_signing_key() -> SigningKey:
    """The process's signing key, loaded once from `JWT_PRIVATE_KEY_PATH`."""
    return load_signing_key(settings.jwt_private_key_path, settings.jwt_key_id)


def jwks_document() -> dict[str, Any]:
    """The public JWKS Identity publishes (public members only)."""
    return {"keys": [dict(get_signing_key().public_jwk)]}
