"""Password hashing via passlib[bcrypt] (CLAUDE.md-mandated).

Never hand-roll hash+salt logic (RESEARCH "Don't Hand-Roll") — bcrypt's
work-factor tuning and salt handling are solved problems in this library.
Hashed passwords are the only form ever stored/logged; the plaintext
password is never persisted or logged anywhere.

Deviation (Rule 1 - bug, documented in 01-03-SUMMARY.md): passlib 1.7.4's
bcrypt backend runs an internal self-test (`detect_wrap_bug`) on first use
that probes with a 255-byte secret. bcrypt>=4.1 (pinned to 5.0.0 here per
CLAUDE.md) enforces its 72-byte hard limit strictly and raises `ValueError`
instead of the old silent-truncation behavior pybcrypt/bcrypt<4.1 had —
this is a documented upstream passlib/bcrypt version incompatibility with
no passlib-1.7.4-side fix, unrelated to any real user password. We call
the `bcrypt` library directly (still bcrypt, still the CLAUDE.md-mandated
algorithm) rather than routing through passlib's incompatible self-test.
Passwords are truncated to bcrypt's own 72-byte hard limit before hashing —
standard, industry-wide practice (bcrypt has never supported longer
secrets; anything beyond 72 bytes is ignored by the algorithm itself).
"""

from __future__ import annotations

import bcrypt

_MAX_PASSWORD_BYTES = 72


def _truncate_to_bcrypt_limit(password: str) -> bytes:
    return password.encode("utf-8")[:_MAX_PASSWORD_BYTES]


def hash_password(password: str) -> str:
    """Hash a plaintext password with bcrypt. Never log the input or output."""
    hashed = bcrypt.hashpw(_truncate_to_bcrypt_limit(password), bcrypt.gensalt())
    return hashed.decode("ascii")


def verify_password(password: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash. `bcrypt.checkpw`
    performs a constant-time comparison internally.
    """
    return bcrypt.checkpw(_truncate_to_bcrypt_limit(password), hashed.encode("ascii"))
