"""Unit tests for `services/identity/security/passwords.py` (WR-03 constant-time login support).

No live containers required — pure unit tests against the bcrypt wrapper
functions.
"""

from __future__ import annotations

import bcrypt

from services.identity.security.passwords import DUMMY_PASSWORD_HASH, hash_password, verify_password


def test_hash_and_verify_round_trip() -> None:
    hashed = hash_password("correct-horse-battery")
    assert verify_password("correct-horse-battery", hashed)
    assert not verify_password("wrong-password", hashed)


def test_dummy_password_hash_is_a_valid_bcrypt_hash() -> None:
    """WR-03: `DUMMY_PASSWORD_HASH` must be a real, checkable bcrypt hash —
    login's constant-time path verifies real password attempts against it
    when no user exists, so it must actually accept/reject via
    `bcrypt.checkpw` like any other stored hash, paying the same cost.
    """
    assert bcrypt.checkpw(b"dummy-password-for-constant-time-login", DUMMY_PASSWORD_HASH.encode("ascii"))
    assert not bcrypt.checkpw(b"some-attacker-guess", DUMMY_PASSWORD_HASH.encode("ascii"))


def test_verify_password_against_dummy_hash_never_succeeds_for_real_passwords() -> None:
    """A real user-submitted password must never accidentally verify
    against the dummy hash (which would be a false-positive login) — it
    exists purely to pay bcrypt's cost, never to authenticate anyone.
    """
    assert not verify_password("correct-horse-battery", DUMMY_PASSWORD_HASH)
