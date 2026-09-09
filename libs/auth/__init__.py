"""Shared auth primitives: password hashing, JWT issuance/validation, and
Redis-backed token revocation (jti denylist).

Consumed by `services/identity/security/current_user.py` and
`services/identity/api/auth.py`, and reused by every later service in this
project — this is the shared auth contract, not a per-service helper.
"""
