"""Shared auth primitives — VERIFICATION ONLY.

`libs/auth` is what every service imports: token verification (RS256 + JWKS,
no private key), the `get_current_user` / `require_role` dependencies, the
Redis jti denylist, and the `Role` enum.

Signing and password hashing are NOT here. Identity alone holds the RSA
private key and the credentials, so token issuance
(`services/identity/security/signing.py`) and password hashing
(`services/identity/security/passwords.py`) live inside the Identity service.
That keeps the blast radius of any other service's compromise to "can verify
tokens", never "can mint tokens" or "can read credentials".
"""
