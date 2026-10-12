"""Identity service settings: the shared infra settings plus Identity's OWN
database URL and its signing-key configuration.

`database_url` is bound to `IDENTITY_DATABASE_URL` only — there is deliberately
no bare `DATABASE_URL` anywhere (it would silently point a service at someone
else's database, defeating database-per-service, D-01). The signing key is a
FILE path (`JWT_PRIVATE_KEY_PATH`), never an inline value, so the private key
cannot leak through an environment dump; the file is gitignored and generated
by `scripts/generate_jwt_keypair.py`.
"""

from __future__ import annotations

from pydantic import Field

from libs.config import SharedSettings


class IdentitySettings(SharedSettings):
    database_url: str = Field(
        default="postgresql+asyncpg://identity_svc:changeme-identity-local-dev-only@localhost:5433/identity_db",
        validation_alias="IDENTITY_DATABASE_URL",
    )

    # RS256 signing key (private key held ONLY by this service).
    jwt_private_key_path: str = Field(
        default="infra/secrets/jwt_private.pem", validation_alias="JWT_PRIVATE_KEY_PATH"
    )
    jwt_key_id: str = Field(default="smarthealth-identity-1", validation_alias="JWT_KEY_ID")

    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7


settings = IdentitySettings()
