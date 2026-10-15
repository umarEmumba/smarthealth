"""Identity's internal routes: account lookup (D-04, D-05) and the service-token
client-credentials mint (D-06).

Patient and Provider validate a profile -> account link (`patients.user_id`,
`providers.user_id`) by calling this at write time, forwarding the caller's token:

    GET /internal/users/{user_id} -> {id, role, clinic_id}

Design rules (STRIDE T-01.1-24):
  * Not routed by the gateway (it has no `/internal` prefix) and excluded from
    OpenAPI (`include_in_schema=False`).
  * Authenticated callers only: ADMIN or FRONT_DESK user tokens
    (`require_internal_caller`). No service needs account lookups, so every
    service token is refused with 403 (plan 02-01, review WR-02).
  * A missing user and a user outside the caller's clinic scope return the
    IDENTICAL 404, so existence cannot be probed across clinics.
  * Minimal fields only. No email and no credential material ever leave Identity
    through this route. Reads are not audited (Phase 1 does not audit reads).
  * Identity makes no outbound call from here.

Service-token mint (`POST /internal/service-tokens`, D-06, research G1, STRIDE
T-02-01): a non-Identity process (a Temporal worker has no user context) proves
who it is with HTTP Basic `<service>:<secret>` and names ONE target audience; it
gets back a 60-second token with `sub=service:<service>`, `aud=<audience>`. The
per-service secret lives in `<SERVICE_CLIENT_SECRETS_DIR>/<service>.secret`, is
read per request and compared with `hmac.compare_digest`. Unknown client,
missing credentials, missing secret file and wrong secret all get the identical
401; an audience outside `SERVICE_TOKEN_AUDIENCES[client]` is 403. Token
issuance for machine callers is high-volume and carries no PHI, so it is NOT
audited — the `internal.service_token_minted` log line (client + audience only,
never the secret or token) is the record.
"""

from __future__ import annotations

import hmac
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.auth.internal import InternalCaller, clinic_scope_permits, require_internal_caller
from libs.auth.roles import Role
from libs.observability.logging import get_logger
from services.identity.config import settings
from services.identity.db import get_db
from services.identity.domain.schemas import ServiceTokenRequest, ServiceTokenResponse
from services.identity.repository.models import User
from services.identity.security.signing import create_service_token

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)

log = get_logger(__name__)

# Which service may obtain a token for which target audience (D-06). A client
# not listed here can never authenticate; `billing` currently calls no one.
SERVICE_TOKEN_AUDIENCES: dict[str, frozenset[str]] = {
    "scheduling": frozenset({"patient", "provider", "organization", "billing", "scheduling"}),
    "visit": frozenset({"scheduling", "provider", "billing", "visit"}),
    "billing": frozenset(),
}

_basic_scheme = HTTPBasic(auto_error=False)


class UserRef(BaseModel):
    id: uuid.UUID
    role: Role
    clinic_id: uuid.UUID | None = None


def _not_found() -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")


@router.get("/users/{user_id}", response_model=UserRef)
async def lookup_user(
    user_id: uuid.UUID,
    caller: InternalCaller = Depends(
        require_internal_caller(Role.ADMIN, Role.FRONT_DESK, audience="identity", allowed_services=frozenset())
    ),
    db: AsyncSession = Depends(get_db),
) -> UserRef:
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if user is None or not clinic_scope_permits(caller, user.clinic_id):
        log.info("internal.user_lookup", user_id=str(user_id), outcome="not_found")
        raise _not_found()

    log.info("internal.user_lookup", user_id=str(user.id), outcome="found")
    return UserRef(id=user.id, role=Role(user.role), clinic_id=user.clinic_id)


def _invalid_client() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="invalid client credentials",
        headers={"WWW-Authenticate": "Basic"},
    )


def _client_secret(client: str) -> str | None:
    """The configured secret for `client`, or None when it cannot authenticate
    (not a known client, no secrets dir, unreadable or empty file)."""
    if client not in SERVICE_TOKEN_AUDIENCES or not settings.service_client_credentials_dir:
        return None
    try:
        secret = (Path(settings.service_client_credentials_dir) / f"{client}.secret").read_text().strip()
    except OSError:
        return None
    return secret or None


@router.post("/service-tokens", response_model=ServiceTokenResponse)
async def mint_service_token(
    body: ServiceTokenRequest,
    credentials: HTTPBasicCredentials | None = Depends(_basic_scheme),
) -> ServiceTokenResponse:
    if credentials is None:
        raise _invalid_client()
    client = credentials.username
    expected = _client_secret(client)
    # Always run one constant-time comparison so an unknown client and a wrong
    # secret cost the same and return the same body.
    presented = credentials.password.encode("utf-8")
    if expected is None:
        hmac.compare_digest(presented, presented)
        log.info("internal.service_token_refused", reason="invalid_client")
        raise _invalid_client()
    if not hmac.compare_digest(presented, expected.encode("utf-8")):
        log.info("internal.service_token_refused", reason="invalid_client")
        raise _invalid_client()

    if body.audience not in SERVICE_TOKEN_AUDIENCES[client]:
        log.info("internal.service_token_refused", client=client, audience=body.audience, reason="audience")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="audience not permitted for this client"
        )

    token = create_service_token(client, audience=body.audience)
    log.info("internal.service_token_minted", client=client, audience=body.audience)
    return ServiceTokenResponse(
        access_token=token,
        token_type="Bearer",
        expires_in=settings.service_token_expire_seconds,
    )
