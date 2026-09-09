"""Auth router: register / login / refresh / logout.

The end-to-end tracer path: register writes a User row (bcrypt-hashed
password) and an audit entry; login verifies the password and issues an
access+refresh JWT pair plus an audit entry; refresh re-derives role/clinic
from the CURRENT DB state (so a role change takes effect on next refresh,
not just next login) and issues a fresh access token; logout revokes the
access token's jti in the Redis denylist and writes an audit entry.

Real error handling on this one path: 401 invalid credentials, 401
revoked/expired token, 409 duplicate email. Denied/failed mutations (e.g.
duplicate email) are ALSO audited with outcome=failure (RESEARCH Pitfall 2 —
audit coverage on denied attempts, not only successes).
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pymongo import AsyncMongoClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.audit.writer import write_audit
from libs.auth.denylist import revoke_token
from libs.auth.jwt import TokenError, create_access_token, create_refresh_token, decode_token
from libs.observability.logging import get_logger
from services.identity.domain.enums import Role
from services.identity.domain.schemas import (
    LoginRequest,
    LoginResponse,
    RefreshRequest,
    RegisterRequest,
    TokenPair,
    UserOut,
)
from services.identity.repository.models import User
from services.identity.security.current_user import AuthenticatedUser, get_current_user, get_redis
from smarthealth.db import get_db, get_mongo

router = APIRouter(prefix="/api/auth", tags=["auth"])

log = get_logger(__name__)


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def register(
    payload: RegisterRequest,
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> UserOut:
    from libs.auth.passwords import hash_password  # noqa: PLC0415

    existing = await db.execute(select(User).where(User.email == payload.email))
    if existing.scalar_one_or_none() is not None:
        await write_audit(
            mongo,
            actor_id="anonymous",
            actor_role="anonymous",
            action="user.create",
            entity_type="user",
            entity_id="unknown",
            clinic_id=str(payload.clinic_id) if payload.clinic_id else None,
            outcome="failure",
        )
        log.info("auth.register.duplicate_email", email_domain=payload.email.split("@")[-1])
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="email already registered"
        )

    user = User(
        email=payload.email,
        hashed_password=hash_password(payload.password),
        role=payload.role.value,
        clinic_id=payload.clinic_id,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    await write_audit(
        mongo,
        actor_id=str(user.id),
        actor_role=user.role,
        action="user.create",
        entity_type="user",
        entity_id=str(user.id),
        clinic_id=str(user.clinic_id) if user.clinic_id else None,
        outcome="success",
    )
    log.info("auth.register.success", user_id=str(user.id), role=user.role)

    return UserOut(
        id=user.id,
        email=user.email,
        role=Role(user.role),
        clinic_id=user.clinic_id,
    )


@router.post("/login", response_model=LoginResponse)
async def login(
    payload: LoginRequest,
    db: AsyncSession = Depends(get_db),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> LoginResponse:
    from libs.auth.passwords import verify_password  # noqa: PLC0415

    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalar_one_or_none()

    if user is None or not verify_password(payload.password, user.hashed_password):
        await write_audit(
            mongo,
            actor_id="anonymous",
            actor_role="anonymous",
            action="auth.login",
            entity_type="user",
            entity_id=str(user.id) if user else "unknown",
            clinic_id=str(user.clinic_id) if user and user.clinic_id else None,
            outcome="failure",
        )
        log.info("auth.login.invalid_credentials")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid email or password"
        )

    clinic_id_str = str(user.clinic_id) if user.clinic_id else None
    access_token, _access_jti = create_access_token(str(user.id), user.role, clinic_id_str)
    refresh_token, _refresh_jti = create_refresh_token(str(user.id))

    await write_audit(
        mongo,
        actor_id=str(user.id),
        actor_role=user.role,
        action="auth.login",
        entity_type="user",
        entity_id=str(user.id),
        clinic_id=clinic_id_str,
        outcome="success",
    )
    log.info("auth.login.success", user_id=str(user.id), role=user.role)

    return LoginResponse(access_token=access_token, refresh_token=refresh_token)


@router.post("/refresh", response_model=TokenPair)
async def refresh(
    payload: RefreshRequest,
    db: AsyncSession = Depends(get_db),
) -> TokenPair:
    try:
        claims = decode_token(payload.refresh_token, expected_type="refresh")
    except TokenError as exc:
        log.info("auth.refresh.invalid_token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or expired refresh token"
        ) from exc

    user_id = claims["sub"]
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        log.info("auth.refresh.user_not_found")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid refresh token")

    # Re-derive role/clinic from CURRENT DB state — a role change takes
    # effect on next refresh, not just next login (locked contract).
    clinic_id_str = str(user.clinic_id) if user.clinic_id else None
    access_token, _access_jti = create_access_token(str(user.id), user.role, clinic_id_str)
    new_refresh_token, _refresh_jti = create_refresh_token(str(user.id))

    log.info("auth.refresh.success", user_id=str(user.id))

    return TokenPair(access_token=access_token, refresh_token=new_refresh_token)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    user: AuthenticatedUser = Depends(get_current_user),
    redis: Redis = Depends(get_redis),
    mongo: AsyncMongoClient = Depends(get_mongo),
) -> None:
    try:
        claims = decode_token(user.raw_token, expected_type="access")
    except TokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token") from exc

    # python-jose serializes `exp` to a numeric epoch timestamp on encode
    # and returns it as such on decode (never a datetime object).
    exp_epoch = float(claims["exp"])
    remaining_seconds = int(exp_epoch - datetime.now(timezone.utc).timestamp())

    await revoke_token(user.jti, remaining_seconds, redis)

    await write_audit(
        mongo,
        actor_id=user.sub,
        actor_role=user.role.value,
        action="auth.logout",
        entity_type="user",
        entity_id=user.sub,
        clinic_id=user.clinic_id,
        outcome="success",
    )
    log.info("auth.logout.success", user_id=user.sub, jti=user.jti)
