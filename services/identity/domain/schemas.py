"""Pydantic v2 request/response models for the auth router.

`RegisterRequest`/`LoginRequest` are the only PHI-adjacent-free auth
payloads Phase 1's tracer needs (no patient PHI fields belong in the auth
router — that's Plan 04/05 CRUD scope).
"""

from __future__ import annotations

import uuid

from pydantic import BaseModel, EmailStr, Field

from services.identity.domain.enums import Role


class RegisterRequest(BaseModel):
    """Public self-registration payload.

    Deliberately has NO `role` field — public, unauthenticated
    self-registration always creates a `Role.PATIENT` account (CR-01).
    Elevated-role accounts (admin/provider/front_desk) must be created via
    an authenticated-admin-only staff-creation route, never via this
    anonymous endpoint.
    """

    email: EmailStr
    password: str = Field(min_length=8)
    clinic_id: uuid.UUID | None = None


class StaffRegisterRequest(BaseModel):
    """Admin-only staff/provider/admin account creation payload (CR-01).

    Unlike `RegisterRequest`, this DOES accept a caller-supplied `role`
    because the caller is required (by the route's `require_role(Role.ADMIN)`
    dependency) to already be an authenticated admin — the role is no
    longer attacker-controlled at this point.
    """

    email: EmailStr
    password: str = Field(min_length=8)
    role: Role
    clinic_id: uuid.UUID | None = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    pass


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class LoginResponse(TokenPair):
    """Alias kept distinct from TokenPair for readability at call sites;
    identical shape today, may diverge (e.g. add `user_id`) without
    breaking TokenPair consumers.
    """


class AccessTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserOut(BaseModel):
    id: uuid.UUID
    email: EmailStr
    role: Role
    clinic_id: uuid.UUID | None = None
