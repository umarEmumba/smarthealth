"""Pydantic v2 request/response models for Identity's auth router.

Identity stores accounts only — no patient PHI fields belong here. The
clinic/department/patient/provider/availability schemas (including the
*Update PATCH models) left with their owning services; the Phase 1 originals
remain readable at reference commit 7ce4490.
"""

from __future__ import annotations

import uuid
from typing import Literal

from pydantic import BaseModel, EmailStr, Field, field_validator

from libs.auth.roles import Role


def normalize_email(value: object) -> object:
    """Canonical email form: surrounding whitespace stripped, lowercased (C3).

    Runs BEFORE `EmailStr` validation so every code path (register, staff create,
    login) sees one form and `Alice@x` / `alice@x` are one account. Non-strings are
    passed through untouched for pydantic to reject.
    """
    if isinstance(value, str):
        return value.strip().lower()
    return value


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

    @field_validator("email", mode="before")
    @classmethod
    def _normalize_email(cls, value: object) -> object:
        return normalize_email(value)


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

    @field_validator("email", mode="before")
    @classmethod
    def _normalize_email(cls, value: object) -> object:
        return normalize_email(value)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str

    @field_validator("email", mode="before")
    @classmethod
    def _normalize_email(cls, value: object) -> object:
        return normalize_email(value)


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


class ServiceTokenRequest(BaseModel):
    """Body of `POST /internal/service-tokens`: the ONE service the token is for."""

    audience: str = Field(min_length=1, max_length=32, pattern=r"^[a-z][a-z0-9_-]*$")


class ServiceTokenResponse(BaseModel):
    access_token: str
    token_type: Literal["Bearer"]
    expires_in: int
