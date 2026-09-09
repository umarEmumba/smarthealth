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
