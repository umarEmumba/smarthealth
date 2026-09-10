"""Pydantic v2 request/response models for the auth router.

`RegisterRequest`/`LoginRequest` are the only PHI-adjacent-free auth
payloads Phase 1's tracer needs (no patient PHI fields belong in the auth
router — that's Plan 04/05 CRUD scope).
"""

from __future__ import annotations

import uuid
from datetime import date, time

from pydantic import BaseModel, EmailStr, Field, model_validator

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


# --- Gap-closure Plan 01-07: partial-update (PATCH) payloads ---------------
#
# Every *Update schema below has all-optional fields (partial update — only
# supplied fields change) and rejects a fully-empty body with 422 via a
# shared `_reject_empty_body` model validator, per the plan's must_haves.
# These live in schemas.py (not inline in the router files, unlike the
# *Create schemas from Plans 04/05) because the empty-body-rejection
# validator is identical across all five resources and is worth sharing
# from one place rather than five near-duplicate copies.


def _reject_empty_body(model: BaseModel) -> BaseModel:
    if not model.model_fields_set:
        raise ValueError("update body must include at least one field")
    return model


class ClinicUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    timezone: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "ClinicUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]


class DepartmentUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "DepartmentUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]


class PatientUpdate(BaseModel):
    """PHI-bearing partial-update payload — never log/audit these field
    VALUES (same PHI boundary as `PatientCreate` in patients.py).
    """

    full_name: str | None = Field(default=None, min_length=1, max_length=255)
    date_of_birth: date | None = None
    phone: str | None = Field(default=None, max_length=32)
    address: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "PatientUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]


class ProviderUpdate(BaseModel):
    full_name: str | None = Field(default=None, min_length=1, max_length=255)
    specialties: list[str] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "ProviderUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]


class AvailabilityTemplateUpdate(BaseModel):
    """Partial update of a weekly availability template.

    Window-consistency validation mirrors `AvailabilityTemplateCreate`
    (availability.py): if `start_time`/`end_time` are BOTH supplied in
    the same PATCH, end must be strictly after start; if only one of the
    pair is supplied, the router validates against the existing stored
    value before persisting (a partial update must never leave the row in
    an inconsistent state). Likewise `effective_from`/`effective_to`.
    """

    day_of_week: int | None = Field(default=None, ge=0, le=6)
    start_time: time | None = None
    end_time: time | None = None
    slot_duration_minutes: int | None = Field(default=None, gt=0)
    effective_from: date | None = None
    effective_to: date | None = None

    @model_validator(mode="after")
    def _check_not_empty(self) -> "AvailabilityTemplateUpdate":
        _reject_empty_body(self)
        if (
            self.start_time is not None
            and self.end_time is not None
            and self.end_time <= self.start_time
        ):
            raise ValueError("end_time must be strictly after start_time")
        if (
            self.effective_from is not None
            and self.effective_to is not None
            and self.effective_from > self.effective_to
        ):
            raise ValueError("effective_from must be on or before effective_to")
        return self
