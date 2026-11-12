"""Pydantic v2 PATCH payload for Patient's own resource.

This service's OWN copy of `PatientUpdate` and the shared empty-body check (the
Phase 1 originals remain readable at reference commit 7ce4490): `libs` carries
infrastructure only and never domain schemas.

Every field is optional (partial update — only supplied fields change) and a
fully-empty body is rejected 422.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, model_validator


def _reject_empty_body(model: BaseModel) -> BaseModel:
    if not model.model_fields_set:
        raise ValueError("update body must include at least one field")
    return model


class PatientUpdate(BaseModel):
    """PHI-bearing partial-update payload — never log/audit these field
    VALUES (same PHI boundary as `PatientCreate` in api/patients.py).
    """

    full_name: str | None = Field(default=None, min_length=1, max_length=255)
    date_of_birth: date | None = None
    phone: str | None = Field(default=None, max_length=32)
    address: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "PatientUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]
