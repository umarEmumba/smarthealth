"""Pydantic v2 PATCH payloads for Organization's own resources.

This service's OWN copy of `ClinicUpdate` / `DepartmentUpdate` and the shared
empty-body check (the Phase 1 originals remain readable at reference commit
7ce4490): `libs` carries infrastructure only and never domain schemas.

Every field is optional (partial update — only supplied fields change) and a
fully-empty body is rejected 422.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


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
