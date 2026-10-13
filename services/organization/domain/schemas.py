"""Pydantic v2 PATCH payloads for Organization's own resources.

This service's OWN copy of `ClinicUpdate` / `DepartmentUpdate` and the shared
empty-body check (the Phase 1 originals remain readable at reference commit
7ce4490): `libs` carries infrastructure only and never domain schemas.

Every field is optional (partial update — only supplied fields change) and a
fully-empty body is rejected 422.

`validate_iana_timezone` (plan 02-03, research G11 / review WR-13) is the shared
clinic-timezone rule: Scheduling interprets naive availability times in the
clinic's zone, so only names the system IANA zone database resolves are
accepted (the PyPI zone-database package is deliberately not a dependency).
"""

from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator, model_validator


def validate_iana_timezone(value: str) -> str:
    """Return `value` unchanged if it names an IANA time zone; else ValueError."""
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("timezone must be an IANA name") from exc
    return value


def _reject_empty_body(model: BaseModel) -> BaseModel:
    if not model.model_fields_set:
        raise ValueError("update body must include at least one field")
    return model


class ClinicUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    timezone: str | None = Field(default=None, min_length=1, max_length=64)

    @field_validator("timezone")
    @classmethod
    def _check_timezone(cls, value: str | None) -> str | None:
        return None if value is None else validate_iana_timezone(value)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "ClinicUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]


class DepartmentUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "DepartmentUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]
