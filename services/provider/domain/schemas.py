"""Pydantic v2 partial-update (PATCH) payloads for the Provider service.

Every *Update schema has all-optional fields (only supplied fields change) and
rejects a fully-empty body with 422 via a shared `_reject_empty_body` model
validator. Neither schema carries a cross-service reference field, so a PATCH
never needs another service (a deactivated department therefore never blocks it,
D-06).
"""

from __future__ import annotations

from datetime import date, time

from pydantic import BaseModel, Field, model_validator


def _reject_empty_body(model: BaseModel) -> BaseModel:
    if not model.model_fields_set:
        raise ValueError("update body must include at least one field")
    return model


class ProviderUpdate(BaseModel):
    full_name: str | None = Field(default=None, min_length=1, max_length=255)
    specialties: list[str] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _check_not_empty(self) -> "ProviderUpdate":
        return _reject_empty_body(self)  # type: ignore[return-value]


class AvailabilityTemplateUpdate(BaseModel):
    """Partial update of a weekly availability template.

    Window-consistency validation mirrors the create schema: if `start_time` and
    `end_time` are BOTH supplied in the same PATCH, end must be strictly after
    start; if only one of the pair is supplied, the router validates against the
    existing stored value before persisting (a partial update must never leave
    the row in an inconsistent state). Likewise `effective_from`/`effective_to`.
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
