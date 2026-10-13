"""Visit request/response models (plan 02-10, D-15, D-17, D-18).

Responses carry ids, statuses and timestamps only: no patient name, no
provider name, nothing enriched from another service, and never the actor
recorded on a history row.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel


class CheckInRequest(BaseModel):
    appointment_id: uuid.UUID
    # Front-desk may omit it (their clinic comes from the token) or must repeat
    # their own; admin must name the clinic the appointment belongs to.
    clinic_id: uuid.UUID | None = None


class VisitHistoryOut(BaseModel):
    from_status: str | None
    to_status: str
    created_at: datetime


class VisitOut(BaseModel):
    id: uuid.UUID
    appointment_id: uuid.UUID
    clinic_id: uuid.UUID
    patient_id: uuid.UUID
    provider_id: uuid.UUID
    status: str
    checked_in_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class VisitDetailOut(VisitOut):
    history: list[VisitHistoryOut]
