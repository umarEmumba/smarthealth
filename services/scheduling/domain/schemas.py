"""Scheduling's public request/response models (plan 02-04).

Slot payloads carry ids and UTC instants only: no names, no PHI, and no data
enriched from another service (Phase 1.1 D-07).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from pydantic import BaseModel


class SlotGenerateRequest(BaseModel):
    provider_id: uuid.UUID
    # Inclusive local dates, read in the provider's clinic timezone.
    from_date: date
    to_date: date


class SlotGenerateResponse(BaseModel):
    # Rows actually inserted / candidate slots the templates produced. A
    # repeated request reports created == 0.
    created: int
    considered: int


class SlotOut(BaseModel):
    id: uuid.UUID
    provider_id: uuid.UUID
    clinic_id: uuid.UUID
    department_id: uuid.UUID
    start_time: datetime
    end_time: datetime
    status: str
