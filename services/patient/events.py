"""Patient's domain events: payload builders and outbox emit helpers (plan 03-08;
EVENT-01, EVENT-02, ANLY-01; D-06, D-07, D-10).

`patient.registered` and `patient.deactivated` are written to patient_db's outbox
inside the caller's transaction, so each commits (or rolls back) with the patient
change that caused it. Debezium relays committed rows to Kafka (plan 03-05); nothing
here talks to Kafka. Total patients (ANLY-01) is derived from these two events, so
the analytics projection never reads patient_db.

Patient is the ONLY service holding PHI, so these builders read exactly three kinds
of attribute from the ORM object: `id`, `clinic_id` and timestamps. `full_name`,
`date_of_birth`, `phone`, `address` and `user_id` are never touched here, so they
cannot reach an event (T-3-24). The Avro schemas (plan 03-03) have no PHI fields
either, so a payload carrying one is rejected at serialization.

Event ids are uuid5 of `patient id : type : discriminator`: `registered` for the
single registration, and the deactivation time for the deactivation. Only
state-changing branches call these helpers; a repeat deactivation never does.
"""

from __future__ import annotations

import datetime
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from libs.events.catalog import EventType
from libs.events.outbox import enqueue_event
from services.patient.repository.models import Patient

PATIENT_EVENT_TYPES: tuple[str, ...] = (
    EventType.PATIENT_REGISTERED,
    EventType.PATIENT_DEACTIVATED,
)

REGISTERED_DISCRIMINATOR = "registered"


async def emit_patient_registered(db: AsyncSession, patient: Patient) -> uuid.UUID:
    """Write the `patient.registered` outbox row (ids and the creation time only).

    `patient.id` and `patient.created_at` must already be loaded (after a flush).
    """
    created_at: datetime.datetime = patient.created_at
    return await enqueue_event(
        db,
        event_type=EventType.PATIENT_REGISTERED,
        aggregate_id=str(patient.id),
        payload={
            "patient_id": str(patient.id),
            "clinic_id": str(patient.clinic_id),
            "registered_at": created_at,
        },
        discriminator=REGISTERED_DISCRIMINATOR,
        occurred_at=created_at,
    )


async def emit_patient_deactivated(db: AsyncSession, patient: Patient) -> uuid.UUID:
    """Write the `patient.deactivated` outbox row (ids and the deactivation time only).

    The discriminator is the deactivation timestamp, so the event id is stable for
    that deactivation.
    """
    deactivated_at: datetime.datetime = patient.deactivated_at  # type: ignore[assignment]
    return await enqueue_event(
        db,
        event_type=EventType.PATIENT_DEACTIVATED,
        aggregate_id=str(patient.id),
        payload={
            "patient_id": str(patient.id),
            "clinic_id": str(patient.clinic_id),
            "deactivated_at": deactivated_at,
        },
        discriminator=deactivated_at.isoformat(),
        occurred_at=deactivated_at,
    )
